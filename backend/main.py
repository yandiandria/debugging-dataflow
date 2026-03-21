import asyncio
import io
import json
import os
import re
import subprocess
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List, Optional

import pandas as pd
from azure.core.exceptions import AzureError
from azure.storage.blob import ContainerClient
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

# Pipeline stage order (most specific names first for detection; order reflects pipeline flow)
STAGE_ORDER = [
    "extract",
    "clean_cleaned",
    "clean_incoherent",
    "compare_and_identify_in_flow_only",
    "compare_and_identify_in_flow_and_db_different",
    "compare_and_identify_not_linked_mandatory",
    "compare_and_identify_not_linked_optional",
    "transform",
]


def detect_stage(filename: str) -> Optional[str]:
    """Detect pipeline stage from filename using keyword matching.
    Longer (more specific) patterns are tested first to avoid 'clean' matching
    'clean_cleaned' before the specific variant does.
    """
    lower = filename.lower()
    for stage in STAGE_ORDER:
        if stage in lower:
            return stage
    return "extract"


def build_container_client(container_url: str) -> ContainerClient:
    return ContainerClient.from_container_url(container_url)


def _sse(payload: Any) -> str:
    """Encode a value as an SSE data line."""
    return f"data: {json.dumps(payload)}\n\n"


def _log(message: str, level: str = "info") -> str:
    return _sse({"type": "log", "level": level, "message": message})


app = FastAPI(title="Data Flow Debugger API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Persistence helpers ─────────────────────────────────────────────────────────

DATA_DIR = Path(__file__).parent
RESOURCES_FILE = DATA_DIR / "resources.json"
DAGS_FILE = DATA_DIR / "dags.json"
DAG_CONFIG_FILE = DATA_DIR / "dag_config.json"
RULES_FILE = DATA_DIR / "rules.json"
DAG_RUNS_FILE = DATA_DIR / "dag_runs.json"
MAPPING_ISSUES_FILE = DATA_DIR / "mapping_issues.json"
QA_EXAMPLES_FILE = DATA_DIR / "qa_examples.json"


def _load_json(path: Path) -> List[Dict]:
    if not path.exists():
        return []
    return json.loads(path.read_text())


def _save_json(path: Path, data: List[Dict]) -> None:
    path.write_text(json.dumps(data, indent=2))


def _load_resources() -> List[Dict]:
    return _load_json(RESOURCES_FILE)


def _save_resources(resources: List[Dict]) -> None:
    _save_json(RESOURCES_FILE, resources)


def _load_dag_config() -> Dict:
    if not DAG_CONFIG_FILE.exists():
        return {"container_name": "", "environments": ["dev", "staging", "prod"]}
    return json.loads(DAG_CONFIG_FILE.read_text())


def _save_dag_config(config: Dict) -> None:
    DAG_CONFIG_FILE.write_text(json.dumps(config, indent=2))


# ── Models ─────────────────────────────────────────────────────────────────────

class Resource(BaseModel):
    id: str
    technical_name: str
    business_name: str
    created_at: str


class ResourceCreate(BaseModel):
    technical_name: str
    business_name: str


class BlobListRequest(BaseModel):
    container_url: str
    prefix: Optional[str] = None     # Azure-side path prefix filter
    date_from: Optional[str] = None  # ISO date string, e.g. "2024-01-15"
    date_to: Optional[str] = None    # ISO date string, inclusive


class BlobInfo(BaseModel):
    name: str
    size: int
    last_modified: str
    detected_stage: Optional[str]


class FilterCondition(BaseModel):
    column: str
    value: str
    filter_type: str = "equals"  # "equals" | "regex"


class AnalyzeRequest(BaseModel):
    container_url: str
    selected_blobs: List[str]
    key_columns: List[str]
    filters: List[FilterCondition]
    deduplicate: bool = True
    filter_logic: str = "AND"  # "AND" | "OR"


class ColumnsRequest(BaseModel):
    container_url: str
    blob_name: str


class PreviewRequest(BaseModel):
    container_url: str
    blob_name: str
    limit: int = 200


class ProfileRequest(BaseModel):
    container_url: str
    blob_names: List[str]


# ── DAG models ─────────────────────────────────────────────────────────────────

class DAG(BaseModel):
    id: str
    dag_id: str
    display_name: str
    created_at: str


class DAGCreate(BaseModel):
    dag_id: str
    display_name: str


class DAGConfigUpdate(BaseModel):
    container_name: str
    environments: List[str] = ["integration", "snap", "recette", "prod"]


class DAGRunRequest(BaseModel):
    dag_id: str
    padoa_env: str


class DAGLogRequest(BaseModel):
    dag_id: str
    run_id: str
    task_ids: List[str] = []


# ── Integration Rule models ───────────────────────────────────────────────────

class IntegrationRule(BaseModel):
    id: str
    description: str
    resource_ids: List[str] = []
    created_at: str


class IntegrationRuleCreate(BaseModel):
    description: str
    resource_ids: List[str] = []


class IntegrationRuleUpdate(BaseModel):
    description: Optional[str] = None
    resource_ids: Optional[List[str]] = None
    checked: Optional[bool] = None


# ── Resource-DAG linking model ─────────────────────────────────────────────────

class ResourceDAGLink(BaseModel):
    dag_ids: List[str]  # ordered list of DAG ids


# ── Mapping Issue models ──────────────────────────────────────────────────────

class MappingIssue(BaseModel):
    id: str
    resource_id: str
    dag_run_id: str
    unmapped_value: str
    column: str
    first_seen: str
    fixed_in_rerun: Optional[str] = None  # date string
    resolved: bool = False


class MappingIssueResolve(BaseModel):
    resolved: bool


# ── QA Example ID models ──────────────────────────────────────────────────────

class QAExampleID(BaseModel):
    id: str
    resource_id: str
    id_column: str
    id_value: str
    label: str
    created_at: str


class QAExampleIDCreate(BaseModel):
    resource_id: str
    id_column: str
    id_value: str
    label: str


# ── Endpoints ──────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "healthy"}


# ── Resource endpoints ──────────────────────────────────────────────────────────

@app.get("/api/resources")
async def list_resources() -> List[Resource]:
    return [Resource(**r) for r in _load_resources()]


@app.post("/api/resources", status_code=201)
async def create_resource(body: ResourceCreate) -> Resource:
    resources = _load_resources()
    resource = Resource(
        id=str(uuid.uuid4()),
        technical_name=body.technical_name,
        business_name=body.business_name,
        created_at=datetime.now(timezone.utc).isoformat(),
    )
    resources.append(resource.model_dump())
    _save_resources(resources)
    return resource


@app.put("/api/resources/{resource_id}")
async def update_resource(resource_id: str, body: ResourceCreate) -> Resource:
    resources = _load_resources()
    for i, r in enumerate(resources):
        if r["id"] == resource_id:
            r["technical_name"] = body.technical_name
            r["business_name"] = body.business_name
            resources[i] = r
            _save_resources(resources)
            return Resource(**r)
    raise HTTPException(status_code=404, detail="Resource not found")


@app.delete("/api/resources/{resource_id}", status_code=204)
async def delete_resource(resource_id: str):
    resources = _load_resources()
    updated = [r for r in resources if r["id"] != resource_id]
    if len(updated) == len(resources):
        raise HTTPException(status_code=404, detail="Resource not found")
    _save_resources(updated)


# ── Resource-DAG linking ───────────────────────────────────────────────────────

@app.put("/api/resources/{resource_id}/dags")
async def link_resource_dags(resource_id: str, body: ResourceDAGLink):
    resources = _load_resources()
    for r in resources:
        if r["id"] == resource_id:
            r["dag_ids"] = body.dag_ids
            _save_resources(resources)
            return {"status": "ok", "dag_ids": body.dag_ids}
    raise HTTPException(status_code=404, detail="Resource not found")


@app.get("/api/resources/{resource_id}/dags")
async def get_resource_dags(resource_id: str):
    resources = _load_resources()
    for r in resources:
        if r["id"] == resource_id:
            return {"dag_ids": r.get("dag_ids", [])}
    raise HTTPException(status_code=404, detail="Resource not found")


# ── Resource-Rule linking ──────────────────────────────────────────────────────

@app.get("/api/resources/{resource_id}/rules")
async def get_resource_rules(resource_id: str):
    rules = _load_json(RULES_FILE)
    return [r for r in rules if resource_id in r.get("resource_ids", [])]


# ── DAG endpoints ─────────────────────────────────────────────────────────────

@app.get("/api/dag-config")
async def get_dag_config():
    return _load_dag_config()


@app.put("/api/dag-config")
async def update_dag_config(body: DAGConfigUpdate):
    config = _load_dag_config()
    config["container_name"] = body.container_name
    config["environments"] = body.environments
    _save_dag_config(config)
    return config


@app.get("/api/dags")
async def list_dags() -> List[DAG]:
    return [DAG(**d) for d in _load_json(DAGS_FILE)]


@app.post("/api/dags", status_code=201)
async def create_dag(body: DAGCreate) -> DAG:
    dags = _load_json(DAGS_FILE)
    dag = DAG(
        id=str(uuid.uuid4()),
        dag_id=body.dag_id,
        display_name=body.display_name,
        created_at=datetime.now(timezone.utc).isoformat(),
    )
    dags.append(dag.model_dump())
    _save_json(DAGS_FILE, dags)
    return dag


@app.put("/api/dags/{dag_internal_id}")
async def update_dag(dag_internal_id: str, body: DAGCreate) -> DAG:
    dags = _load_json(DAGS_FILE)
    for i, d in enumerate(dags):
        if d["id"] == dag_internal_id:
            d["dag_id"] = body.dag_id
            d["display_name"] = body.display_name
            dags[i] = d
            _save_json(DAGS_FILE, dags)
            return DAG(**d)
    raise HTTPException(status_code=404, detail="DAG not found")


@app.delete("/api/dags/{dag_internal_id}", status_code=204)
async def delete_dag(dag_internal_id: str):
    dags = _load_json(DAGS_FILE)
    updated = [d for d in dags if d["id"] != dag_internal_id]
    if len(updated) == len(dags):
        raise HTTPException(status_code=404, detail="DAG not found")
    _save_json(DAGS_FILE, updated)


# ── DAG execution ─────────────────────────────────────────────────────────────

@app.post("/api/dags/trigger")
async def trigger_dag(body: DAGRunRequest) -> StreamingResponse:
    config = _load_dag_config()
    container = config.get("container_name", "")
    if not container:
        raise HTTPException(status_code=400, detail="Docker container name not configured. Set it in DAG config.")

    conf_json = json.dumps({"padoa_env": body.padoa_env})
    cmd = ["docker", "exec", container, "airflow", "dags", "trigger", body.dag_id, "--conf", conf_json]

    async def generate() -> AsyncGenerator[str, None]:
        yield _log(f"Triggering DAG {body.dag_id} with padoa_env={body.padoa_env}...")
        yield _log(f"Command: {' '.join(cmd)}")

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await proc.communicate()

            stdout_text = stdout.decode() if stdout else ""
            stderr_text = stderr.decode() if stderr else ""

            if stdout_text:
                for line in stdout_text.strip().split("\n"):
                    yield _log(line, "info")

            if stderr_text:
                for line in stderr_text.strip().split("\n"):
                    yield _log(line, "warning")

            if proc.returncode == 0:
                # Try to extract run_id from output
                run_id = ""
                for line in stdout_text.split("\n"):
                    if "run_id" in line.lower() or "dag_run" in line.lower():
                        # Common airflow output: "Created <DagRun ... run_id=manual__2024-..."
                        match = re.search(r"run_id[=:\s]+(\S+)", line)
                        if match:
                            run_id = match.group(1).strip(",").strip("'\"")
                            break

                # Save run record
                runs = _load_json(DAG_RUNS_FILE)
                run_record = {
                    "id": str(uuid.uuid4()),
                    "dag_id": body.dag_id,
                    "run_id": run_id,
                    "padoa_env": body.padoa_env,
                    "triggered_at": datetime.now(timezone.utc).isoformat(),
                    "status": "triggered",
                    "stdout": stdout_text,
                    "stderr": stderr_text,
                }
                runs.append(run_record)
                _save_json(DAG_RUNS_FILE, runs)

                yield _log(f"DAG triggered successfully. Run ID: {run_id or 'unknown'}", "success")
                yield _sse({"type": "result", "run_id": run_id, "record_id": run_record["id"]})
            else:
                yield _log(f"DAG trigger failed with exit code {proc.returncode}", "error")
                yield _sse({"type": "error", "message": f"Exit code {proc.returncode}"})

        except FileNotFoundError:
            yield _sse({"type": "error", "message": "Docker command not found. Is Docker installed and in PATH?"})
        except Exception as e:
            yield _sse({"type": "error", "message": str(e)})

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/api/dags/logs")
async def get_dag_logs(body: DAGLogRequest) -> StreamingResponse:
    config = _load_dag_config()
    container = config.get("container_name", "")
    if not container:
        raise HTTPException(status_code=400, detail="Docker container name not configured.")

    task_ids = body.task_ids if body.task_ids else [None]  # None = no task filter

    async def generate() -> AsyncGenerator[str, None]:
        yield _log(f"Fetching logs for DAG {body.dag_id}, run {body.run_id}...")

        all_logs = ""
        all_mapping_issues: List[Dict[str, str]] = []

        try:
            for task_id in task_ids:
                cmd = ["docker", "exec", container, "airflow", "tasks", "logs", body.dag_id]
                if task_id:
                    cmd.append(task_id)
                cmd.append(body.run_id)

                if task_id:
                    yield _log(f"── task: {task_id} ──", "info")

                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, stderr = await proc.communicate()
                stdout_text = stdout.decode() if stdout else ""
                stderr_text = stderr.decode() if stderr else ""

                if stdout_text:
                    for line in stdout_text.strip().split("\n"):
                        yield _log(line, "info")
                if stderr_text:
                    for line in stderr_text.strip().split("\n"):
                        yield _log(line, "warning")

                all_logs += stdout_text + "\n" + stderr_text + "\n"

                # Parse mapping issues from this task's logs
                issues = _parse_mapping_issues(stdout_text + "\n" + stderr_text)
                all_mapping_issues.extend(issues)

            if all_mapping_issues:
                yield _log(f"Found {len(all_mapping_issues)} mapping issue(s) in logs", "warning")
                yield _sse({"type": "mapping_issues", "issues": all_mapping_issues})

            yield _sse({"type": "done", "logs": all_logs})

        except Exception as e:
            yield _sse({"type": "error", "message": str(e)})

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/api/dags/list-airflow")
async def list_airflow_dags():
    """List DAGs from airflow CLI."""
    config = _load_dag_config()
    container = config.get("container_name", "")
    if not container:
        raise HTTPException(status_code=400, detail="Docker container name not configured.")

    cmd = ["docker", "exec", container, "airflow", "dags", "list", "-o", "json"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            raise HTTPException(status_code=500, detail=result.stderr or "Failed to list DAGs")
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            return {"raw": result.stdout}
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Timeout listing DAGs from Airflow")
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="Docker not found")


# ── DAG run history ───────────────────────────────────────────────────────────

@app.get("/api/dag-runs")
async def list_dag_runs(dag_id: Optional[str] = None):
    runs = _load_json(DAG_RUNS_FILE)
    if dag_id:
        runs = [r for r in runs if r.get("dag_id") == dag_id]
    return runs


# ── Mapping issue parsing & endpoints ─────────────────────────────────────────

def _parse_mapping_issues(log_text: str) -> List[Dict[str, str]]:
    """Parse mapping issue lines from Airflow logs.
    Expected format: WARNING - Unmapped value 'X' for column 'Y'
    Adjust the regex pattern to match your actual log format.
    """
    issues = []
    # Pattern: adjust to match your actual Airflow log format
    patterns = [
        r"(?:WARNING|WARN)\s*[-:]\s*[Uu]nmapped\s+value\s+['\"]([^'\"]+)['\"]\s+(?:for|in)\s+column\s+['\"]([^'\"]+)['\"]",
        r"(?:WARNING|WARN)\s*[-:]\s*[Mm]apping\s+not\s+found\s+(?:for\s+)?['\"]([^'\"]+)['\"]\s+(?:in|for)\s+['\"]([^'\"]+)['\"]",
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, log_text):
            issues.append({
                "unmapped_value": match.group(1),
                "column": match.group(2),
            })
    return issues


@app.get("/api/mapping-issues")
async def list_mapping_issues(resource_id: Optional[str] = None):
    issues = _load_json(MAPPING_ISSUES_FILE)
    if resource_id:
        issues = [i for i in issues if i.get("resource_id") == resource_id]
    return issues


@app.post("/api/mapping-issues")
async def save_mapping_issues(issues: List[Dict], resource_id: str, dag_run_id: str):
    """Save parsed mapping issues, auto-tagging fixed ones."""
    existing = _load_json(MAPPING_ISSUES_FILE)
    now = datetime.now(timezone.utc).isoformat()

    # Current unmapped (value, column) pairs from this run
    new_pairs = {(i["unmapped_value"], i["column"]) for i in issues}

    # Tag existing issues as fixed if they no longer appear
    for ex in existing:
        if ex.get("resource_id") != resource_id or ex.get("resolved"):
            continue
        pair = (ex["unmapped_value"], ex["column"])
        if pair not in new_pairs and not ex.get("fixed_in_rerun"):
            ex["fixed_in_rerun"] = now[:10]  # date only

    # Add new issues that don't already exist
    existing_pairs = {(e["unmapped_value"], e["column"]) for e in existing if e.get("resource_id") == resource_id}
    for issue in issues:
        pair = (issue["unmapped_value"], issue["column"])
        if pair not in existing_pairs:
            existing.append({
                "id": str(uuid.uuid4()),
                "resource_id": resource_id,
                "dag_run_id": dag_run_id,
                "unmapped_value": issue["unmapped_value"],
                "column": issue["column"],
                "first_seen": now,
                "fixed_in_rerun": None,
                "resolved": False,
            })
        else:
            # Re-appeared: clear fixed_in_rerun tag
            for ex in existing:
                if ex.get("resource_id") == resource_id and ex["unmapped_value"] == issue["unmapped_value"] and ex["column"] == issue["column"]:
                    ex["fixed_in_rerun"] = None

    _save_json(MAPPING_ISSUES_FILE, existing)
    return [i for i in existing if i.get("resource_id") == resource_id]


@app.put("/api/mapping-issues/{issue_id}/resolve")
async def resolve_mapping_issue(issue_id: str, body: MappingIssueResolve):
    issues = _load_json(MAPPING_ISSUES_FILE)
    for i in issues:
        if i["id"] == issue_id:
            i["resolved"] = body.resolved
            _save_json(MAPPING_ISSUES_FILE, issues)
            return i
    raise HTTPException(status_code=404, detail="Mapping issue not found")


# ── Integration Rule endpoints ────────────────────────────────────────────────

@app.get("/api/rules")
async def list_rules() -> List[Dict]:
    return _load_json(RULES_FILE)


@app.post("/api/rules", status_code=201)
async def create_rule(body: IntegrationRuleCreate) -> Dict:
    rules = _load_json(RULES_FILE)
    rule = {
        "id": str(uuid.uuid4()),
        "description": body.description,
        "resource_ids": body.resource_ids,
        "checked": False,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    rules.append(rule)
    _save_json(RULES_FILE, rules)
    return rule


@app.put("/api/rules/{rule_id}")
async def update_rule(rule_id: str, body: IntegrationRuleUpdate) -> Dict:
    rules = _load_json(RULES_FILE)
    for i, r in enumerate(rules):
        if r["id"] == rule_id:
            if body.description is not None:
                r["description"] = body.description
            if body.resource_ids is not None:
                r["resource_ids"] = body.resource_ids
            if body.checked is not None:
                r["checked"] = body.checked
            rules[i] = r
            _save_json(RULES_FILE, rules)
            return r
    raise HTTPException(status_code=404, detail="Rule not found")


@app.delete("/api/rules/{rule_id}", status_code=204)
async def delete_rule(rule_id: str):
    rules = _load_json(RULES_FILE)
    updated = [r for r in rules if r["id"] != rule_id]
    if len(updated) == len(rules):
        raise HTTPException(status_code=404, detail="Rule not found")
    _save_json(RULES_FILE, updated)


# ── QA Example ID endpoints ──────────────────────────────────────────────────

@app.get("/api/qa-examples")
async def list_qa_examples(resource_id: Optional[str] = None):
    examples = _load_json(QA_EXAMPLES_FILE)
    if resource_id:
        examples = [e for e in examples if e.get("resource_id") == resource_id]
    return examples


@app.post("/api/qa-examples", status_code=201)
async def create_qa_example(body: QAExampleIDCreate) -> Dict:
    examples = _load_json(QA_EXAMPLES_FILE)
    example = {
        "id": str(uuid.uuid4()),
        "resource_id": body.resource_id,
        "id_column": body.id_column,
        "id_value": body.id_value,
        "label": body.label,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    examples.append(example)
    _save_json(QA_EXAMPLES_FILE, examples)
    return example


@app.delete("/api/qa-examples/{example_id}", status_code=204)
async def delete_qa_example(example_id: str):
    examples = _load_json(QA_EXAMPLES_FILE)
    updated = [e for e in examples if e["id"] != example_id]
    if len(updated) == len(examples):
        raise HTTPException(status_code=404, detail="QA example not found")
    _save_json(QA_EXAMPLES_FILE, updated)


# ── ID column detection ──────────────────────────────────────────────────────

@app.post("/api/blobs/id-columns")
async def get_id_columns(request: ProfileRequest):
    """Find all columns containing 'id' (case-insensitive) across selected blobs."""
    try:
        client = build_container_client(request.container_url)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    id_columns: set = set()
    for blob_name in request.blob_names:
        try:
            raw = client.get_blob_client(blob_name).download_blob().readall()
            df = pd.read_csv(io.BytesIO(raw), nrows=0)
            for col in df.columns:
                if "id" in col.lower():
                    id_columns.add(col)
        except Exception:
            continue

    return sorted(id_columns)


class ColumnValuesRequest(BaseModel):
    container_url: str
    blob_names: List[str]
    column: str
    limit: int = 500


@app.post("/api/blobs/column-values")
async def get_column_values_v2(request: ColumnValuesRequest):
    """Get distinct values for a specific column across selected blobs."""
    try:
        client = build_container_client(request.container_url)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    all_values: set = set()
    for blob_name in request.blob_names:
        try:
            raw = client.get_blob_client(blob_name).download_blob().readall()
            df = pd.read_csv(io.BytesIO(raw))
            if request.column in df.columns:
                vals = df[request.column].dropna().astype(str).unique()
                all_values.update(vals)
        except Exception:
            continue

    sorted_values = sorted(all_values)
    return {"values": sorted_values[:request.limit], "total": len(sorted_values)}


@app.post("/api/blobs/list")
async def list_blobs(request: BlobListRequest) -> List[BlobInfo]:
    """List all CSV blobs in the container, optionally filtered by last_modified date."""
    from datetime import datetime, timezone
    try:
        date_from = (
            datetime.fromisoformat(request.date_from).replace(tzinfo=timezone.utc)
            if request.date_from else None
        )
        date_to = (
            datetime.fromisoformat(request.date_to).replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
            if request.date_to else None
        )

        client = build_container_client(request.container_url)
        blobs: List[BlobInfo] = []
        for blob in client.list_blobs(name_starts_with=request.prefix or None):
            if not blob.name.lower().endswith(".csv"):
                continue
            lm = blob.last_modified
            if lm:
                if date_from and lm < date_from:
                    continue
                if date_to and lm > date_to:
                    continue
            blobs.append(BlobInfo(
                name=blob.name,
                size=blob.size or 0,
                last_modified=lm.isoformat() if lm else "",
                detected_stage=detect_stage(blob.name),
            ))
        return blobs
    except AzureError as e:
        raise HTTPException(status_code=400, detail=f"Azure error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/blobs/columns")
async def get_blob_columns(request: ColumnsRequest) -> List[str]:
    """Return column names of a single CSV blob (reads only the first row)."""
    try:
        client = build_container_client(request.container_url)
        blob_client = client.get_blob_client(request.blob_name)
        raw = blob_client.download_blob().readall()
        df = pd.read_csv(io.BytesIO(raw), nrows=0)
        return list(df.columns)
    except AzureError as e:
        raise HTTPException(status_code=400, detail=f"Azure error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/blobs/preview")
async def preview_blob(request: PreviewRequest):
    """Return the first N rows of a CSV blob as JSON."""
    try:
        client = build_container_client(request.container_url)
        blob_client = client.get_blob_client(request.blob_name)
        raw = blob_client.download_blob().readall()
        df = pd.read_csv(io.BytesIO(raw), nrows=request.limit)
        return {
            "columns": list(df.columns),
            "rows": json.loads(df.to_json(orient="records")),
            "total_rows_loaded": len(df),
        }
    except AzureError as e:
        raise HTTPException(status_code=400, detail=f"Azure error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/blobs/profile")
async def profile_blobs(request: ProfileRequest) -> List[Dict]:
    """Return row count and per-column cardinality for each blob."""
    try:
        client = build_container_client(request.container_url)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    results = []
    for blob_name in request.blob_names:
        try:
            raw = client.get_blob_client(blob_name).download_blob().readall()
            df = pd.read_csv(io.BytesIO(raw))

            col_profiles = []
            for col in df.columns:
                distinct = int(df[col].nunique(dropna=False))
                vc = None
                if distinct < 10:
                    series = df[col].value_counts(dropna=False)
                    vc = {}
                    for k, v in series.items():
                        key = "null" if (k is None or (isinstance(k, float) and pd.isna(k))) else str(k)
                        vc[key] = int(v)
                col_profiles.append({
                    "name": col,
                    "distinct_count": distinct,
                    "value_counts": vc,
                })

            results.append({
                "blob_name": blob_name,
                "detected_stage": detect_stage(blob_name) or "unknown",
                "row_count": len(df),
                "columns": col_profiles,
            })
        except Exception as e:
            results.append({
                "blob_name": blob_name,
                "detected_stage": detect_stage(blob_name) or "unknown",
                "row_count": -1,
                "columns": [],
                "error": str(e),
            })
    return results


@app.post("/api/blobs/profile/stream")
async def profile_blobs_stream(request: ProfileRequest) -> StreamingResponse:
    """Stream SSE events while profiling blobs one by one."""

    async def generate() -> AsyncGenerator[str, None]:
        try:
            client = build_container_client(request.container_url)
        except Exception as e:
            yield _sse({"type": "error", "message": str(e)})
            return

        total = len(request.blob_names)
        for i, blob_name in enumerate(request.blob_names):
            yield _sse({
                "type": "progress",
                "current": i + 1,
                "total": total,
                "blob_name": blob_name,
            })
            try:
                raw = client.get_blob_client(blob_name).download_blob().readall()
                df = pd.read_csv(io.BytesIO(raw))

                col_profiles = []
                for col in df.columns:
                    distinct = int(df[col].nunique(dropna=False))
                    vc = None
                    if distinct < 10:
                        series = df[col].value_counts(dropna=False)
                        vc = {}
                        for k, v in series.items():
                            key = "null" if (k is None or (isinstance(k, float) and pd.isna(k))) else str(k)
                            vc[key] = int(v)
                    col_profiles.append({
                        "name": col,
                        "distinct_count": distinct,
                        "value_counts": vc,
                    })

                yield _sse({
                    "type": "profile",
                    "data": {
                        "blob_name": blob_name,
                        "detected_stage": detect_stage(blob_name) or "unknown",
                        "row_count": len(df),
                        "columns": col_profiles,
                    },
                })
            except Exception as e:
                yield _sse({
                    "type": "profile",
                    "data": {
                        "blob_name": blob_name,
                        "detected_stage": detect_stage(blob_name) or "unknown",
                        "row_count": -1,
                        "columns": [],
                        "error": str(e),
                    },
                })

        yield _sse({"type": "done"})

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


def _apply_single_filter(df: pd.DataFrame, f: FilterCondition) -> pd.DataFrame:
    """Apply one filter condition to a DataFrame and return the filtered result."""
    if f.column not in df.columns:
        return df  # caller handles the warning

    col = df[f.column]

    if f.filter_type == "regex":
        try:
            mask = col.astype(str).str.contains(f.value, regex=True, na=False)
            return df[mask]
        except re.error:
            # Invalid regex — return unchanged so caller can warn
            raise ValueError(f"Invalid regex: {f.value!r}")
    else:
        # equals — try numeric cast, fall back to string comparison
        try:
            cast_value = col.dtype.type(f.value)
            return df[col == cast_value]
        except (ValueError, TypeError):
            return df[col.astype(str) == f.value]


@app.post("/api/analyze")
async def analyze(request: AnalyzeRequest) -> StreamingResponse:
    """
    Stream SSE events while downloading selected blobs to a tmp folder,
    merging and processing per stage, then building the row-flow report.

    Event shapes:
      {"type": "log",    "level": "info|success|warning|error", "message": "..."}
      {"type": "result", "data": {...}, "downloaded_files": [...], "tmp_dir": "..."}
      {"type": "error",  "message": "..."}
    """
    if not request.key_columns:
        raise HTTPException(status_code=400, detail="At least one key column is required.")
    if not request.selected_blobs:
        raise HTTPException(status_code=400, detail="No blobs selected.")

    async def generate() -> AsyncGenerator[str, None]:
        tmp_dir = tempfile.mkdtemp(prefix="dataflow_")
        downloaded_files: List[str] = []

        try:
            yield _log("Connecting to Azure container…")
            client = build_container_client(request.container_url)
            yield _log("Connected.", "success")

            # ── 1. Group blobs by stage ────────────────────────────────────────
            stage_blobs: Dict[str, List[str]] = {}
            for blob_name in request.selected_blobs:
                stage = detect_stage(blob_name) or "unknown"
                stage_blobs.setdefault(stage, []).append(blob_name)

            yield _log(
                f"Grouped {len(request.selected_blobs)} file(s) into "
                f"{len(stage_blobs)} stage(s): {', '.join(stage_blobs.keys())}."
            )

            # ── 2. Download → disk, read, merge, filter, dedup per stage ──────
            stage_dfs: Dict[str, pd.DataFrame] = {}
            dedup_warnings: Dict[str, Dict[str, int]] = {}

            for stage, blobs in stage_blobs.items():
                dfs = []

                for blob_name in blobs:
                    yield _log(f"[{stage}] Downloading {blob_name}…")
                    blob_client = client.get_blob_client(blob_name)

                    # Flatten folder separators so the local filename is safe
                    safe_name = blob_name.replace("/", "__").replace("\\", "__")
                    local_path = os.path.join(tmp_dir, safe_name)

                    raw = blob_client.download_blob().readall()
                    with open(local_path, "wb") as fh:
                        fh.write(raw)

                    downloaded_files.append(local_path)
                    size_kb = len(raw) / 1024
                    yield _log(
                        f"[{stage}] Saved → {local_path}  ({size_kb:.1f} KB)",
                        "success",
                    )

                    df = pd.read_csv(local_path)
                    yield _log(
                        f"[{stage}] {blob_name}: {len(df)} rows, {len(df.columns)} columns."
                    )
                    dfs.append(df)

                yield _log(f"[{stage}] Merging {len(dfs)} file(s)…")
                merged = pd.concat(dfs, ignore_index=True)
                yield _log(f"[{stage}] {len(merged)} rows after merge.", "success")

                # Apply filters (AND: sequential reduction; OR: union of individual results)
                active_filters = [f for f in request.filters if f.column and f.value]
                if active_filters:
                    if request.filter_logic == "OR":
                        parts: List[pd.DataFrame] = []
                        for f in active_filters:
                            if f.column not in merged.columns:
                                yield _log(
                                    f"[{stage}] Filter column '{f.column}' not found — skipped.",
                                    "warning",
                                )
                                continue
                            try:
                                part = _apply_single_filter(merged, f)
                                parts.append(part)
                                yield _log(
                                    f"[{stage}] Filter '{f.column} {'~' if f.filter_type == 'regex' else '='} {f.value}'"
                                    f" matched {len(part)} row(s)."
                                )
                            except ValueError as exc:
                                yield _log(f"[{stage}] {exc} — skipped.", "warning")
                        if parts:
                            merged = pd.concat(parts).drop_duplicates()
                        yield _log(
                            f"[{stage}] OR filter → {len(merged)} rows remain.", "success"
                        )
                    else:  # AND
                        for f in active_filters:
                            if f.column not in merged.columns:
                                yield _log(
                                    f"[{stage}] Filter column '{f.column}' not found — skipped.",
                                    "warning",
                                )
                                continue
                            before = len(merged)
                            try:
                                merged = _apply_single_filter(merged, f)
                            except ValueError as exc:
                                yield _log(f"[{stage}] {exc} — skipped.", "warning")
                                continue
                            yield _log(
                                f"[{stage}] Filter '{f.column} {'~' if f.filter_type == 'regex' else '='} {f.value}'"
                                f" → {len(merged)} rows remain (was {before})."
                            )

                # Deduplication
                if request.deduplicate:
                    valid_keys = [k for k in request.key_columns if k in merged.columns]
                    if valid_keys:
                        before = len(merged)
                        merged = merged.drop_duplicates(subset=valid_keys, keep="first")
                        after = len(merged)
                        if before > after:
                            removed = before - after
                            dedup_warnings[stage] = {"removed": removed, "kept": after}
                            yield _log(
                                f"[{stage}] Deduplicated: {removed} duplicate(s) removed,"
                                f" {after} row(s) kept.",
                                "warning",
                            )
                        else:
                            yield _log(f"[{stage}] No duplicates found.")

                stage_dfs[stage] = merged

            # ── 3. Collect all unique key tuples ──────────────────────────────
            yield _log("Building flow report…")

            all_key_tuples: set = set()
            for stage, df in stage_dfs.items():
                valid_keys = [k for k in request.key_columns if k in df.columns]
                if not valid_keys:
                    continue
                for _, row in df.iterrows():
                    key_tuple = tuple((k, str(row[k])) for k in valid_keys)
                    all_key_tuples.add(key_tuple)

            yield _log(
                f"Found {len(all_key_tuples)} unique record(s) matching the filters."
            )

            # ── 4. Build per-record flow ───────────────────────────────────────
            ordered_stages = (
                [s for s in STAGE_ORDER if s in stage_dfs]
                + [s for s in stage_dfs if s not in STAGE_ORDER]
            )

            rows = []
            for key_tuple in sorted(all_key_tuples):
                key_dict = dict(key_tuple)
                flow: Dict[str, Any] = {}
                missing_stages: List[str] = []
                last_seen_stage: Optional[str] = None

                for stage in ordered_stages:
                    df = stage_dfs[stage]
                    valid_keys = [k for k in request.key_columns if k in df.columns]

                    if not valid_keys:
                        missing_stages.append(stage)
                        continue

                    mask = pd.Series(True, index=df.index)
                    for k in valid_keys:
                        if k in key_dict:
                            mask &= df[k].astype(str) == key_dict[k]

                    matches = df[mask]
                    if matches.empty:
                        missing_stages.append(stage)
                    else:
                        flow[stage] = json.loads(
                            matches.iloc[[0]].to_json(orient="records")
                        )[0]
                        last_seen_stage = stage

                rows.append({
                    "key_value": key_dict,
                    "flow": flow,
                    "missing_stages": missing_stages,
                    "last_seen_stage": last_seen_stage,
                    "dedup_warnings": {
                        s: dedup_warnings[s] for s in dedup_warnings if s in flow
                    },
                })

            columns_per_stage = {
                stage: list(df.columns) for stage, df in stage_dfs.items()
            }

            yield _log(
                f"Done — {len(rows)} record(s) traced across"
                f" {len(ordered_stages)} stage(s).",
                "success",
            )

            # ── 5. Emit final result ───────────────────────────────────────────
            yield _sse({
                "type": "result",
                "data": {
                    "stages": ordered_stages,
                    "key_columns": request.key_columns,
                    "rows": rows,
                    "columns": columns_per_stage,
                    "dedup_warnings": dedup_warnings,
                },
                "downloaded_files": downloaded_files,
                "tmp_dir": tmp_dir,
            })

        except AzureError as e:
            yield _sse({"type": "error", "message": f"Azure error: {str(e)}"})
        except Exception as e:
            yield _sse({"type": "error", "message": str(e)})

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
