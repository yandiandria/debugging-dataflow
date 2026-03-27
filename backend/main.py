import asyncio
import base64
import hashlib
import io
import json
import os
import random
import re
import subprocess
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple
from urllib.parse import quote

import httpx
import pandas as pd
import polars as pl
from azure.core.exceptions import AzureError
from azure.storage.blob import ContainerClient
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from notion_client import AsyncClient as NotionAsyncClient
from pydantic import BaseModel

# Resolved after DATA_DIR is set below — see the DATA_DIR block.

_MAX_DAG_RUNS = 200  # cap dag_runs.json from growing unbounded

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
    allow_origins=["http://localhost:3001", "http://127.0.0.1:3001"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
    allow_headers=["Content-Type"],
)

# ── In-memory caches ───────────────────────────────────────────────────────────

# Cache for `airflow dags list` — avoids a costly docker exec on every page load.
# TTL: 5 minutes.  Invalidated by POST /api/dags/list-airflow?force_refresh=true.
_airflow_dags_cache: Optional[Dict] = None
_AIRFLOW_DAGS_CACHE_TTL = 300  # seconds


# ── Persistence helpers ─────────────────────────────────────────────────────────

# DATA_DIR can be overridden via the DATAFLOW_DATA_DIR environment variable so
# that the JSON files can live on a mounted volume (e.g. a Docker bind-mount)
# and survive container restarts.
#
#   export DATAFLOW_DATA_DIR=/mnt/data/dataflow   # or docker-compose volume path
#
# If not set, defaults to the directory containing this file (original behaviour).
DATA_DIR = Path(os.environ.get("DATAFLOW_DATA_DIR", "")).expanduser() if os.environ.get("DATAFLOW_DATA_DIR") else Path(__file__).parent
DATA_DIR.mkdir(parents=True, exist_ok=True)

# These live inside DATA_DIR so they survive on Docker volumes when
# DATAFLOW_DATA_DIR is set (previously they were hardcoded relative paths).
BLOB_CACHE_DIR = DATA_DIR / ".cache" / "dataflow" / "blobs"
HISTORY_DIR = DATA_DIR / "history"

RESOURCES_FILE = DATA_DIR / "resources.json"
DAGS_FILE = DATA_DIR / "dags.json"
DAG_CONFIG_FILE = DATA_DIR / "dag_config.json"
RULES_FILE = DATA_DIR / "rules.json"
DAG_RUNS_FILE = DATA_DIR / "dag_runs.json"
MAPPING_ISSUES_FILE = DATA_DIR / "mapping_issues.json"
QA_EXAMPLES_FILE = DATA_DIR / "qa_examples.json"
NOTION_CONFIG_FILE = DATA_DIR / "notion_config.json"


def _load_json(path: Path) -> List[Dict]:
    if not path.exists():
        return []
    return json.loads(path.read_text())


def _save_json(path: Path, data: List[Dict]) -> None:
    # Write to a sibling .tmp file then rename so a crash mid-write never
    # leaves a corrupted JSON file behind (rename is atomic on POSIX).
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    tmp.replace(path)


def _load_resources() -> List[Dict]:
    return _load_json(RESOURCES_FILE)


def _save_resources(resources: List[Dict]) -> None:
    _save_json(RESOURCES_FILE, resources)


def _load_dag_config() -> Dict:
    if not DAG_CONFIG_FILE.exists():
        return {"container_name": "", "environments": ["dev", "staging", "prod"]}
    return json.loads(DAG_CONFIG_FILE.read_text())


def _save_dag_config(config: Dict) -> None:
    tmp = DAG_CONFIG_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(config, indent=2))
    tmp.replace(DAG_CONFIG_FILE)


def _load_notion_config() -> Dict:
    if not NOTION_CONFIG_FILE.exists():
        return {"token": "", "database_id": ""}
    return json.loads(NOTION_CONFIG_FILE.read_text())


def _save_notion_config(config: Dict) -> None:
    tmp = NOTION_CONFIG_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(config, indent=2))
    tmp.replace(NOTION_CONFIG_FILE)


# Notion property types that can be written via the API
_NOTION_EDITABLE = {"title", "rich_text", "number", "select", "multi_select", "date", "checkbox", "url", "email", "phone_number"}


def _extract_prop_value(prop: Dict) -> Any:
    ptype = prop["type"]
    val = prop.get(ptype)
    if ptype in ("title", "rich_text"):
        return val[0]["plain_text"] if val else ""
    if ptype == "number":
        return val
    if ptype == "select":
        return val["name"] if val else None
    if ptype == "multi_select":
        return [o["name"] for o in val] if val else []
    if ptype == "date":
        return val["start"] if val else None
    if ptype == "checkbox":
        return bool(val)
    if ptype in ("url", "email", "phone_number"):
        return val
    if ptype in ("created_time", "last_edited_time"):
        return val
    if ptype == "formula" and isinstance(val, dict):
        ft = val.get("type")
        return val.get(ft)
    return None


def _build_prop_payload(ptype: str, value: Any) -> Dict:
    if ptype in ("title", "rich_text"):
        return {ptype: [{"text": {"content": str(value or "")}}]}
    if ptype == "number":
        return {"number": float(value) if value not in (None, "") else None}
    if ptype == "select":
        return {"select": {"name": value} if value else None}
    if ptype == "multi_select":
        vals = value if isinstance(value, list) else [v.strip() for v in str(value).split(",") if v.strip()] if value else []
        return {"multi_select": [{"name": v} for v in vals]}
    if ptype == "date":
        return {"date": {"start": value} if value else None}
    if ptype == "checkbox":
        return {"checkbox": bool(value)}
    if ptype in ("url", "email", "phone_number"):
        return {ptype: value or None}
    raise ValueError(f"Cannot edit property of type: {ptype}")


def _page_to_dict(page: Dict) -> Dict:
    props = {name: _extract_prop_value(prop) for name, prop in page["properties"].items()}
    return {
        "id": page["id"],
        "url": page.get("url", ""),
        "created_time": page.get("created_time", ""),
        "last_edited_time": page.get("last_edited_time", ""),
        "properties": props,
    }


# ── Models ─────────────────────────────────────────────────────────────────────

class Resource(BaseModel):
    id: str
    technical_name: str
    business_name: str
    created_at: str
    extract_prefixes: List[str] = []
    dag_ids: List[str] = []


class ResourceCreate(BaseModel):
    technical_name: str
    business_name: str
    extract_prefixes: List[str] = []


class BlobListRequest(BaseModel):
    container_url: str
    prefix: Optional[str] = None           # Azure-side path prefix filter (all stages)
    extract_prefixes: List[str] = []       # Extra prefixes — only extract-stage blobs kept
    date_from: Optional[str] = None        # ISO date string, e.g. "2024-01-15"
    date_to: Optional[str] = None          # ISO date string, inclusive


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
    force_refresh: bool = False  # bypass blob cache


class HistorySummary(BaseModel):
    id: str
    saved_at: str
    container_url: str
    blob_count: int
    key_columns: List[str]


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


class AirflowRestAPIConfig(BaseModel):
    base_url: str
    username: str
    password: str
    dag_id: str
    verify_tls: bool = True


class NotionConfigUpdate(BaseModel):
    token: str
    database_id: str


class NotionPageCreate(BaseModel):
    properties: Dict[str, Any]


class NotionPageUpdate(BaseModel):
    properties: Dict[str, Any]


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
        extract_prefixes=body.extract_prefixes,
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
            r["extract_prefixes"] = body.extract_prefixes
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


# ── Docker helpers ────────────────────────────────────────────────────────────

@app.get("/api/docker/containers")
async def list_docker_containers():
    """Return running Docker containers so the UI can offer a picker.

    Each item has: id (short), name, image, status.
    Containers whose name or image contains 'airflow' are flagged with
    is_airflow=True so the UI can highlight them.
    """
    try:
        proc = await asyncio.create_subprocess_exec(
            "docker", "ps", "--format", "{{.ID}}\t{{.Names}}\t{{.Image}}\t{{.Status}}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
        except asyncio.TimeoutError:
            proc.kill()
            raise HTTPException(status_code=504, detail="Timeout listing Docker containers")

        containers = []
        for line in stdout.decode().strip().splitlines():
            parts = line.split("\t", 3)
            if len(parts) < 3:
                continue
            cid, name, image, status = parts[0], parts[1], parts[2], parts[3] if len(parts) > 3 else ""
            containers.append({
                "id": cid,
                "name": name,
                "image": image,
                "status": status,
                "is_airflow": "airflow" in name.lower() or "airflow" in image.lower(),
            })
        return containers
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="Docker not found")


# ── Config export / import ─────────────────────────────────────────────────────

_CONFIG_FILES = {
    "resources": RESOURCES_FILE,
    "dags": DAGS_FILE,
    "dag_config": DAG_CONFIG_FILE,
    "rules": RULES_FILE,
    "mapping_issues": MAPPING_ISSUES_FILE,
    "qa_examples": QA_EXAMPLES_FILE,
}


@app.get("/api/config/export")
async def export_config():
    """Download all configuration as a single JSON bundle.

    dag_runs are intentionally excluded (they are run history, not config).
    """
    bundle: Dict[str, Any] = {"exported_at": datetime.now(timezone.utc).isoformat()}
    for key, path in _CONFIG_FILES.items():
        try:
            bundle[key] = json.loads(path.read_text()) if path.exists() else []
        except Exception:
            bundle[key] = []
    return bundle


class ConfigImportBody(BaseModel):
    resources: Optional[List[Dict]] = None
    dags: Optional[List[Dict]] = None
    dag_config: Optional[Dict] = None
    rules: Optional[List[Dict]] = None
    mapping_issues: Optional[List[Dict]] = None
    qa_examples: Optional[List[Dict]] = None


@app.post("/api/config/import")
async def import_config(body: ConfigImportBody):
    """Restore configuration from an export bundle.

    Only keys present in the body are written; omitted keys are left untouched.
    dag_runs are never overwritten by an import.
    """
    written: List[str] = []
    if body.resources is not None:
        _save_json(RESOURCES_FILE, body.resources); written.append("resources")
    if body.dags is not None:
        _save_json(DAGS_FILE, body.dags); written.append("dags")
    if body.dag_config is not None:
        _save_dag_config(body.dag_config); written.append("dag_config")
    if body.rules is not None:
        _save_json(RULES_FILE, body.rules); written.append("rules")
    if body.mapping_issues is not None:
        _save_json(MAPPING_ISSUES_FILE, body.mapping_issues); written.append("mapping_issues")
    if body.qa_examples is not None:
        _save_json(QA_EXAMPLES_FILE, body.qa_examples); written.append("qa_examples")
    return {"restored": written}


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
                # Try to extract run_id from output.
                # Airflow 2.x format: "Created <DagRun dag_id @ exec_date: run_id, externally triggered: True>"
                run_id = ""
                for line in stdout_text.split("\n"):
                    # Primary: parse the DagRun repr produced by `airflow dags trigger`
                    match = re.search(r"<DagRun [^>]+ @ [^:]+:\s+([^,>]+)", line)
                    if match:
                        run_id = match.group(1).strip()
                        break
                    # Fallback: explicit run_id= / run_id: pattern
                    match = re.search(r"run_id[=:\s]+(\S+)", line)
                    if match:
                        run_id = match.group(1).strip(",").strip("'\"")
                        break

                # Save run record, pruning old entries to cap file size
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
                # Keep only the most recent _MAX_DAG_RUNS entries
                if len(runs) > _MAX_DAG_RUNS:
                    runs = runs[-_MAX_DAG_RUNS:]
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


async def _fetch_single_task_log(
    client: httpx.AsyncClient,
    auth_header: str,
    log_url: str,
    params: Dict,
    task_id: str,
    map_index: int,
    try_number: int,
    state: str,
    max_retries: int = 3,
) -> Dict[str, Any]:
    """Fetch log for a single task instance, with retry on 404 (log not yet available)."""
    log_text = ""
    log_status = "not_found"
    for attempt in range(max_retries):
        try:
            log_resp = await client.get(log_url, params=params, headers={"Authorization": auth_header})
            if log_resp.status_code == 200:
                log_text = log_resp.text
                log_status = "success"
                break
            elif log_resp.status_code == 404:
                if attempt < max_retries - 1:
                    await asyncio.sleep(2)
                else:
                    log_status = "not_found"
            else:
                log_status = f"error_{log_resp.status_code}"
                break
        except Exception:
            log_status = "exception"
            break
    return {
        "task_id": task_id,
        "map_index": map_index,
        "try_number": try_number,
        "state": state,
        "logs": log_text,
        "status": log_status,
    }


@app.post("/api/dags/logs-rest-api")
async def get_dag_logs_rest_api(body: AirflowRestAPIConfig) -> StreamingResponse:
    """Fetch Airflow logs using the REST API instead of Docker/filesystem.

    This implementation follows the Airflow REST API contract:
    1. Get the latest DAG run via /api/v1/dags/{dag_id}/dagRuns
    2. List all task instances with pagination via /api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances
    3. Fetch logs for each task via /api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{try_number}

    Handles:
    - Mapped task instances (using map_index)
    - Pagination of task instances
    - Proper URL encoding of dag_id, run_id, and task_id
    - Latest attempt (try_number) per task instance
    - Plain text logs (not JSON)
    """

    async def generate() -> AsyncGenerator[str, None]:
        try:
            # Build authorization header
            credentials = f"{body.username}:{body.password}"
            encoded_credentials = base64.b64encode(credentials.encode()).decode()
            auth_header = f"Basic {encoded_credentials}"

            # Strip trailing slash from base_url if present
            base_url = body.base_url.rstrip("/")

            async with httpx.AsyncClient(verify=body.verify_tls, timeout=30.0) as client:
                # Step 0: Verify health/connectivity
                yield _log("Verifying Airflow connectivity...", "info")
                try:
                    health_resp = await client.get(
                        f"{base_url}/api/v1/health",
                        headers={"Authorization": auth_header}
                    )
                    if health_resp.status_code == 401:
                        yield _log("Authentication failed (401). Check username/password.", "error")
                        return
                    if health_resp.status_code == 403:
                        yield _log("Access forbidden (403). Check permissions.", "error")
                        return
                    if health_resp.status_code != 200:
                        yield _log(f"Health check failed with status {health_resp.status_code}", "warning")
                except Exception as e:
                    yield _log(f"Failed to connect to Airflow at {base_url}: {e}", "error")
                    return

                yield _log("Connected to Airflow successfully", "success")

                # Step 1: Get the latest DAG run
                yield _log("Fetching latest DAG run...", "info")
                dag_id_encoded = quote(body.dag_id, safe="")

                try:
                    runs_resp = await client.get(
                        f"{base_url}/api/v1/dags/{dag_id_encoded}/dagRuns",
                        params={"limit": "1", "order_by": "-execution_date"},
                        headers={"Authorization": auth_header}
                    )
                    runs_resp.raise_for_status()
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 404:
                        yield _log(f"DAG '{body.dag_id}' not found", "error")
                    else:
                        yield _log(f"Failed to fetch DAG runs: {e.response.status_code}", "error")
                    return
                except Exception as e:
                    yield _log(f"Error fetching DAG runs: {e}", "error")
                    return

                runs_data = runs_resp.json()
                dag_runs = runs_data.get("dag_runs", [])

                if not dag_runs:
                    yield _log(f"No runs found for DAG '{body.dag_id}'", "warning")
                    return

                run_id = dag_runs[0].get("dag_run_id")
                yield _log(f"Found latest run: {run_id}", "success")

                # Step 2: List all task instances (with pagination)
                yield _log("Fetching all task instances...", "info")

                run_id_encoded = quote(run_id, safe="")
                all_task_instances = []
                offset = 0
                page_size = 100
                page_num = 0

                while True:
                    page_num += 1
                    try:
                        ti_resp = await client.get(
                            f"{base_url}/api/v1/dags/{dag_id_encoded}/dagRuns/{run_id_encoded}/taskInstances",
                            params={"limit": str(page_size), "offset": str(offset)},
                            headers={"Authorization": auth_header}
                        )
                        ti_resp.raise_for_status()
                    except httpx.HTTPStatusError as e:
                        yield _log(f"Failed to fetch task instances: {e.response.status_code}", "error")
                        return
                    except Exception as e:
                        yield _log(f"Error fetching task instances: {e}", "error")
                        return

                    ti_data = ti_resp.json()
                    task_instances = ti_data.get("task_instances", [])

                    if not task_instances:
                        break

                    all_task_instances.extend(task_instances)
                    yield _log(f"Page {page_num}: Fetched {len(task_instances)} task instances", "info")

                    if len(task_instances) < page_size:
                        break

                    offset += page_size

                yield _log(f"Total task instances: {len(all_task_instances)}", "success")

                # Step 3: Fetch all task logs concurrently (previously sequential,
                # which caused up to N * 3 * 2s wait for N tasks with 404 retries).
                yield _log(f"Fetching logs for {len(all_task_instances)} task instance(s) concurrently...", "info")

                fetch_coros = []
                for ti in all_task_instances:
                    task_id = ti.get("task_id", "")
                    try_number = ti.get("try_number", 1)
                    map_index = ti.get("map_index", -1)
                    task_id_encoded = quote(task_id, safe="")
                    log_url = (
                        f"{base_url}/api/v1/dags/{dag_id_encoded}/dagRuns/{run_id_encoded}/"
                        f"taskInstances/{task_id_encoded}/logs/{try_number}"
                    )
                    params: Dict = {}
                    if map_index >= 0:
                        params["map_index"] = str(map_index)
                    fetch_coros.append(_fetch_single_task_log(
                        client, auth_header, log_url, params,
                        task_id, map_index, try_number, ti.get("state", "unknown"),
                    ))

                all_logs: List[Dict[str, Any]] = list(await asyncio.gather(*fetch_coros))

                # Surface any tasks that had errors
                for log_entry in all_logs:
                    if log_entry["status"] not in ("success", "not_found"):
                        yield _log(f"Task {log_entry['task_id']}: {log_entry['status']}", "warning")

                yield _log("Log fetching complete", "success")
                yield _sse({
                    "type": "done",
                    "dag_id": body.dag_id,
                    "run_id": run_id,
                    "task_count": len(all_task_instances),
                    "logs": all_logs
                })

        except Exception as e:
            yield _sse({"type": "error", "message": str(e)})

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/api/dags/list-airflow")
async def list_airflow_dags(force_refresh: bool = False):
    """List DAGs from airflow CLI with in-memory TTL cache (5 min).

    Pass ?force_refresh=true to bypass the cache and re-query Airflow.
    The response includes `cached` (bool) and `fetched_at` (ISO timestamp)
    so the frontend can show how fresh the data is.
    """
    global _airflow_dags_cache

    now = time.monotonic()
    if (
        not force_refresh
        and _airflow_dags_cache is not None
        and now - _airflow_dags_cache["fetched_at_mono"] < _AIRFLOW_DAGS_CACHE_TTL
    ):
        return {**_airflow_dags_cache["payload"], "cached": True}

    config = _load_dag_config()
    container = config.get("container_name", "")
    if not container:
        raise HTTPException(status_code=400, detail="Docker container name not configured.")

    cmd = ["docker", "exec", container, "airflow", "dags", "list", "-o", "json"]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
        except asyncio.TimeoutError:
            proc.kill()
            raise HTTPException(status_code=504, detail="Timeout listing DAGs from Airflow")

        if proc.returncode != 0:
            raise HTTPException(status_code=500, detail=stderr.decode() or "Failed to list DAGs")

        fetched_at_iso = datetime.now(timezone.utc).isoformat()
        try:
            dags = json.loads(stdout.decode())
        except json.JSONDecodeError:
            dags = []

        payload = {"dags": dags, "fetched_at": fetched_at_iso}
        _airflow_dags_cache = {"payload": payload, "fetched_at_mono": now}
        return {**payload, "cached": False}

    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="Docker not found")


# ── DAG run history ───────────────────────────────────────────────────────────

_LOG_KEYS = {"task_logs", "task_sub_logs", "stdout", "stderr"}


def _strip_logs(run: Dict) -> Dict:
    """Return a copy of a run record with bulky log fields removed."""
    return {k: v for k, v in run.items() if k not in _LOG_KEYS}


@app.get("/api/dag-runs")
async def list_dag_runs(dag_id: Optional[str] = None, include_logs: bool = False):
    """List DAG runs.

    By default log fields (task_logs, task_sub_logs, stdout, stderr) are
    stripped from the response to keep it lightweight — the list is used for
    status/timestamp display only.  Pass ?include_logs=true when you need
    the full data (e.g. for restoring a previous run's log view).
    """
    runs = _load_json(DAG_RUNS_FILE)
    if dag_id:
        runs = [r for r in runs if r.get("dag_id") == dag_id]
    if not include_logs:
        runs = [_strip_logs(r) for r in runs]
    return runs


@app.get("/api/dag-runs/{run_id}")
async def get_dag_run(run_id: str):
    """Fetch a single DAG run with full log data."""
    runs = _load_json(DAG_RUNS_FILE)
    for r in runs:
        if r["id"] == run_id:
            return r
    raise HTTPException(status_code=404, detail="Run not found")


@app.patch("/api/dag-runs/{run_id}")
async def update_dag_run(run_id: str, body: Dict):
    runs = _load_json(DAG_RUNS_FILE)
    for r in runs:
        if r["id"] == run_id:
            r.update(body)
            _save_json(DAG_RUNS_FILE, runs)
            return r
    raise HTTPException(status_code=404, detail="Run not found")


@app.get("/api/dags/{dag_id}/latest-run-id")
async def get_latest_airflow_run_id(dag_id: str):
    """Return the most recent run_id for dag_id by querying Airflow directly."""
    config = _load_dag_config()
    container = config.get("container_name", "")
    if not container:
        raise HTTPException(status_code=400, detail="Docker container name not configured.")

    proc = await asyncio.create_subprocess_exec(
        "docker", "exec", container,
        "airflow", "dags", "list-runs", "-d", dag_id, "--output", "json",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await proc.communicate()

    runs: List[Dict] = []
    for line in stdout.decode().strip().split("\n"):
        stripped = line.strip()
        if stripped.startswith("["):
            try:
                parsed = json.loads(stripped)
                if isinstance(parsed, list):
                    runs = parsed
                    break
            except json.JSONDecodeError:
                pass

    if not runs:
        raise HTTPException(status_code=404, detail="No runs found for this DAG.")

    latest = sorted(runs, key=lambda r: r.get("execution_date", ""), reverse=True)[0]
    run_id = latest.get("run_id", "")
    if not run_id:
        raise HTTPException(status_code=404, detail="Could not determine run_id.")

    return {"run_id": run_id}


# ── Mapping issue parsing & endpoints ─────────────────────────────────────────

_AIRFLOW_NOISE_SUBSTRINGS = [
    "Could not import graphviz",
    "Rendering graph to the graphical format will not be possible",
]


def _is_airflow_noise(line: str) -> bool:
    return any(s in line for s in _AIRFLOW_NOISE_SUBSTRINGS)


def _parse_mapping_issues(log_text: str) -> List[Dict]:
    """Parse mapping issue lines from Airflow logs.
    Expected format:
    {map_and_log.py:NNN} WARNING - Les valeurs (...) (COUNT) ne sont pas présentes dans le mapping NAME.
    """
    issues = []
    pattern = r"\} WARNING - Les valeurs (\(.+?\)) \((\d+)\) ne sont pas présentes dans le mapping (.+?)\."
    for match in re.finditer(pattern, log_text):
        issues.append({
            "unmapped_value": match.group(1),
            "count": int(match.group(2)),
            "column": match.group(3),
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

    # Add new issues that don't already exist; update count if they reappear
    existing_pairs = {(e["unmapped_value"], e["column"]) for e in existing if e.get("resource_id") == resource_id}
    for issue in issues:
        pair = (issue["unmapped_value"], issue["column"])
        if pair not in existing_pairs:
            existing.append({
                "id": str(uuid.uuid4()),
                "resource_id": resource_id,
                "dag_run_id": dag_run_id,
                "unmapped_value": issue["unmapped_value"],
                "count": issue.get("count"),
                "column": issue["column"],
                "first_seen": now,
                "fixed_in_rerun": None,
                "resolved": False,
            })
        else:
            # Re-appeared: clear fixed_in_rerun tag and refresh count
            for ex in existing:
                if ex.get("resource_id") == resource_id and ex["unmapped_value"] == issue["unmapped_value"] and ex["column"] == issue["column"]:
                    ex["fixed_in_rerun"] = None
                    ex["count"] = issue.get("count")

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

    # Only sample from clean_cleaned blobs
    cleaned_blobs = [b for b in request.blob_names if detect_stage(b) == "clean_cleaned"]

    all_values: set = set()
    for blob_name in cleaned_blobs:
        try:
            # Stream lines and reservoir-sample 10 data rows (avoids loading full file)
            stream = client.get_blob_client(blob_name).download_blob()
            lines: list[bytes] = []
            header: bytes | None = None
            reservoir: list[bytes] = []
            n_seen = 0
            for chunk in stream.chunks():
                lines.extend(chunk.splitlines(keepends=True))
                # Process complete lines (keep last partial line for next iteration)
                complete, lines = lines[:-1], lines[-1:]
                for line in complete:
                    if header is None:
                        header = line
                        continue
                    n_seen += 1
                    if len(reservoir) < 10:
                        reservoir.append(line)
                    else:
                        j = random.randint(0, n_seen - 1)
                        if j < 10:
                            reservoir[j] = line
            # Handle any remaining partial line
            if lines and lines[0]:
                line = lines[0]
                if header is None:
                    header = line
                else:
                    n_seen += 1
                    if len(reservoir) < 10:
                        reservoir.append(line)
                    else:
                        j = random.randint(0, n_seen - 1)
                        if j < 10:
                            reservoir[j] = line

            if header is None or not reservoir:
                continue
            sample_bytes = header + b"".join(reservoir)
            df = pd.read_csv(io.BytesIO(sample_bytes), low_memory=False)
            if request.column in df.columns:
                vals = df[request.column].dropna().astype(str).unique()
                all_values.update(vals)
        except Exception:
            continue

    sorted_values = sorted(all_values)
    return {"values": sorted_values[:request.limit], "total": len(sorted_values)}


def _collect_blobs(
    client: ContainerClient,
    prefix: Optional[str],
    extract_only: bool,
    date_from: Optional[datetime],
    date_to: Optional[datetime],
    seen: set,
    blobs: List["BlobInfo"],
) -> None:
    """Append CSV blobs from *client* into *blobs*, skipping duplicates and applying date/stage filters."""
    for blob in client.list_blobs(name_starts_with=prefix):
        if not blob.name.lower().endswith(".csv"):
            continue
        lm = blob.last_modified
        if lm:
            if date_from and lm < date_from:
                continue
            if date_to and lm > date_to:
                continue
        if blob.name in seen:
            continue
        stage = detect_stage(blob.name)
        if extract_only and stage != "extract":
            continue
        seen.add(blob.name)
        blobs.append(BlobInfo(
            name=blob.name,
            size=blob.size or 0,
            last_modified=lm.isoformat() if lm else "",
            detected_stage=stage,
        ))


@app.post("/api/blobs/list")
async def list_blobs(request: BlobListRequest) -> List[BlobInfo]:
    """List all CSV blobs in the container, optionally filtered by last_modified date."""
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
        seen: set = set()
        blobs: List[BlobInfo] = []

        _collect_blobs(client, request.prefix or None, False, date_from, date_to, seen, blobs)
        for ep in request.extract_prefixes:
            ep = ep.strip()
            if ep:
                _collect_blobs(client, ep, True, date_from, date_to, seen, blobs)

        return blobs
    except AzureError as e:
        raise HTTPException(status_code=400, detail=f"Azure error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/resources/{resource_id}/blobs")
async def get_resource_blobs(
    resource_id: str,
    container_url: str,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> List[BlobInfo]:
    """List CSV blobs scoped to a single resource (by technical_name + extract_prefixes)."""
    resources_list = _load_resources()
    resource = next((r for r in resources_list if r["id"] == resource_id), None)
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")
    try:
        date_from_dt = (
            datetime.fromisoformat(date_from).replace(tzinfo=timezone.utc)
            if date_from else None
        )
        date_to_dt = (
            datetime.fromisoformat(date_to).replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
            if date_to else None
        )
        client = build_container_client(container_url)
        seen: set = set()
        blobs: List[BlobInfo] = []
        tech_name = resource.get("technical_name", "")
        extract_prefixes = resource.get("extract_prefixes") or []
        if tech_name:
            _collect_blobs(client, tech_name, False, date_from_dt, date_to_dt, seen, blobs)
        for ep in extract_prefixes:
            ep = ep.strip()
            if ep:
                _collect_blobs(client, ep, True, date_from_dt, date_to_dt, seen, blobs)
        return blobs
    except AzureError as e:
        raise HTTPException(status_code=400, detail=f"Azure error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/blobs/columns")
async def get_blob_columns(request: ColumnsRequest) -> List[str]:
    """Return column names of a CSV blob. Reads from local cache when available,
    otherwise fetches only the first 8 KB from Azure (enough for any header row)."""
    try:
        cache_path = _blob_cache_path(request.container_url, request.blob_name)
        if cache_path.exists():
            return pl.read_csv(cache_path, n_rows=0).columns
        client = build_container_client(request.container_url)
        blob_client = client.get_blob_client(request.blob_name)
        raw = await asyncio.to_thread(
            lambda: blob_client.download_blob(offset=0, length=8_192).readall()
        )
        return pl.read_csv(io.BytesIO(raw), n_rows=0, truncate_ragged_lines=True).columns
    except AzureError as e:
        raise HTTPException(status_code=400, detail=f"Azure error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/blobs/preview")
async def preview_blob(request: PreviewRequest):
    """Return the first N rows of a CSV blob. Reads from local cache when available,
    otherwise fetches only the first 512 KB from Azure."""
    try:
        cache_path = _blob_cache_path(request.container_url, request.blob_name)
        if cache_path.exists():
            df = pl.read_csv(cache_path, n_rows=request.limit, infer_schema_length=100)
        else:
            client = build_container_client(request.container_url)
            blob_client = client.get_blob_client(request.blob_name)
            raw = await asyncio.to_thread(
                lambda: blob_client.download_blob(offset=0, length=512 * 1024).readall()
            )
            # Drop the potentially truncated last line before parsing
            raw = raw[: raw.rfind(b"\n") + 1] if b"\n" in raw else raw
            df = pl.read_csv(io.BytesIO(raw), n_rows=request.limit, infer_schema_length=100)
        return {
            "columns": df.columns,
            "rows": df.to_dicts(),
            "total_rows_loaded": len(df),
        }
    except AzureError as e:
        raise HTTPException(status_code=400, detail=f"Azure error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _profile_one_blob_sync(blob_client, blob_name: str) -> dict:
    """Download and profile a single blob. Runs synchronously — call via asyncio.to_thread."""
    raw = blob_client.download_blob().readall()
    df = pl.read_csv(io.BytesIO(raw), infer_schema_length=10000)
    col_profiles = []
    for col in df.columns:
        distinct = df[col].n_unique()
        vc = None
        if distinct < 10:
            vc_df = df[col].value_counts(sort=True)
            vc = {
                ("null" if k is None else str(k)): int(count)
                for k, count in vc_df.iter_rows()
            }
        col_profiles.append({"name": col, "distinct_count": int(distinct), "value_counts": vc})
    return {
        "blob_name": blob_name,
        "detected_stage": detect_stage(blob_name) or "unknown",
        "row_count": len(df),
        "columns": col_profiles,
    }


@app.post("/api/blobs/profile")
async def profile_blobs(request: ProfileRequest) -> List[Dict]:
    """Return row count and per-column cardinality for each blob."""
    try:
        client = build_container_client(request.container_url)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    async def profile_one(blob_name: str) -> dict:
        try:
            return await asyncio.to_thread(
                _profile_one_blob_sync, client.get_blob_client(blob_name), blob_name
            )
        except Exception as e:
            return {
                "blob_name": blob_name,
                "detected_stage": detect_stage(blob_name) or "unknown",
                "row_count": -1,
                "columns": [],
                "error": str(e),
            }

    return list(await asyncio.gather(*[profile_one(b) for b in request.blob_names]))


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
                data = await asyncio.to_thread(
                    _profile_one_blob_sync, client.get_blob_client(blob_name), blob_name
                )
                yield _sse({"type": "profile", "data": data})
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


def _apply_single_filter(df: pl.DataFrame, f: FilterCondition) -> pl.DataFrame:
    """Apply one filter condition to a Polars DataFrame and return the filtered result."""
    if f.column not in df.columns:
        return df  # caller handles the warning

    if f.filter_type == "regex":
        try:
            re.compile(f.value)
            return df.filter(pl.col(f.column).cast(pl.Utf8).str.contains(f.value))
        except re.error:
            raise ValueError(f"Invalid regex: {f.value!r}")
    else:
        # equals — try numeric match for numeric columns, fall back to string
        dtype = df[f.column].dtype
        if dtype in (pl.Float32, pl.Float64):
            try:
                return df.filter(pl.col(f.column) == float(f.value))
            except (ValueError, TypeError):
                pass
        elif dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64,
                       pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64):
            try:
                return df.filter(pl.col(f.column) == int(f.value))
            except (ValueError, TypeError):
                pass
        return df.filter(pl.col(f.column).cast(pl.Utf8) == f.value)


def _blob_cache_path(container_url: str, blob_name: str) -> Path:
    """Return the persistent cache path for a blob."""
    slug = hashlib.md5(container_url.encode()).hexdigest()[:12]
    safe = blob_name.replace("/", "__").replace("\\", "__")
    return BLOB_CACHE_DIR / slug / safe


def _download_blob_to_disk(blob_client, local_path: str) -> int:
    """Download a blob and save it to disk. Returns file size in bytes. Runs sync — use to_thread."""
    raw = blob_client.download_blob().readall()
    with open(local_path, "wb") as fh:
        fh.write(raw)
    return len(raw)


def _process_stage_sync(
    local_paths: List[str],
    active_filters: List,
    filter_logic: str,
    deduplicate: bool,
    key_columns: List[str],
) -> tuple:
    """Read CSVs, merge, filter, dedup. Returns (merged_df, dedup_info, log_lines). Runs sync — use to_thread."""
    logs: List[tuple] = []
    dfs = [pl.read_csv(p, infer_schema_length=None) for p in local_paths]
    merged = pl.concat(dfs, how="diagonal")
    logs.append(("success", f"{len(merged)} rows after merge."))

    if active_filters:
        if filter_logic == "OR":
            parts: List[pl.DataFrame] = []
            for f in active_filters:
                if f.column not in merged.columns:
                    logs.append(("warning", f"Filter column '{f.column}' not found — skipped."))
                    continue
                try:
                    part = _apply_single_filter(merged, f)
                    parts.append(part)
                    op = "~" if f.filter_type == "regex" else "="
                    logs.append(("info", f"Filter '{f.column} {op} {f.value}' matched {len(part)} row(s)."))
                except ValueError as exc:
                    logs.append(("warning", f"{exc} — skipped."))
            if parts:
                merged = pl.concat(parts).unique(maintain_order=True)
            logs.append(("success", f"OR filter → {len(merged)} rows remain."))
        else:  # AND
            for f in active_filters:
                if f.column not in merged.columns:
                    logs.append(("warning", f"Filter column '{f.column}' not found — skipped."))
                    continue
                before = len(merged)
                try:
                    merged = _apply_single_filter(merged, f)
                except ValueError as exc:
                    logs.append(("warning", f"{exc} — skipped."))
                    continue
                op = "~" if f.filter_type == "regex" else "="
                logs.append(("info", f"Filter '{f.column} {op} {f.value}' → {len(merged)} rows remain (was {before})."))

    dedup_info: Optional[Dict] = None
    if deduplicate:
        valid_keys = [k for k in key_columns if k in merged.columns]
        if valid_keys:
            before = len(merged)
            merged = merged.unique(subset=valid_keys, keep="first", maintain_order=True)
            after = len(merged)
            if before > after:
                removed = before - after
                dedup_info = {"removed": removed, "kept": after}
                logs.append(("warning", f"Deduplicated: {removed} duplicate(s) removed, {after} row(s) kept."))
            else:
                logs.append(("info", "No duplicates found."))

    return merged, dedup_info, logs


def _build_flow_report(
    stage_dfs: Dict[str, pl.DataFrame],
    key_columns: List[str],
    ordered_stages: List[str],
    dedup_warnings: Dict[str, Dict[str, int]],
) -> tuple:
    """Collect unique key tuples and build per-record flow. Returns (rows, columns_per_stage). Runs sync — use to_thread."""
    # Pre-build a lookup dict per stage: key_tuple -> first matching row dict
    # This avoids O(keys * stages * rows) mask scans in the original nested loop.
    stage_lookup: Dict[str, Dict[tuple, dict]] = {}
    all_key_tuples: set = set()

    for stage, df in stage_dfs.items():
        valid_keys = [k for k in key_columns if k in df.columns]
        if not valid_keys:
            stage_lookup[stage] = {}
            continue
        records = df.to_dicts()
        str_key_rows = df.select([pl.col(k).cast(pl.Utf8).fill_null("") for k in valid_keys]).to_dicts()
        lookup: Dict[tuple, dict] = {}
        for i, key_row in enumerate(str_key_rows):
            key_tuple = tuple((k, key_row[k]) for k in valid_keys)
            if key_tuple not in lookup:
                lookup[key_tuple] = records[i]
            all_key_tuples.add(key_tuple)
        stage_lookup[stage] = lookup

    rows = []
    for key_tuple in sorted(all_key_tuples):
        key_dict = dict(key_tuple)
        flow: Dict[str, Any] = {}
        missing_stages: List[str] = []
        last_seen_stage: Optional[str] = None

        for stage in ordered_stages:
            row_data = stage_lookup.get(stage, {}).get(key_tuple)
            if row_data is None:
                missing_stages.append(stage)
            else:
                flow[stage] = row_data
                last_seen_stage = stage

        rows.append({
            "key_value": key_dict,
            "flow": flow,
            "missing_stages": missing_stages,
            "last_seen_stage": last_seen_stage,
            "dedup_warnings": {s: dedup_warnings[s] for s in dedup_warnings if s in flow},
        })

    columns_per_stage = {stage: list(df.columns) for stage, df in stage_dfs.items()}
    return rows, columns_per_stage


@app.post("/api/analyze")
async def analyze(request: AnalyzeRequest) -> StreamingResponse:
    """
    Stream SSE events while downloading selected blobs to a tmp folder,
    merging and processing per stage, then building the row-flow report.

    Event shapes:
      {"type": "log",          "level": "info|success|warning|error", "message": "..."}
      {"type": "stage_result", "stage": "...", "row_count": N}
      {"type": "result",       "data": {...}, "downloaded_files": [...], "tmp_dir": "..."}
      {"type": "error",        "message": "..."}
    """
    if not request.key_columns:
        raise HTTPException(status_code=400, detail="At least one key column is required.")
    if not request.selected_blobs:
        raise HTTPException(status_code=400, detail="No blobs selected.")

    async def generate() -> AsyncGenerator[str, None]:
        BLOB_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        HISTORY_DIR.mkdir(parents=True, exist_ok=True)

        try:
            yield _log("Connecting to Azure container…")
            client = build_container_client(request.container_url)
            yield _log("Connected.", "success")

            # ── 1. Identify which blobs need downloading ───────────────────────
            to_download: List[Tuple[str, Path]] = []
            for blob_name in request.selected_blobs:
                cache_path = _blob_cache_path(request.container_url, blob_name)
                if not cache_path.exists() or request.force_refresh:
                    to_download.append((blob_name, cache_path))

            cached_count = len(request.selected_blobs) - len(to_download)
            if cached_count:
                yield _log(f"{cached_count} file(s) from cache.", "info")

            # ── 2. Download all missing blobs in parallel ──────────────────────
            if to_download:
                yield _log(f"Downloading {len(to_download)} file(s) in parallel…")

                async def _dl_one(blob_name: str, cache_path: Path) -> int:
                    bc = client.get_blob_client(blob_name)
                    cache_path.parent.mkdir(parents=True, exist_ok=True)
                    return await asyncio.to_thread(_download_blob_to_disk, bc, str(cache_path))

                dl_results = await asyncio.gather(
                    *[_dl_one(bn, cp) for bn, cp in to_download],
                    return_exceptions=True,
                )
                failed: List[str] = []
                total_kb = 0.0
                for (bn, _), res in zip(to_download, dl_results):
                    if isinstance(res, Exception):
                        failed.append(bn)
                        yield _log(f"Download failed for {bn}: {res}", "error")
                    else:
                        total_kb += res / 1024  # type: ignore[operator]
                if failed:
                    yield _log(f"{len(failed)} file(s) failed to download — they will be skipped.", "warning")
                else:
                    yield _log(f"Downloaded {len(to_download)} file(s) ({total_kb:.0f} KB total).", "success")

            downloaded_files = [str(_blob_cache_path(request.container_url, bn)) for bn in request.selected_blobs]

            # ── 3. Group blobs by stage ────────────────────────────────────────
            stage_blobs: Dict[str, List[str]] = {}
            for blob_name in request.selected_blobs:
                stage = detect_stage(blob_name) or "unknown"
                stage_blobs.setdefault(stage, []).append(blob_name)

            ordered_stages = (
                [s for s in STAGE_ORDER if s in stage_blobs]
                + [s for s in stage_blobs if s not in STAGE_ORDER]
            )
            yield _log(
                f"Processing {len(ordered_stages)} stage(s): {', '.join(ordered_stages)}."
            )

            # ── 4. Process each stage and stream per-stage results ─────────────
            stage_dfs: Dict[str, pl.DataFrame] = {}
            dedup_warnings: Dict[str, Dict[str, int]] = {}
            active_filters = [f for f in request.filters if f.column and f.value]

            for stage in ordered_stages:
                blobs = stage_blobs[stage]
                local_paths = [
                    str(_blob_cache_path(request.container_url, bn))
                    for bn in blobs
                    if _blob_cache_path(request.container_url, bn).exists()
                ]
                if not local_paths:
                    yield _log(f"[{stage}] No usable files — skipped.", "warning")
                    continue

                yield _log(f"[{stage}] Merging {len(local_paths)} file(s)…")
                merged, dedup_info, stage_logs = await asyncio.to_thread(
                    _process_stage_sync,
                    local_paths,
                    active_filters,
                    request.filter_logic,
                    request.deduplicate,
                    request.key_columns,
                )
                for level, msg in stage_logs:
                    yield _log(f"[{stage}] {msg}", level)
                if dedup_info:
                    dedup_warnings[stage] = dedup_info
                stage_dfs[stage] = merged
                # Emit per-stage result so the frontend can display progressive output
                yield _sse({"type": "stage_result", "stage": stage, "row_count": len(merged)})

            # ── 5. Build cross-stage flow report ──────────────────────────────
            yield _log("Building flow report…")
            rows, columns_per_stage = await asyncio.to_thread(
                _build_flow_report, stage_dfs, request.key_columns, ordered_stages, dedup_warnings
            )
            yield _log(
                f"Done — {len(rows)} record(s) traced across {len(ordered_stages)} stage(s).",
                "success",
            )

            # ── 6. Save to history and emit final result ───────────────────────
            result_data = {
                "stages": ordered_stages,
                "key_columns": request.key_columns,
                "rows": rows,
                "columns": columns_per_stage,
                "dedup_warnings": dedup_warnings,
            }
            entry_id = str(uuid.uuid4())
            history_path = HISTORY_DIR / f"{entry_id}.json"
            tmp = history_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(
                {
                    "id": entry_id,
                    "saved_at": datetime.now(timezone.utc).isoformat(),
                    "request": request.model_dump(),
                    "result": result_data,
                },
                default=str,
            ))
            tmp.replace(history_path)

            yield _sse({
                "type": "result",
                "data": result_data,
                "downloaded_files": downloaded_files,
                "tmp_dir": str(BLOB_CACHE_DIR),
                "history_id": entry_id,
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


# ── Analysis history ──────────────────────────────────────────────────────────

@app.get("/api/history")
async def list_history() -> List[HistorySummary]:
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    summaries: List[HistorySummary] = []
    for f in sorted(HISTORY_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
        try:
            data = json.loads(f.read_text())
            summaries.append(HistorySummary(
                id=data["id"],
                saved_at=data["saved_at"],
                container_url=data["request"]["container_url"],
                blob_count=len(data["request"]["selected_blobs"]),
                key_columns=data["request"]["key_columns"],
            ))
        except Exception:
            pass
    return summaries


@app.get("/api/history/{entry_id}")
async def get_history_entry(entry_id: str):
    path = HISTORY_DIR / f"{entry_id}.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail="History entry not found")
    return json.loads(path.read_text())


@app.delete("/api/history/{entry_id}", status_code=204)
async def delete_history_entry(entry_id: str):
    path = HISTORY_DIR / f"{entry_id}.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail="History entry not found")
    path.unlink()


# ── Notion integration ────────────────────────────────────────────────────────

def _notion_client() -> NotionAsyncClient:
    config = _load_notion_config()
    if not config.get("token") or not config.get("database_id"):
        raise HTTPException(status_code=400, detail="Notion not configured — set token and database_id first")
    return NotionAsyncClient(auth=config["token"])


@app.get("/api/notion/config")
async def get_notion_config():
    return _load_notion_config()


@app.put("/api/notion/config")
async def update_notion_config(body: NotionConfigUpdate):
    config = {"token": body.token, "database_id": body.database_id}
    _save_notion_config(config)
    return config


@app.get("/api/notion/schema")
async def get_notion_schema():
    notion = _notion_client()
    db_id = _load_notion_config()["database_id"]
    db = await notion.databases.retrieve(database_id=db_id)
    props: Dict[str, Any] = {}
    order: List[str] = []
    for name, prop in db["properties"].items():
        ptype = prop["type"]
        info: Dict[str, Any] = {"type": ptype}
        if ptype == "select":
            info["options"] = [o["name"] for o in prop["select"]["options"]]
        elif ptype == "multi_select":
            info["options"] = [o["name"] for o in prop["multi_select"]["options"]]
        props[name] = info
        order.append(name)
    return {"properties": props, "property_order": order}


@app.get("/api/notion/rows")
async def get_notion_rows():
    notion = _notion_client()
    db_id = _load_notion_config()["database_id"]
    all_results: List[Dict] = []
    cursor: Optional[str] = None
    while True:
        kwargs: Dict[str, Any] = {"database_id": db_id}
        if cursor:
            kwargs["start_cursor"] = cursor
        resp = await notion.databases.query(**kwargs)
        all_results.extend(resp["results"])
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
    return [_page_to_dict(p) for p in all_results]


@app.post("/api/notion/rows", status_code=201)
async def create_notion_row(body: NotionPageCreate):
    notion = _notion_client()
    db_id = _load_notion_config()["database_id"]
    db = await notion.databases.retrieve(database_id=db_id)
    schema = {name: prop["type"] for name, prop in db["properties"].items()}
    notion_props = {
        name: _build_prop_payload(schema[name], value)
        for name, value in body.properties.items()
        if schema.get(name) in _NOTION_EDITABLE
    }
    page = await notion.pages.create(
        parent={"database_id": db_id},
        properties=notion_props,
    )
    return _page_to_dict(page)


@app.patch("/api/notion/rows/{page_id}")
async def update_notion_row(page_id: str, body: NotionPageUpdate):
    notion = _notion_client()
    db_id = _load_notion_config()["database_id"]
    db = await notion.databases.retrieve(database_id=db_id)
    schema = {name: prop["type"] for name, prop in db["properties"].items()}
    notion_props = {
        name: _build_prop_payload(schema[name], value)
        for name, value in body.properties.items()
        if schema.get(name) in _NOTION_EDITABLE
    }
    page = await notion.pages.update(page_id=page_id, properties=notion_props)
    return _page_to_dict(page)


@app.delete("/api/notion/rows/{page_id}", status_code=204)
async def delete_notion_row(page_id: str):
    notion = _notion_client()
    await notion.pages.update(page_id=page_id, archived=True)
