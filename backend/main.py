import io
import json
import os
import re
import tempfile
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
    "load",
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


# ── Models ─────────────────────────────────────────────────────────────────────

class BlobListRequest(BaseModel):
    container_url: str
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


# ── Endpoints ──────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "healthy"}


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
        for blob in client.list_blobs():
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
            "rows": df.where(pd.notna(df), None).to_dict(orient="records"),
            "total_rows_loaded": len(df),
        }
    except AzureError as e:
        raise HTTPException(status_code=400, detail=f"Azure error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
                        record = matches.iloc[0].to_dict()
                        flow[stage] = {
                            k: (None if pd.isna(v) else v)
                            for k, v in record.items()
                        }
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
