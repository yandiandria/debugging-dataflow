const BASE_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

export interface BlobInfo {
  name: string;
  size: number;
  last_modified: string;
  detected_stage: string | null;
}

export interface FilterCondition {
  column: string;
  value: string;
  filter_type?: "equals" | "regex";
}

export interface FlowRow {
  key_value: Record<string, string>;
  flow: Record<string, Record<string, unknown>>;
  missing_stages: string[];
  last_seen_stage: string | null;
  dedup_warnings: Record<string, { removed: number; kept: number }>;
}

export interface AnalyzeResult {
  stages: string[];
  key_columns: string[];
  rows: FlowRow[];
  columns: Record<string, string[]>;
  dedup_warnings: Record<string, { removed: number; kept: number }>;
}

export interface AnalyzeResultFull extends AnalyzeResult {
  downloaded_files: string[];
  tmp_dir: string;
  history_id?: string;
}

export interface HistorySummary {
  id: string;
  saved_at: string;
  container_url: string;
  blob_count: number;
  key_columns: string[];
}

export interface HistoryEntry {
  id: string;
  saved_at: string;
  request: {
    container_url: string;
    selected_blobs: string[];
    key_columns: string[];
    filters: FilterCondition[];
    deduplicate: boolean;
    filter_logic: "AND" | "OR";
    force_refresh: boolean;
  };
  result: AnalyzeResult;
}

export interface LogEntry {
  level: "info" | "success" | "warning" | "error";
  message: string;
  timestamp: string; // ISO string set client-side
}

export async function listBlobs(
  containerUrl: string,
  dateFrom?: string,
  dateTo?: string,
  prefix?: string,
  extractPrefixes?: string[],
): Promise<BlobInfo[]> {
  const res = await fetch(`${BASE_URL}/api/blobs/list`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      container_url: containerUrl,
      ...(prefix && { prefix }),
      ...(extractPrefixes && extractPrefixes.length > 0 && { extract_prefixes: extractPrefixes }),
      ...(dateFrom && { date_from: dateFrom }),
      ...(dateTo && { date_to: dateTo }),
    }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || "Failed to list blobs");
  }
  return res.json();
}

export interface Resource {
  id: string;
  technical_name: string;
  business_name: string;
  created_at: string;
  dag_ids?: string[];
  extract_prefixes?: string[];
}

// ── DAG types ──────────────────────────────────────────────────────────────

export interface DAG {
  id: string;
  dag_id: string;
  display_name: string;
  created_at: string;
}

export interface DAGConfig {
  container_name: string;
  environments: string[];
}

export interface DAGRun {
  id: string;
  dag_id: string;
  run_id: string;
  padoa_env: string;
  triggered_at: string;
  status: string;
  stdout?: string;
  stderr?: string;
  task_logs?: LogEntry[];
  task_sub_logs?: Array<{ task_id: string; logs: LogEntry[] }>;
  task_final_states?: Array<{ task_id: string; state: string }>;
}

// ── Integration Rule types ────────────────────────────────────────────────

export interface IntegrationRule {
  id: string;
  description: string;
  resource_ids: string[];
  checked: boolean;
  created_at: string;
}

// ── Mapping Issue types ───────────────────────────────────────────────────

export interface MappingIssue {
  id: string;
  resource_id: string;
  dag_run_id: string;
  unmapped_value: string;
  count?: number;
  column: string;
  first_seen: string;
  fixed_in_rerun: string | null;
  resolved: boolean;
}

// ── QA Example ID types ──────────────────────────────────────────────────

export interface QAExampleID {
  id: string;
  resource_id: string;
  id_column: string;
  id_value: string;
  label: string;
  created_at: string;
}

export async function getResources(): Promise<Resource[]> {
  const res = await fetch(`${BASE_URL}/api/resources`);
  if (!res.ok) throw new Error("Failed to load resources");
  return res.json();
}

export async function createResource(technical_name: string, business_name: string, extract_prefixes?: string[]): Promise<Resource> {
  const res = await fetch(`${BASE_URL}/api/resources`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ technical_name, business_name, ...(extract_prefixes && { extract_prefixes }) }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || "Failed to create resource");
  }
  return res.json();
}

export async function updateResource(id: string, technical_name: string, business_name: string, extract_prefixes?: string[]): Promise<Resource> {
  const res = await fetch(`${BASE_URL}/api/resources/${id}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ technical_name, business_name, ...(extract_prefixes && { extract_prefixes }) }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || "Failed to update resource");
  }
  return res.json();
}

export async function deleteResource(id: string): Promise<void> {
  const res = await fetch(`${BASE_URL}/api/resources/${id}`, { method: "DELETE" });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || "Failed to delete resource");
  }
}

export async function getResourceBlobs(
  resourceId: string,
  containerUrl: string,
  dateFrom?: string,
  dateTo?: string,
): Promise<BlobInfo[]> {
  const params = new URLSearchParams({ container_url: containerUrl });
  if (dateFrom) params.set("date_from", dateFrom);
  if (dateTo) params.set("date_to", dateTo);
  const res = await fetch(`${BASE_URL}/api/resources/${resourceId}/blobs?${params}`);
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || "Failed to load resource blobs");
  }
  return res.json();
}

export interface BlobPreview {
  columns: string[];
  rows: Record<string, unknown>[];
  total_rows_loaded: number;
}

export async function getBlobPreview(
  containerUrl: string,
  blobName: string,
  limit = 200
): Promise<BlobPreview> {
  const res = await fetch(`${BASE_URL}/api/blobs/preview`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ container_url: containerUrl, blob_name: blobName, limit }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || "Failed to load preview");
  }
  return res.json();
}

export async function getBlobColumns(
  containerUrl: string,
  blobName: string
): Promise<string[]> {
  const res = await fetch(`${BASE_URL}/api/blobs/columns`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ container_url: containerUrl, blob_name: blobName }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || "Failed to get columns");
  }
  return res.json();
}

export interface ColumnProfile {
  name: string;
  distinct_count: number;
  value_counts: Record<string, number> | null;
}

export interface BlobProfile {
  blob_name: string;
  detected_stage: string;
  row_count: number;
  columns: ColumnProfile[];
  error?: string;
}

export async function profileBlobs(
  containerUrl: string,
  blobNames: string[]
): Promise<BlobProfile[]> {
  const res = await fetch(`${BASE_URL}/api/blobs/profile`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ container_url: containerUrl, blob_names: blobNames }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || "Failed to profile blobs");
  }
  return res.json();
}

export interface ProfileProgress {
  current: number;
  total: number;
  blob_name: string;
}

/**
 * Stream SSE events from /api/blobs/profile/stream.
 * Calls onProgress for each blob starting, onProfile when a blob is done, onError on failure.
 */
export async function profileBlobsStream(
  containerUrl: string,
  blobNames: string[],
  onProgress: (progress: ProfileProgress) => void,
  onProfile: (profile: BlobProfile) => void,
  onDone: () => void,
  onError: (message: string) => void
): Promise<void> {
  const res = await fetch(`${BASE_URL}/api/blobs/profile/stream`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ container_url: containerUrl, blob_names: blobNames }),
  });

  if (!res.ok || !res.body) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    onError(err.detail || "Failed to profile blobs");
    return;
  }

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split("\n");
    buffer = lines.pop() ?? "";

    for (const line of lines) {
      if (!line.startsWith("data: ")) continue;
      try {
        const event = JSON.parse(line.slice(6));
        if (event.type === "progress") {
          onProgress(event as ProfileProgress);
        } else if (event.type === "profile") {
          onProfile(event.data as BlobProfile);
        } else if (event.type === "done") {
          onDone();
        } else if (event.type === "error") {
          onError(event.message);
        }
      } catch {
        // malformed SSE line — ignore
      }
    }
  }
}

/**
 * Stream SSE events from /api/analyze.
 * Calls onLog for each progress message, onResult when complete, onError on failure.
 */
export async function analyzeStream(
  payload: {
    container_url: string;
    selected_blobs: string[];
    key_columns: string[];
    filters: FilterCondition[];
    deduplicate: boolean;
    filter_logic: "AND" | "OR";
    force_refresh?: boolean;
  },
  onLog: (entry: LogEntry) => void,
  onResult: (result: AnalyzeResultFull) => void,
  onError: (message: string) => void,
  onStageResult?: (stage: string, rowCount: number) => void,
): Promise<void> {
  const res = await fetch(`${BASE_URL}/api/analyze`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  if (!res.ok || !res.body) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    onError(err.detail || "Analysis failed");
    return;
  }

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split("\n");
    buffer = lines.pop() ?? "";

    for (const line of lines) {
      if (!line.startsWith("data: ")) continue;
      try {
        const event = JSON.parse(line.slice(6));
        if (event.type === "log") {
          onLog({
            level: event.level ?? "info",
            message: event.message,
            timestamp: new Date().toISOString(),
          });
        } else if (event.type === "stage_result") {
          onStageResult?.(event.stage, event.row_count);
        } else if (event.type === "result") {
          onResult({
            ...event.data,
            downloaded_files: event.downloaded_files,
            tmp_dir: event.tmp_dir,
          });
        } else if (event.type === "error") {
          onError(event.message);
        }
      } catch {
        // malformed SSE line — ignore
      }
    }
  }
}

// ── DAG API ──────────────────────────────────────────────────────────────────

export async function getDags(): Promise<DAG[]> {
  const res = await fetch(`${BASE_URL}/api/dags`);
  if (!res.ok) throw new Error("Failed to load DAGs");
  return res.json();
}

export async function createDag(dag_id: string, display_name: string): Promise<DAG> {
  const res = await fetch(`${BASE_URL}/api/dags`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ dag_id, display_name }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || "Failed to create DAG");
  }
  return res.json();
}

export async function updateDag(id: string, dag_id: string, display_name: string): Promise<DAG> {
  const res = await fetch(`${BASE_URL}/api/dags/${id}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ dag_id, display_name }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || "Failed to update DAG");
  }
  return res.json();
}

export async function deleteDag(id: string): Promise<void> {
  const res = await fetch(`${BASE_URL}/api/dags/${id}`, { method: "DELETE" });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || "Failed to delete DAG");
  }
}

export async function getDagConfig(): Promise<DAGConfig> {
  const res = await fetch(`${BASE_URL}/api/dag-config`);
  if (!res.ok) throw new Error("Failed to load DAG config");
  return res.json();
}

export async function updateDagConfig(config: DAGConfig): Promise<DAGConfig> {
  const res = await fetch(`${BASE_URL}/api/dag-config`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(config),
  });
  if (!res.ok) throw new Error("Failed to update DAG config");
  return res.json();
}

export interface AirflowDagsResult {
  dags: { dag_id?: string }[];
  fetched_at: string; // ISO timestamp
  cached: boolean;
}

export async function listAirflowDags(forceRefresh = false): Promise<AirflowDagsResult> {
  const url = `${BASE_URL}/api/dags/list-airflow${forceRefresh ? "?force_refresh=true" : ""}`;
  const res = await fetch(url, { method: "POST" });
  if (!res.ok) throw new Error("Failed to list Airflow DAGs");
  const data = await res.json();
  // Handle both old (plain array) and new (wrapped object) response shapes
  if (Array.isArray(data)) {
    return { dags: data, fetched_at: new Date().toISOString(), cached: false };
  }
  return {
    dags: Array.isArray(data.dags) ? data.dags : [],
    fetched_at: data.fetched_at ?? new Date().toISOString(),
    cached: data.cached ?? false,
  };
}

// ── Docker helpers ──────────────────────────────────────────────────────────

export interface DockerContainer {
  id: string;
  name: string;
  image: string;
  status: string;
  is_airflow: boolean;
}

export async function listDockerContainers(): Promise<DockerContainer[]> {
  const res = await fetch(`${BASE_URL}/api/docker/containers`);
  if (!res.ok) throw new Error("Failed to list Docker containers");
  return res.json();
}

// ── Config export / import ──────────────────────────────────────────────────

export async function exportConfig(): Promise<void> {
  const res = await fetch(`${BASE_URL}/api/config/export`);
  if (!res.ok) throw new Error("Export failed");
  const blob = await res.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `dataflow-config-${new Date().toISOString().slice(0, 10)}.json`;
  a.click();
  URL.revokeObjectURL(url);
}

export async function importConfig(file: File): Promise<{ restored: string[] }> {
  const text = await file.text();
  const bundle = JSON.parse(text);
  const res = await fetch(`${BASE_URL}/api/config/import`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(bundle),
  });
  if (!res.ok) throw new Error("Import failed");
  return res.json();
}

export async function triggerDagStream(
  dag_id: string,
  padoa_env: string,
  onLog: (entry: LogEntry) => void,
  onResult: (data: { run_id: string; record_id: string }) => void,
  onError: (message: string) => void,
): Promise<void> {
  const res = await fetch(`${BASE_URL}/api/dags/trigger`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ dag_id, padoa_env }),
  });

  if (!res.ok || !res.body) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    onError(err.detail || "Failed to trigger DAG");
    return;
  }

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split("\n");
    buffer = lines.pop() ?? "";
    for (const line of lines) {
      if (!line.startsWith("data: ")) continue;
      try {
        const event = JSON.parse(line.slice(6));
        if (event.type === "log") {
          onLog({ level: event.level ?? "info", message: event.message, timestamp: new Date().toISOString() });
        } else if (event.type === "result") {
          onResult({ run_id: event.run_id, record_id: event.record_id });
        } else if (event.type === "error") {
          onError(event.message);
        }
      } catch { /* ignore */ }
    }
  }
}

export async function getLatestAirflowRunId(dagId: string): Promise<string> {
  const res = await fetch(`${BASE_URL}/api/dags/${encodeURIComponent(dagId)}/latest-run-id`);
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || "Failed to get latest run ID");
  }
  const data = await res.json();
  return data.run_id as string;
}

export interface AirflowConfig {
  base_url: string;
  username: string;
  password: string;
  verify_tls?: boolean;
}

export interface TaskInstanceState {
  task_id: string;
  state: string;
  start_date: string | null;
  end_date: string | null;
}

/** Lightweight poll — returns task instance states without fetching logs. */
export async function getTaskStates(
  config: AirflowConfig,
  dag_id: string,
  run_id: string,
): Promise<TaskInstanceState[]> {
  const res = await fetch(`${BASE_URL}/api/dags/task-states`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ...config, dag_id, run_id }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || "Failed to fetch task states");
  }
  return res.json();
}

/** Fetch logs for a single running task (1 HTTP call instead of N). */
export async function getRunningTaskLog(
  config: AirflowConfig,
  dag_id: string,
  run_id: string,
  task_id: string,
  try_number = 1,
): Promise<{ task_id: string; logs: string }> {
  const res = await fetch(`${BASE_URL}/api/dags/running-task-log`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ...config, dag_id, run_id, task_id, try_number }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || "Failed to fetch running task log");
  }
  return res.json();
}

export interface TaskLog {
  task_id: string;
  map_index: number;
  try_number: number;
  state: string;
  logs: string;
  status: "success" | "not_found" | string;
}

/**
 * Fetch Airflow task logs using the REST API.
 * Calls onLog for progress messages, onTaskLog for each task's logs, onDone when complete.
 */
export async function fetchDagLogsRestApi(
  config: AirflowConfig,
  dag_id: string,
  onLog: (entry: LogEntry) => void,
  onTaskLog: (taskId: string, logs: string) => void,
  onDone: (taskLogs: TaskLog[]) => void | Promise<void>,
  onError: (message: string) => void,
): Promise<void> {
  const res = await fetch(`${BASE_URL}/api/dags/logs-rest-api`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ...config, dag_id }),
  });

  if (!res.ok || !res.body) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    onError(err.detail || "Failed to fetch DAG logs");
    return;
  }

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let allTaskLogs: TaskLog[] = [];

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split("\n");
    buffer = lines.pop() ?? "";
    for (const line of lines) {
      if (!line.startsWith("data: ")) continue;
      try {
        const event = JSON.parse(line.slice(6));
        const ts = new Date().toISOString();
        if (event.type === "log") {
          onLog({ level: event.level ?? "info", message: event.message, timestamp: ts });
        } else if (event.type === "done") {
          allTaskLogs = event.logs ?? [];
          // Emit individual task logs
          for (const taskLog of allTaskLogs) {
            if (taskLog.logs) {
              // Log raw content for debugging
              console.log(`Task ${taskLog.task_id}: ${taskLog.logs.length} bytes`, taskLog.logs.slice(0, 200));
              onTaskLog(taskLog.task_id, taskLog.logs);
            }
          }
          await onDone(allTaskLogs);
        } else if (event.type === "error") {
          onError(event.message);
        }
      } catch { /* ignore */ }
    }
  }
}

/** Fetch the run list. By default logs are stripped (fast). Pass includeLogs for full data. */
export async function getDagRuns(dagId?: string, includeLogs = false): Promise<DAGRun[]> {
  const params = new URLSearchParams();
  if (dagId) params.set("dag_id", dagId);
  if (includeLogs) params.set("include_logs", "true");
  const qs = params.toString();
  const res = await fetch(`${BASE_URL}/api/dag-runs${qs ? `?${qs}` : ""}`);
  if (!res.ok) throw new Error("Failed to load DAG runs");
  return res.json();
}

/** Fetch a single run with full log data (task_logs, task_sub_logs, etc.). */
export async function getDagRun(id: string): Promise<DAGRun> {
  const res = await fetch(`${BASE_URL}/api/dag-runs/${encodeURIComponent(id)}`);
  if (!res.ok) throw new Error("Failed to load DAG run");
  return res.json();
}

export async function updateDagRun(id: string, data: Partial<DAGRun>): Promise<void> {
  await fetch(`${BASE_URL}/api/dag-runs/${id}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(data),
  });
}

// ── Resource-DAG linking ────────────────────────────────────────────────────

export async function getResourceDags(resourceId: string): Promise<string[]> {
  const res = await fetch(`${BASE_URL}/api/resources/${resourceId}/dags`);
  if (!res.ok) throw new Error("Failed to load resource DAGs");
  const data = await res.json();
  return data.dag_ids;
}

export async function linkResourceDags(resourceId: string, dagIds: string[]): Promise<void> {
  const res = await fetch(`${BASE_URL}/api/resources/${resourceId}/dags`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ dag_ids: dagIds }),
  });
  if (!res.ok) throw new Error("Failed to link DAGs to resource");
}

// ── Integration Rule API ────────────────────────────────────────────────────

export async function getRules(): Promise<IntegrationRule[]> {
  const res = await fetch(`${BASE_URL}/api/rules`);
  if (!res.ok) throw new Error("Failed to load rules");
  return res.json();
}

export async function createRule(description: string, resourceIds: string[] = []): Promise<IntegrationRule> {
  const res = await fetch(`${BASE_URL}/api/rules`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ description, resource_ids: resourceIds }),
  });
  if (!res.ok) throw new Error("Failed to create rule");
  return res.json();
}

export async function updateRule(id: string, updates: { description?: string; resource_ids?: string[]; checked?: boolean }): Promise<IntegrationRule> {
  const res = await fetch(`${BASE_URL}/api/rules/${id}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(updates),
  });
  if (!res.ok) throw new Error("Failed to update rule");
  return res.json();
}

export async function deleteRule(id: string): Promise<void> {
  const res = await fetch(`${BASE_URL}/api/rules/${id}`, { method: "DELETE" });
  if (!res.ok) throw new Error("Failed to delete rule");
}

// ── Mapping Issue API ──────────────────────────────────────────────────────

export async function getMappingIssues(resourceId?: string): Promise<MappingIssue[]> {
  const url = resourceId
    ? `${BASE_URL}/api/mapping-issues?resource_id=${encodeURIComponent(resourceId)}`
    : `${BASE_URL}/api/mapping-issues`;
  const res = await fetch(url);
  if (!res.ok) throw new Error("Failed to load mapping issues");
  return res.json();
}

export async function saveMappingIssues(
  issues: Array<{ unmapped_value: string; column: string }>,
  resourceId: string,
  dagRunId: string,
): Promise<MappingIssue[]> {
  const res = await fetch(`${BASE_URL}/api/mapping-issues?resource_id=${encodeURIComponent(resourceId)}&dag_run_id=${encodeURIComponent(dagRunId)}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(issues),
  });
  if (!res.ok) throw new Error("Failed to save mapping issues");
  return res.json();
}

export async function resolveMappingIssue(issueId: string, resolved: boolean): Promise<MappingIssue> {
  const res = await fetch(`${BASE_URL}/api/mapping-issues/${issueId}/resolve`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ resolved }),
  });
  if (!res.ok) throw new Error("Failed to resolve mapping issue");
  return res.json();
}

// ── QA Example ID API ──────────────────────────────────────────────────────

export async function getQAExamples(resourceId?: string): Promise<QAExampleID[]> {
  const url = resourceId
    ? `${BASE_URL}/api/qa-examples?resource_id=${encodeURIComponent(resourceId)}`
    : `${BASE_URL}/api/qa-examples`;
  const res = await fetch(url);
  if (!res.ok) throw new Error("Failed to load QA examples");
  return res.json();
}

export async function createQAExample(body: { resource_id: string; id_column: string; id_value: string; label: string }): Promise<QAExampleID> {
  const res = await fetch(`${BASE_URL}/api/qa-examples`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error("Failed to create QA example");
  return res.json();
}

export async function deleteQAExample(id: string): Promise<void> {
  const res = await fetch(`${BASE_URL}/api/qa-examples/${id}`, { method: "DELETE" });
  if (!res.ok) throw new Error("Failed to delete QA example");
}

// ── ID column detection ────────────────────────────────────────────────────

export async function getIdColumns(containerUrl: string, blobNames: string[]): Promise<string[]> {
  const res = await fetch(`${BASE_URL}/api/blobs/id-columns`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ container_url: containerUrl, blob_names: blobNames }),
  });
  if (!res.ok) throw new Error("Failed to detect ID columns");
  return res.json();
}

export async function getColumnValues(
  containerUrl: string,
  blobNames: string[],
  column: string,
  limit = 500,
): Promise<{ values: string[]; total: number }> {
  const res = await fetch(`${BASE_URL}/api/blobs/column-values`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ container_url: containerUrl, blob_names: blobNames, column, limit }),
  });
  if (!res.ok) throw new Error("Failed to get column values");
  return res.json();
}

// ── Notion integration ────────────────────────────────────────────────────────

export interface NotionPropertySchema {
  type: string;
  options?: string[];
}

export interface NotionSchema {
  properties: Record<string, NotionPropertySchema>;
  property_order: string[];
}

export interface NotionRow {
  id: string;
  url: string;
  created_time: string;
  last_edited_time: string;
  properties: Record<string, unknown>;
}

export interface NotionConfig {
  token: string;
  database_id: string;
}

export async function getNotionConfig(): Promise<NotionConfig> {
  const res = await fetch(`${BASE_URL}/api/notion/config`);
  if (!res.ok) throw new Error("Failed to load Notion config");
  return res.json();
}

export async function saveNotionConfig(config: NotionConfig): Promise<NotionConfig> {
  const res = await fetch(`${BASE_URL}/api/notion/config`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(config),
  });
  if (!res.ok) throw new Error("Failed to save Notion config");
  return res.json();
}

export async function getNotionSchema(): Promise<NotionSchema> {
  const res = await fetch(`${BASE_URL}/api/notion/schema`);
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || "Failed to load Notion schema");
  }
  return res.json();
}

export async function getNotionRows(): Promise<NotionRow[]> {
  const res = await fetch(`${BASE_URL}/api/notion/rows`);
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || "Failed to load Notion rows");
  }
  return res.json();
}

export async function createNotionRow(properties: Record<string, unknown>): Promise<NotionRow> {
  const res = await fetch(`${BASE_URL}/api/notion/rows`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ properties }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || "Failed to create row");
  }
  return res.json();
}

export async function updateNotionRow(pageId: string, properties: Record<string, unknown>): Promise<NotionRow> {
  const res = await fetch(`${BASE_URL}/api/notion/rows/${pageId}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ properties }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || "Failed to update row");
  }
  return res.json();
}

export async function deleteNotionRow(pageId: string): Promise<void> {
  const res = await fetch(`${BASE_URL}/api/notion/rows/${pageId}`, { method: "DELETE" });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || "Failed to delete row");
  }
}

// ── Analysis history ──────────────────────────────────────────────────────────

export async function listHistory(): Promise<HistorySummary[]> {
  const res = await fetch(`${BASE_URL}/api/history`);
  if (!res.ok) throw new Error("Failed to load history");
  return res.json();
}

export async function getHistoryEntry(id: string): Promise<HistoryEntry> {
  const res = await fetch(`${BASE_URL}/api/history/${encodeURIComponent(id)}`);
  if (!res.ok) throw new Error("History entry not found");
  return res.json();
}

export async function deleteHistoryEntry(id: string): Promise<void> {
  const res = await fetch(`${BASE_URL}/api/history/${encodeURIComponent(id)}`, {
    method: "DELETE",
  });
  if (!res.ok) throw new Error("Failed to delete history entry");
}
