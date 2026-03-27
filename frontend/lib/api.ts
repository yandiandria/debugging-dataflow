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
  onError: (message: string) => void
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

export async function listAirflowDags(): Promise<string[]> {
  const res = await fetch(`${BASE_URL}/api/dags/list-airflow`, { method: "POST" });
  if (!res.ok) throw new Error("Failed to list Airflow DAGs");
  const data = await res.json();
  if (Array.isArray(data)) {
    return (data as { dag_id?: string }[])
      .map((d) => d.dag_id ?? "")
      .filter(Boolean);
  }
  return [];
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

export async function fetchDagLogsStream(
  dag_id: string,
  run_id: string,
  onLog: (entry: LogEntry) => void,
  onTaskLog: (taskId: string, entry: LogEntry) => void,
  onTaskStates: (tasks: Array<{ task_id: string; state: string }>) => void,
  onMappingIssues: (issues: Array<{ unmapped_value: string; count?: number; column: string }>) => void,
  onDone: () => void | Promise<void>,
  onError: (message: string) => void,
): Promise<void> {
  const res = await fetch(`${BASE_URL}/api/dags/logs`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ dag_id, run_id, task_ids: [] }),
  });

  if (!res.ok || !res.body) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    onError(err.detail || "Failed to fetch DAG logs");
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
        const ts = new Date().toISOString();
        if (event.type === "log") {
          onLog({ level: event.level ?? "info", message: event.message, timestamp: ts });
        } else if (event.type === "task_log") {
          onTaskLog(event.task_id, { level: event.level ?? "info", message: event.message, timestamp: ts });
        } else if (event.type === "task_states") {
          onTaskStates(event.tasks);
        } else if (event.type === "mapping_issues") {
          onMappingIssues(event.issues);
        } else if (event.type === "done") {
          await onDone();
        } else if (event.type === "error") {
          onError(event.message);
        }
      } catch { /* ignore */ }
    }
  }
}

export async function getDagRuns(dagId?: string): Promise<DAGRun[]> {
  const url = dagId ? `${BASE_URL}/api/dag-runs?dag_id=${encodeURIComponent(dagId)}` : `${BASE_URL}/api/dag-runs`;
  const res = await fetch(url);
  if (!res.ok) throw new Error("Failed to load DAG runs");
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
