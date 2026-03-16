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
}

export interface LogEntry {
  level: "info" | "success" | "warning" | "error";
  message: string;
  timestamp: string; // ISO string set client-side
}

export async function listBlobs(containerUrl: string): Promise<BlobInfo[]> {
  const res = await fetch(`${BASE_URL}/api/blobs/list`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ container_url: containerUrl }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || "Failed to list blobs");
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
