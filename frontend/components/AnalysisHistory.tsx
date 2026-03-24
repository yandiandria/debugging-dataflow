import { useState, useEffect } from "react";
import { listHistory, getHistoryEntry, deleteHistoryEntry } from "../lib/api";
import type { HistorySummary, HistoryEntry, AnalyzeResultFull, FilterCondition } from "../lib/api";

interface Props {
  onBack: () => void;
  onLoadResult: (result: AnalyzeResultFull) => void;
  onUseAsTemplate: (config: {
    containerUrl: string;
    selectedBlobs: string[];
    keyColumns: string[];
    filters: FilterCondition[];
    deduplicate: boolean;
    filterLogic: "AND" | "OR";
  }) => void;
}

export default function AnalysisHistory({ onBack, onLoadResult, onUseAsTemplate }: Props) {
  const [entries, setEntries] = useState<HistorySummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [loadingId, setLoadingId] = useState<string | null>(null);

  useEffect(() => {
    listHistory()
      .then(setEntries)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  const handleDelete = async (id: string) => {
    try {
      await deleteHistoryEntry(id);
      setEntries((prev) => prev.filter((e) => e.id !== id));
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Delete failed");
    }
  };

  const handleLoadResult = async (id: string) => {
    setLoadingId(id);
    try {
      const entry: HistoryEntry = await getHistoryEntry(id);
      onLoadResult({
        ...entry.result,
        downloaded_files: [],
        tmp_dir: "",
        history_id: entry.id,
      });
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Failed to load entry");
    } finally {
      setLoadingId(null);
    }
  };

  const handleUseAsTemplate = async (id: string) => {
    setLoadingId(id);
    try {
      const entry: HistoryEntry = await getHistoryEntry(id);
      onUseAsTemplate({
        containerUrl: entry.request.container_url,
        selectedBlobs: entry.request.selected_blobs,
        keyColumns: entry.request.key_columns,
        filters: entry.request.filters,
        deduplicate: entry.request.deduplicate,
        filterLogic: entry.request.filter_logic,
      });
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Failed to load entry");
    } finally {
      setLoadingId(null);
    }
  };

  return (
    <div className="min-h-screen bg-gray-50 flex flex-col">
      {/* Top bar */}
      <div className="bg-white border-b border-gray-200 px-6 py-3 flex items-center gap-3">
        <button
          onClick={onBack}
          className="text-sm text-gray-400 hover:text-gray-700 transition-colors"
        >
          ←
        </button>
        <span className="text-gray-200">|</span>
        <h2 className="text-sm font-semibold text-gray-800">Analysis History</h2>
      </div>

      <div className="flex-1 p-6 max-w-4xl mx-auto w-full">
        {loading && <p className="text-sm text-gray-400">Loading…</p>}
        {error && (
          <div className="bg-red-50 border border-red-200 rounded-lg p-3 text-sm text-red-700 mb-4">
            {error}
          </div>
        )}

        {!loading && entries.length === 0 && (
          <div className="text-center py-16 text-gray-400 text-sm">
            No analyses saved yet. Run an analysis to see it here.
          </div>
        )}

        <div className="space-y-3">
          {entries.map((entry) => (
            <div
              key={entry.id}
              className="bg-white border border-gray-200 rounded-xl p-4 flex items-start justify-between gap-4"
            >
              <div className="min-w-0 flex-1">
                <div className="flex items-center gap-2 mb-1">
                  <span className="text-xs text-gray-400">
                    {new Date(entry.saved_at).toLocaleString()}
                  </span>
                  <span className="text-xs bg-gray-100 text-gray-500 px-2 py-0.5 rounded-full">
                    {entry.blob_count} file{entry.blob_count !== 1 ? "s" : ""}
                  </span>
                </div>
                <p className="text-sm text-gray-700 font-mono truncate" title={entry.container_url}>
                  {entry.container_url}
                </p>
                {entry.key_columns.length > 0 && (
                  <p className="text-xs text-gray-400 mt-1">
                    Keys: {entry.key_columns.join(", ")}
                  </p>
                )}
              </div>

              <div className="flex items-center gap-2 flex-shrink-0">
                <button
                  onClick={() => handleLoadResult(entry.id)}
                  disabled={loadingId === entry.id}
                  className="text-xs bg-blue-600 hover:bg-blue-700 disabled:bg-blue-300 text-white px-3 py-1.5 rounded-lg transition-colors"
                >
                  {loadingId === entry.id ? "Loading…" : "View results"}
                </button>
                <button
                  onClick={() => handleUseAsTemplate(entry.id)}
                  disabled={loadingId === entry.id}
                  className="text-xs bg-gray-100 hover:bg-gray-200 disabled:opacity-50 text-gray-700 px-3 py-1.5 rounded-lg transition-colors"
                >
                  Use as template
                </button>
                <button
                  onClick={() => handleDelete(entry.id)}
                  className="text-xs text-gray-300 hover:text-red-500 transition-colors px-1"
                  title="Delete"
                >
                  ×
                </button>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
