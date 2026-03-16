import { useState, useEffect } from "react";
import { getBlobColumns } from "../lib/api";
import type { FilterCondition } from "../lib/api";

interface Props {
  containerUrl: string;
  selectedBlobs: string[];
  onSubmit: (config: {
    keyColumns: string[];
    filters: FilterCondition[];
    deduplicate: boolean;
    filterLogic: "AND" | "OR";
  }) => void;
  onBack: () => void;
  loading: boolean;
  error: string | null;
}

export default function AnalysisConfig({
  containerUrl,
  selectedBlobs,
  onSubmit,
  onBack,
  loading,
  error,
}: Props) {
  const [columns, setColumns] = useState<string[]>([]);
  const [columnsLoading, setColumnsLoading] = useState(true);
  const [columnsError, setColumnsError] = useState<string | null>(null);

  const [keyColumns, setKeyColumns] = useState<string[]>([]);
  const [filters, setFilters] = useState<FilterCondition[]>([
    { column: "", value: "", filter_type: "equals" },
  ]);
  const [filterLogic, setFilterLogic] = useState<"AND" | "OR">("AND");
  const [deduplicate, setDeduplicate] = useState(true);

  // Load columns from the first selected blob
  useEffect(() => {
    if (selectedBlobs.length === 0) return;
    setColumnsLoading(true);
    setColumnsError(null);
    getBlobColumns(containerUrl, selectedBlobs[0])
      .then((cols) => {
        setColumns(cols);
        setColumnsLoading(false);
      })
      .catch((e) => {
        setColumnsError(e.message);
        setColumnsLoading(false);
      });
  }, [containerUrl, selectedBlobs]);

  const toggleKeyColumn = (col: string) => {
    setKeyColumns((prev) =>
      prev.includes(col) ? prev.filter((c) => c !== col) : [...prev, col]
    );
  };

  const updateFilter = (idx: number, field: keyof FilterCondition, val: string) => {
    setFilters((prev) => {
      const next = [...prev];
      next[idx] = { ...next[idx], [field]: val };
      return next;
    });
  };

  const addFilter = () =>
    setFilters((prev) => [...prev, { column: "", value: "", filter_type: "equals" }]);
  const removeFilter = (idx: number) =>
    setFilters((prev) => prev.filter((_, i) => i !== idx));

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const validFilters = filters.filter((f) => f.column && f.value);
    onSubmit({ keyColumns, filters: validFilters, deduplicate, filterLogic });
  };

  return (
    <div className="min-h-screen bg-gray-50 flex items-center justify-center p-4">
      <div className="bg-white rounded-2xl shadow-lg p-8 w-full max-w-xl">
        <button
          onClick={onBack}
          className="text-sm text-gray-500 hover:text-gray-700 mb-4 flex items-center gap-1"
        >
          ← Back to file browser
        </button>

        <h2 className="text-xl font-bold text-gray-800 mb-1">Configure Analysis</h2>
        <p className="text-sm text-gray-500 mb-6">
          {selectedBlobs.length} file{selectedBlobs.length !== 1 ? "s" : ""} selected.
          Choose a key column to trace rows across pipeline stages.
        </p>

        {columnsLoading && (
          <p className="text-sm text-gray-400 mb-4">Loading columns…</p>
        )}
        {columnsError && (
          <div className="bg-red-50 border border-red-200 rounded-lg p-3 text-sm text-red-700 mb-4">
            Could not read columns: {columnsError}
          </div>
        )}

        <form onSubmit={handleSubmit} className="space-y-6">
          {/* Key columns */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Key column(s) — used to identify and join rows across stages
            </label>
            {columns.length > 0 ? (
              <div className="flex flex-wrap gap-2">
                {columns.map((col) => (
                  <button
                    key={col}
                    type="button"
                    onClick={() => toggleKeyColumn(col)}
                    className={`px-3 py-1 rounded-full text-sm border transition-colors ${
                      keyColumns.includes(col)
                        ? "bg-blue-600 text-white border-blue-600"
                        : "bg-white text-gray-700 border-gray-300 hover:border-blue-400"
                    }`}
                  >
                    {col}
                  </button>
                ))}
              </div>
            ) : (
              !columnsLoading && (
                <input
                  type="text"
                  placeholder="Type column name (e.g. sourceId)"
                  className="border border-gray-300 rounded-lg px-3 py-2 text-sm w-full focus:outline-none focus:ring-2 focus:ring-blue-500"
                  onKeyDown={(e) => {
                    if (e.key === "Enter") {
                      e.preventDefault();
                      const val = (e.target as HTMLInputElement).value.trim();
                      if (val && !keyColumns.includes(val))
                        setKeyColumns((p) => [...p, val]);
                      (e.target as HTMLInputElement).value = "";
                    }
                  }}
                />
              )
            )}
            {keyColumns.length > 0 && (
              <p className="text-xs text-blue-600 mt-1">
                Selected: {keyColumns.join(", ")}
              </p>
            )}
          </div>

          {/* Filters */}
          <div>
            <div className="flex items-center justify-between mb-2">
              <label className="block text-sm font-medium text-gray-700">
                Filters <span className="text-gray-400 font-normal">(optional)</span>
              </label>
              {/* AND / OR toggle — only shown when there are 2+ filters */}
              {filters.length >= 2 && (
                <div className="flex items-center gap-1 text-xs">
                  <span className="text-gray-400 mr-1">Logic:</span>
                  {(["AND", "OR"] as const).map((mode) => (
                    <button
                      key={mode}
                      type="button"
                      onClick={() => setFilterLogic(mode)}
                      className={`px-2.5 py-1 rounded-md border font-medium transition-colors ${
                        filterLogic === mode
                          ? "bg-blue-600 text-white border-blue-600"
                          : "bg-white text-gray-500 border-gray-300 hover:border-gray-400"
                      }`}
                    >
                      {mode}
                    </button>
                  ))}
                </div>
              )}
            </div>
            <div className="space-y-2">
              {filters.map((f, idx) => (
                <div key={idx} className="flex gap-2 items-center">
                  {/* Column selector */}
                  {columns.length > 0 ? (
                    <select
                      value={f.column}
                      onChange={(e) => updateFilter(idx, "column", e.target.value)}
                      className="border border-gray-300 rounded-lg px-3 py-2 text-sm flex-1 focus:outline-none focus:ring-2 focus:ring-blue-500"
                    >
                      <option value="">— column —</option>
                      {columns.map((c) => (
                        <option key={c} value={c}>{c}</option>
                      ))}
                    </select>
                  ) : (
                    <input
                      type="text"
                      placeholder="column"
                      value={f.column}
                      onChange={(e) => updateFilter(idx, "column", e.target.value)}
                      className="border border-gray-300 rounded-lg px-3 py-2 text-sm flex-1 focus:outline-none focus:ring-2 focus:ring-blue-500"
                    />
                  )}

                  {/* Filter type toggle: = vs ~ (regex) */}
                  <button
                    type="button"
                    title={f.filter_type === "regex" ? "Regex match (click to switch to equals)" : "Exact match (click to switch to regex)"}
                    onClick={() =>
                      updateFilter(idx, "filter_type", f.filter_type === "regex" ? "equals" : "regex")
                    }
                    className={`px-2 py-2 rounded-lg border text-sm font-mono font-semibold transition-colors flex-shrink-0 ${
                      f.filter_type === "regex"
                        ? "bg-violet-600 text-white border-violet-600"
                        : "bg-white text-gray-400 border-gray-300 hover:border-gray-400"
                    }`}
                  >
                    {f.filter_type === "regex" ? "~" : "="}
                  </button>

                  {/* Value input */}
                  <input
                    type="text"
                    placeholder={f.filter_type === "regex" ? "regex pattern" : "value"}
                    value={f.value}
                    onChange={(e) => updateFilter(idx, "value", e.target.value)}
                    className="border border-gray-300 rounded-lg px-3 py-2 text-sm flex-1 focus:outline-none focus:ring-2 focus:ring-blue-500"
                  />

                  {filters.length > 1 && (
                    <button
                      type="button"
                      onClick={() => removeFilter(idx)}
                      className="text-gray-400 hover:text-red-500 text-lg leading-none"
                    >
                      ×
                    </button>
                  )}
                </div>
              ))}
            </div>
            <button
              type="button"
              onClick={addFilter}
              className="text-sm text-blue-600 hover:text-blue-700 mt-2"
            >
              + Add filter
            </button>
          </div>

          {/* Deduplication */}
          <div className="flex items-start gap-3">
            <input
              id="dedup"
              type="checkbox"
              checked={deduplicate}
              onChange={(e) => setDeduplicate(e.target.checked)}
              className="mt-0.5 rounded"
            />
            <label htmlFor="dedup" className="text-sm text-gray-700">
              <span className="font-medium">Deduplicate rows</span> — if multiple rows
              share the same key within a stage, keep the first and warn me
            </label>
          </div>

          {error && (
            <div className="bg-red-50 border border-red-200 rounded-lg p-3 text-sm text-red-700">
              {error}
            </div>
          )}

          <button
            type="submit"
            disabled={loading || keyColumns.length === 0}
            className="w-full bg-blue-600 hover:bg-blue-700 disabled:bg-blue-300 text-white font-medium py-2.5 rounded-lg transition-colors"
          >
            {loading ? "Analyzing…" : "Run Analysis"}
          </button>
        </form>
      </div>
    </div>
  );
}
