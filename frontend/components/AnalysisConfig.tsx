import { useState, useEffect } from "react";
import { getBlobColumns, getBlobPreview } from "../lib/api";
import type { FilterCondition, BlobPreview } from "../lib/api";

interface InitialConfig {
  keyColumns: string[];
  filters: FilterCondition[];
  deduplicate: boolean;
  filterLogic: "AND" | "OR";
}

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
  initialConfig?: InitialConfig;
}

export default function AnalysisConfig({
  containerUrl,
  selectedBlobs,
  onSubmit,
  onBack,
  loading,
  error,
  initialConfig,
}: Props) {
  const [columns, setColumns] = useState<string[]>([]);
  const [columnsLoading, setColumnsLoading] = useState(true);
  const [columnsError, setColumnsError] = useState<string | null>(null);

  const [keyColumns, setKeyColumns] = useState<string[]>(initialConfig?.keyColumns ?? []);
  const [filters, setFilters] = useState<FilterCondition[]>(
    initialConfig?.filters?.length ? initialConfig.filters : [{ column: "", value: "", filter_type: "equals" }]
  );
  const [filterLogic, setFilterLogic] = useState<"AND" | "OR">(initialConfig?.filterLogic ?? "AND");
  const [deduplicate, setDeduplicate] = useState(initialConfig?.deduplicate ?? true);

  // Preview state
  const [previewBlob, setPreviewBlob] = useState<string>(selectedBlobs[0] ?? "");
  const [preview, setPreview] = useState<BlobPreview | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewError, setPreviewError] = useState<string | null>(null);

  // Load columns from the first selected blob
  useEffect(() => {
    if (selectedBlobs.length === 0) return;
    setColumnsLoading(true);
    setColumnsError(null);
    getBlobColumns(containerUrl, selectedBlobs[0])
      .then((cols) => { setColumns(cols); setColumnsLoading(false); })
      .catch((e) => { setColumnsError(e.message); setColumnsLoading(false); });
  }, [containerUrl, selectedBlobs]);

  // Load preview when previewBlob changes
  useEffect(() => {
    if (!previewBlob) return;
    setPreviewLoading(true);
    setPreviewError(null);
    setPreview(null);
    getBlobPreview(containerUrl, previewBlob)
      .then((data) => { setPreview(data); setPreviewLoading(false); })
      .catch((e) => { setPreviewError(e.message); setPreviewLoading(false); });
  }, [containerUrl, previewBlob]);

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

  /** Clicking a cell in the preview adds (or sets) a filter for that column. */
  const handleCellClick = (col: string, value: unknown) => {
    const strVal = value === null || value === undefined ? "" : String(value);
    const existingIdx = filters.findIndex((f) => f.column === col || f.column === "");
    if (existingIdx !== -1) {
      setFilters((prev) => {
        const next = [...prev];
        next[existingIdx] = { column: col, value: strVal, filter_type: "equals" };
        return next;
      });
    } else {
      setFilters((prev) => [...prev, { column: col, value: strVal, filter_type: "equals" }]);
    }
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const validFilters = filters.filter((f) => f.column && f.value);
    onSubmit({ keyColumns, filters: validFilters, deduplicate, filterLogic });
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
        <h2 className="text-sm font-semibold text-gray-800">Configure Analysis</h2>
        <span className="text-xs text-gray-400 bg-gray-100 px-2 py-0.5 rounded-full">
          {selectedBlobs.length} file{selectedBlobs.length !== 1 ? "s" : ""} selected
        </span>
      </div>

      {/* Split body */}
      <div className="flex flex-1 overflow-hidden">

        {/* ── Left: config form ────────────────────────────────────────────── */}
        <div className="w-96 flex-shrink-0 bg-white border-r border-gray-200 overflow-y-auto p-6">
          {columnsLoading && <p className="text-sm text-gray-400 mb-4">Loading columns…</p>}
          {columnsError && (
            <div className="bg-red-50 border border-red-200 rounded-lg p-3 text-sm text-red-700 mb-4">
              Could not read columns: {columnsError}
            </div>
          )}

          <form onSubmit={handleSubmit} className="space-y-6">
            {/* Key columns */}
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Key column(s)
              </label>
              <p className="text-xs text-gray-400 mb-2">
                Used to identify and join rows across stages.
              </p>
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
                    placeholder="Type column name and press Enter"
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
                <p className="text-xs text-blue-600 mt-1">Selected: {keyColumns.join(", ")}</p>
              )}
            </div>

            {/* Filters */}
            <div>
              <div className="flex items-center justify-between mb-2">
                <label className="block text-sm font-medium text-gray-700">
                  Filters{" "}
                  <span className="text-gray-400 font-normal">(optional)</span>
                </label>
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
              <p className="text-xs text-gray-400 mb-2">
                Click any cell in the preview to fill a filter value.
              </p>
              <div className="space-y-2">
                {filters.map((f, idx) => (
                  <div key={idx} className="flex gap-1.5 items-center">
                    {columns.length > 0 ? (
                      <select
                        value={f.column}
                        onChange={(e) => updateFilter(idx, "column", e.target.value)}
                        className="border border-gray-300 rounded-lg px-2 py-1.5 text-sm flex-1 focus:outline-none focus:ring-2 focus:ring-blue-500 min-w-0"
                      >
                        <option value="">— col —</option>
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
                        className="border border-gray-300 rounded-lg px-2 py-1.5 text-sm flex-1 focus:outline-none focus:ring-2 focus:ring-blue-500 min-w-0"
                      />
                    )}
                    <button
                      type="button"
                      title={f.filter_type === "regex" ? "Regex (click for equals)" : "Equals (click for regex)"}
                      onClick={() =>
                        updateFilter(idx, "filter_type", f.filter_type === "regex" ? "equals" : "regex")
                      }
                      className={`px-2 py-1.5 rounded-lg border text-xs font-mono font-semibold transition-colors flex-shrink-0 ${
                        f.filter_type === "regex"
                          ? "bg-violet-600 text-white border-violet-600"
                          : "bg-white text-gray-400 border-gray-300 hover:border-gray-400"
                      }`}
                    >
                      {f.filter_type === "regex" ? "~" : "="}
                    </button>
                    <input
                      type="text"
                      placeholder={f.filter_type === "regex" ? "pattern" : "value"}
                      value={f.value}
                      onChange={(e) => updateFilter(idx, "value", e.target.value)}
                      className="border border-gray-300 rounded-lg px-2 py-1.5 text-sm flex-1 focus:outline-none focus:ring-2 focus:ring-blue-500 min-w-0"
                    />
                    {filters.length > 1 && (
                      <button
                        type="button"
                        onClick={() => removeFilter(idx)}
                        className="text-gray-400 hover:text-red-500 text-lg leading-none flex-shrink-0"
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
                <span className="font-medium">Deduplicate rows</span> — keep the first
                row per key within a stage and warn
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

        {/* ── Right: data preview ──────────────────────────────────────────── */}
        <div className="flex-1 flex flex-col overflow-hidden p-4 gap-3">
          {/* Preview toolbar */}
          <div className="flex items-center gap-3 flex-shrink-0">
            <span className="text-sm font-medium text-gray-700">Preview</span>
            <select
              value={previewBlob}
              onChange={(e) => setPreviewBlob(e.target.value)}
              className="border border-gray-300 rounded-lg px-3 py-1.5 text-xs focus:outline-none focus:ring-2 focus:ring-blue-500 flex-1 max-w-lg font-mono"
            >
              {selectedBlobs.map((b) => (
                <option key={b} value={b}>{b}</option>
              ))}
            </select>
            {preview && (
              <span className="text-xs text-gray-400">
                {preview.total_rows_loaded} rows · {preview.columns.length} columns
              </span>
            )}
          </div>

          {/* Preview table */}
          <div className="flex-1 overflow-auto border border-gray-200 rounded-xl bg-white">
            {previewLoading && (
              <div className="flex items-center justify-center h-full text-sm text-gray-400">
                Loading preview…
              </div>
            )}
            {previewError && (
              <div className="flex items-center justify-center h-full text-sm text-red-500 p-4">
                {previewError}
              </div>
            )}
            {preview && !previewLoading && (
              <table className="w-full text-xs">
                <thead className="bg-gray-50 sticky top-0 z-10 border-b border-gray-200">
                  <tr>
                    {preview.columns.map((col) => (
                      <th
                        key={col}
                        className={`px-3 py-2 text-left font-medium whitespace-nowrap border-r border-gray-100 last:border-r-0 ${
                          filters.some((f) => f.column === col)
                            ? "text-blue-700 bg-blue-50"
                            : "text-gray-600"
                        }`}
                      >
                        {col}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {preview.rows.map((row, i) => (
                    <tr
                      key={i}
                      className="border-t border-gray-100 hover:bg-gray-50"
                    >
                      {preview.columns.map((col) => {
                        const val = row[col];
                        const isFiltered = filters.some(
                          (f) => f.column === col && f.value === String(val ?? "")
                        );
                        return (
                          <td
                            key={col}
                            onClick={() => handleCellClick(col, val)}
                            title="Click to use as filter"
                            className={`px-3 py-1.5 border-r border-gray-100 last:border-r-0 cursor-pointer whitespace-nowrap max-w-xs truncate transition-colors ${
                              isFiltered
                                ? "bg-blue-50 text-blue-700 font-medium"
                                : "text-gray-700 hover:bg-blue-50 hover:text-blue-700"
                            }`}
                          >
                            {val === null || val === undefined ? (
                              <span className="text-gray-300 italic">null</span>
                            ) : (
                              String(val)
                            )}
                          </td>
                        );
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
