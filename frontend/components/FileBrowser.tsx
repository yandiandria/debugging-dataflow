import { useState, useMemo } from "react";
import type { BlobInfo } from "../lib/api";

const STAGE_COLORS: Record<string, string> = {
  extract: "bg-purple-100 text-purple-700",
  transform: "bg-blue-100 text-blue-700",
  clean_cleaned: "bg-teal-100 text-teal-700",
  clean_incoherent: "bg-rose-100 text-rose-700",
  compare_identify_in_flow_only: "bg-indigo-100 text-indigo-700",
  compare_identify_in_flow_and_db_different: "bg-violet-100 text-violet-700",
  compare: "bg-yellow-100 text-yellow-700",
  clean: "bg-cyan-100 text-cyan-700",
  load: "bg-green-100 text-green-700",
  unknown: "bg-gray-100 text-gray-600",
};

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 ** 2) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / 1024 ** 2).toFixed(1)} MB`;
}

interface Props {
  blobs: BlobInfo[];
  selected: Set<string>;
  onSelectionChange: (selected: Set<string>) => void;
  onAnalyze: () => void;
  onDisconnect: () => void;
}

export default function FileBrowser({
  blobs,
  selected,
  onSelectionChange,
  onAnalyze,
  onDisconnect,
}: Props) {
  const [search, setSearch] = useState("");
  const [stageFilter, setStageFilter] = useState<string>("all");

  const stages = useMemo(() => {
    const s = new Set(blobs.map((b) => b.detected_stage ?? "unknown"));
    return ["all", ...Array.from(s).sort()];
  }, [blobs]);

  const filtered = useMemo(() => {
    return blobs.filter((b) => {
      const matchesSearch = b.name.toLowerCase().includes(search.toLowerCase());
      const matchesStage =
        stageFilter === "all" ||
        (b.detected_stage ?? "unknown") === stageFilter;
      return matchesSearch && matchesStage;
    });
  }, [blobs, search, stageFilter]);

  const toggleOne = (name: string) => {
    const next = new Set(selected);
    if (next.has(name)) next.delete(name);
    else next.add(name);
    onSelectionChange(next);
  };

  const toggleAll = () => {
    if (filtered.every((b) => selected.has(b.name))) {
      const next = new Set(selected);
      filtered.forEach((b) => next.delete(b.name));
      onSelectionChange(next);
    } else {
      const next = new Set(selected);
      filtered.forEach((b) => next.add(b.name));
      onSelectionChange(next);
    }
  };

  const allFilteredSelected =
    filtered.length > 0 && filtered.every((b) => selected.has(b.name));

  return (
    <div className="flex flex-col h-full">
      {/* Toolbar */}
      <div className="flex items-center justify-between mb-4 flex-wrap gap-2">
        <div className="flex items-center gap-2 flex-1 min-w-0">
          <input
            type="text"
            placeholder="Search files…"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 flex-1 min-w-0"
          />
          <select
            value={stageFilter}
            onChange={(e) => setStageFilter(e.target.value)}
            className="border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            {stages.map((s) => (
              <option key={s} value={s}>
                {s === "all" ? "All stages" : s}
              </option>
            ))}
          </select>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-sm text-gray-500">
            {selected.size} selected
          </span>
          <button
            onClick={onAnalyze}
            disabled={selected.size === 0}
            className="bg-blue-600 hover:bg-blue-700 disabled:bg-blue-300 text-white text-sm font-medium px-4 py-2 rounded-lg transition-colors"
          >
            Analyze →
          </button>
          <button
            onClick={onDisconnect}
            className="text-sm text-gray-500 hover:text-gray-700 px-3 py-2 rounded-lg border border-gray-200 hover:border-gray-300 transition-colors"
          >
            Disconnect
          </button>
        </div>
      </div>

      {/* Table */}
      <div className="flex-1 overflow-auto border border-gray-200 rounded-xl">
        <table className="w-full text-sm">
          <thead className="bg-gray-50 sticky top-0 z-10">
            <tr>
              <th className="px-3 py-2.5 text-left w-8">
                <input
                  type="checkbox"
                  checked={allFilteredSelected}
                  onChange={toggleAll}
                  className="rounded"
                />
              </th>
              <th className="px-3 py-2.5 text-left font-medium text-gray-600">
                File name
              </th>
              <th className="px-3 py-2.5 text-left font-medium text-gray-600">
                Stage
              </th>
              <th className="px-3 py-2.5 text-right font-medium text-gray-600">
                Size
              </th>
              <th className="px-3 py-2.5 text-left font-medium text-gray-600">
                Last modified
              </th>
            </tr>
          </thead>
          <tbody>
            {filtered.length === 0 && (
              <tr>
                <td colSpan={5} className="px-3 py-8 text-center text-gray-400">
                  No files match your search.
                </td>
              </tr>
            )}
            {filtered.map((blob) => {
              const stage = blob.detected_stage ?? "unknown";
              const isSelected = selected.has(blob.name);
              return (
                <tr
                  key={blob.name}
                  onClick={() => toggleOne(blob.name)}
                  className={`border-t border-gray-100 cursor-pointer hover:bg-blue-50 transition-colors ${
                    isSelected ? "bg-blue-50" : ""
                  }`}
                >
                  <td className="px-3 py-2.5">
                    <input
                      type="checkbox"
                      checked={isSelected}
                      onChange={() => toggleOne(blob.name)}
                      onClick={(e) => e.stopPropagation()}
                      className="rounded"
                    />
                  </td>
                  <td className="px-3 py-2.5 font-mono text-xs text-gray-800 break-all">
                    {blob.name}
                  </td>
                  <td className="px-3 py-2.5">
                    <span
                      className={`inline-block px-2 py-0.5 rounded-full text-xs font-medium ${
                        STAGE_COLORS[stage] ?? STAGE_COLORS.unknown
                      }`}
                    >
                      {stage}
                    </span>
                  </td>
                  <td className="px-3 py-2.5 text-right text-gray-500 tabular-nums">
                    {formatBytes(blob.size)}
                  </td>
                  <td className="px-3 py-2.5 text-gray-500 text-xs">
                    {blob.last_modified
                      ? new Date(blob.last_modified).toLocaleString()
                      : "—"}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
