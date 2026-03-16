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

// Stage keywords ordered longest-first (same as backend STAGE_ORDER)
const STAGE_KEYWORDS = [
  "clean_incoherent",
  "clean_cleaned",
  "compare_and_identify_in_flow_and_db_different",
  "compare_and_identify_in_flow_only",
  "compare_and_identify_not_linked_mandatory",
  "compare_and_identify_not_linked_optional",
  "extract",
  "transform",
  "load",
  "clean",
  "compare",
];

function extractSuffix(name: string, stage: string | null): string {
  const stem = name.split("/").pop()?.replace(/\.csv$/i, "") ?? name;
  if (!stage) return stem || "—";
  const keyword = STAGE_KEYWORDS.find((k) => stem.toLowerCase().includes(k));
  if (!keyword) return stem || "—";
  const stripped = stem.replace(new RegExp(keyword, "i"), "").replace(/^[_\-.\s]+|[_\-.\s]+$/g, "");
  return stripped || stem;
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 ** 2) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / 1024 ** 2).toFixed(1)} MB`;
}

function formatDate(iso: string): string {
  if (!iso) return "Unknown date";
  return new Date(iso).toLocaleDateString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

interface GroupKey {
  date: string;   // YYYY-MM-DD
  suffix: string;
}

interface BlobGroup {
  key: GroupKey;
  label: string;
  blobs: BlobInfo[];
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
  const [collapsedGroups, setCollapsedGroups] = useState<Set<string>>(new Set());

  const stages = useMemo(() => {
    const s = new Set(blobs.map((b) => b.detected_stage ?? "unknown"));
    return ["all", ...Array.from(s).sort()];
  }, [blobs]);

  const filtered = useMemo(() => {
    return blobs.filter((b) => {
      const matchesSearch = b.name.toLowerCase().includes(search.toLowerCase());
      const matchesStage =
        stageFilter === "all" || (b.detected_stage ?? "unknown") === stageFilter;
      return matchesSearch && matchesStage;
    });
  }, [blobs, search, stageFilter]);

  const groups = useMemo((): BlobGroup[] => {
    const map = new Map<string, BlobGroup>();

    for (const blob of filtered) {
      const date = blob.last_modified ? blob.last_modified.slice(0, 10) : "unknown";
      const suffix = extractSuffix(blob.name, blob.detected_stage);
      const key = `${date}__${suffix}`;

      if (!map.has(key)) {
        map.set(key, {
          key: { date, suffix },
          label: suffix ? `${suffix}  ·  ${formatDate(blob.last_modified)}` : formatDate(blob.last_modified),
          blobs: [],
        });
      }
      map.get(key)!.blobs.push(blob);
    }

    return Array.from(map.values()).sort((a, b) => {
      // Sort by date descending, then suffix ascending
      if (b.key.date !== a.key.date) return b.key.date.localeCompare(a.key.date);
      return a.key.suffix.localeCompare(b.key.suffix);
    });
  }, [filtered]);

  const toggleOne = (name: string) => {
    const next = new Set(selected);
    if (next.has(name)) next.delete(name);
    else next.add(name);
    onSelectionChange(next);
  };

  const toggleGroup = (group: BlobGroup) => {
    const allSelected = group.blobs.every((b) => selected.has(b.name));
    const next = new Set(selected);
    if (allSelected) {
      group.blobs.forEach((b) => next.delete(b.name));
    } else {
      group.blobs.forEach((b) => next.add(b.name));
    }
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

  const toggleCollapse = (groupKey: string) => {
    setCollapsedGroups((prev) => {
      const next = new Set(prev);
      if (next.has(groupKey)) next.delete(groupKey);
      else next.add(groupKey);
      return next;
    });
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
          <span className="text-sm text-gray-500">{selected.size} selected</span>
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

      {/* Groups */}
      <div className="flex-1 overflow-auto space-y-3">
        {groups.length === 0 && (
          <div className="border border-gray-200 rounded-xl px-4 py-10 text-center text-gray-400 text-sm">
            No files match your search.
          </div>
        )}

        {groups.map((group) => {
          const groupKeyStr = `${group.key.date}__${group.key.suffix}`;
          const isCollapsed = collapsedGroups.has(groupKeyStr);
          const allGroupSelected = group.blobs.every((b) => selected.has(b.name));
          const someGroupSelected = group.blobs.some((b) => selected.has(b.name));

          return (
            <div
              key={groupKeyStr}
              className="border border-gray-200 rounded-xl overflow-hidden"
            >
              {/* Group header */}
              <div className="bg-gray-50 px-4 py-2.5 flex items-center justify-between gap-3 border-b border-gray-200">
                <div className="flex items-center gap-3">
                  <input
                    type="checkbox"
                    checked={allGroupSelected}
                    ref={(el) => {
                      if (el) el.indeterminate = !allGroupSelected && someGroupSelected;
                    }}
                    onChange={() => toggleGroup(group)}
                    className="rounded"
                    onClick={(e) => e.stopPropagation()}
                  />
                  <button
                    onClick={() => toggleCollapse(groupKeyStr)}
                    className="flex items-center gap-2 text-left"
                  >
                    <span className="text-gray-400 text-xs">{isCollapsed ? "▶" : "▼"}</span>
                    <span className="text-sm font-medium text-gray-700">{group.key.suffix || "—"}</span>
                    <span className="text-xs text-gray-400">·</span>
                    <span className="text-xs text-gray-500">{formatDate(group.blobs[0].last_modified)}</span>
                  </button>
                </div>
                <div className="flex items-center gap-2">
                  <span className="text-xs text-gray-400">
                    {group.blobs.length} file{group.blobs.length !== 1 ? "s" : ""}
                  </span>
                  {someGroupSelected && (
                    <span className="text-xs text-blue-600 font-medium">
                      {group.blobs.filter((b) => selected.has(b.name)).length} selected
                    </span>
                  )}
                </div>
              </div>

              {/* Group rows */}
              {!isCollapsed && (
                <table className="w-full text-sm">
                  <tbody>
                    {group.blobs.map((blob) => {
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
                          <td className="px-4 py-2.5 w-8">
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
                          <td className="px-3 py-2.5 whitespace-nowrap">
                            <span
                              className={`inline-block px-2 py-0.5 rounded-full text-xs font-medium ${
                                STAGE_COLORS[stage] ?? STAGE_COLORS.unknown
                              }`}
                            >
                              {stage}
                            </span>
                          </td>
                          <td className="px-3 py-2.5 text-right text-gray-500 tabular-nums whitespace-nowrap">
                            {formatBytes(blob.size)}
                          </td>
                          <td className="px-3 py-2.5 text-gray-500 text-xs whitespace-nowrap">
                            {blob.last_modified
                              ? new Date(blob.last_modified).toLocaleString()
                              : "—"}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              )}
            </div>
          );
        })}
      </div>

      {/* Footer summary */}
      {groups.length > 0 && (
        <div className="mt-3 flex items-center justify-between text-xs text-gray-400">
          <span>
            {groups.length} group{groups.length !== 1 ? "s" : ""} · {filtered.length} file{filtered.length !== 1 ? "s" : ""}
          </span>
          <button onClick={toggleAll} className="hover:text-gray-600 transition-colors">
            {allFilteredSelected ? "Deselect all" : "Select all"}
          </button>
        </div>
      )}
    </div>
  );
}
