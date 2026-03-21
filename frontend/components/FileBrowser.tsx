import { useState, useMemo } from "react";
import type { BlobInfo, Resource } from "../lib/api";

const STAGE_COLORS: Record<string, string> = {
  extract: "bg-purple-100 text-purple-700",
  transform: "bg-blue-100 text-blue-700",
  clean_cleaned: "bg-teal-100 text-teal-700",
  clean_incoherent: "bg-rose-100 text-rose-700",
  compare_and_identify_in_flow_only: "bg-blue-100 text-blue-700",
  compare_and_identify_in_flow_and_db_different: "bg-violet-100 text-violet-700",
  compare_and_identify_not_linked_optional: "bg-yellow-100 text-yellow-700",
  compare_and_identify_not_linked_mandatory: "bg-orange-100 text-orange-700",
  unknown: "bg-gray-100 text-gray-600",
};

const OVERRIDES_KEY = "dataflow-group-overrides";

function loadOverrides(): Map<string, string> {
  try {
    const saved = localStorage.getItem(OVERRIDES_KEY);
    return saved ? new Map(JSON.parse(saved)) : new Map();
  } catch {
    return new Map();
  }
}

function saveOverrides(overrides: Map<string, string>) {
  localStorage.setItem(OVERRIDES_KEY, JSON.stringify(Array.from(overrides.entries())));
}

/**
 * Build a map from each blob name → its group prefix (stem of the extract blob).
 */
function buildGroupPrefixMap(allBlobs: BlobInfo[], resources: Resource[]): Map<string, string> {
  // Resources sorted longest-first so the most specific prefix wins
  const sortedResources = [...resources].sort(
    (a, b) => b.technical_name.length - a.technical_name.length
  );

  // Extract stems as fallback grouping
  const extractStems = allBlobs
    .filter((b) => b.detected_stage === "extract")
    .map((b) => b.name.replace(/\.csv$/i, ""))
    .sort((a, b) => b.length - a.length);

  const map = new Map<string, string>();
  for (const blob of allBlobs) {
    // Resource prefix takes priority
    const matchedResource = sortedResources.find((r) =>
      blob.name.startsWith(r.technical_name)
    );
    if (matchedResource) {
      map.set(blob.name, matchedResource.technical_name);
      continue;
    }
    // Fallback: extract-based grouping
    const match = extractStems.find((stem) => blob.name.startsWith(stem));
    map.set(blob.name, match ?? blob.name.replace(/\.csv$/i, ""));
  }
  return map;
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
  date: string;
  suffix: string;
}

interface BlobGroup {
  key: GroupKey;
  blobs: BlobInfo[];
}

interface Props {
  blobs: BlobInfo[];
  selected: Set<string>;
  onSelectionChange: (selected: Set<string>) => void;
  onAnalyze: () => void;
  onDisconnect: () => void;
  resources: Resource[];
  onAssignResource: (technicalName: string, businessName: string) => Promise<void>;
  onManageResources: () => void;
}

export default function FileBrowser({
  blobs,
  selected,
  onSelectionChange,
  onAnalyze,
  onDisconnect,
  resources,
  onAssignResource,
  onManageResources,
}: Props) {
  const [search, setSearch] = useState("");
  const [stageFilter, setStageFilter] = useState<string>("all");
  const [resourceFilter, setResourceFilter] = useState<string>("all");
  const [dateFilter, setDateFilter] = useState<string>("all");
  const [collapsedGroups, setCollapsedGroups] = useState<Set<string>>(new Set());

  // assign form state: key of the group being assigned
  const [assigningGroup, setAssigningGroup] = useState<string | null>(null);
  const [assignBusiness, setAssignBusiness] = useState("");
  const [assignTechnical, setAssignTechnical] = useState("");
  const [assignExisting, setAssignExisting] = useState("");
  const [assignSaving, setAssignSaving] = useState(false);

  // manual group override state
  const [manualGroupOverrides, setManualGroupOverrides] = useState<Map<string, string>>(loadOverrides);
  const [movingBlob, setMovingBlob] = useState<string | null>(null);
  const [moveTargetPrefix, setMoveTargetPrefix] = useState("");

  const stages = useMemo(() => {
    const s = new Set(blobs.map((b) => b.detected_stage ?? "unknown"));
    return ["all", ...Array.from(s).sort()];
  }, [blobs]);

  // Map technical_name → resource for fast lookup
  const resourceByTechnical = useMemo(() => {
    const m = new Map<string, Resource>();
    for (const r of resources) m.set(r.technical_name, r);
    return m;
  }, [resources]);

  const filtered = useMemo(() => {
    return blobs.filter((b) => {
      const matchesSearch = b.name.toLowerCase().includes(search.toLowerCase());
      const matchesStage =
        stageFilter === "all" || (b.detected_stage ?? "unknown") === stageFilter;
      return matchesSearch && matchesStage;
    });
  }, [blobs, search, stageFilter]);

  const groupPrefixMap = useMemo(() => {
    const map = buildGroupPrefixMap(blobs, resources);
    // Apply manual overrides on top of auto-detected prefixes
    for (const [blobName, prefix] of manualGroupOverrides) {
      if (map.has(blobName)) map.set(blobName, prefix);
    }
    return map;
  }, [blobs, resources, manualGroupOverrides]);

  const allGroups = useMemo((): BlobGroup[] => {
    const map = new Map<string, BlobGroup>();
    for (const blob of filtered) {
      const prefix = groupPrefixMap.get(blob.name) ?? blob.name.replace(/\.csv$/i, "");
      const date = blob.last_modified ? blob.last_modified.slice(0, 10) : "unknown";
      const key = `${prefix}__${date}`;
      if (!map.has(key)) {
        map.set(key, { key: { date, suffix: prefix }, blobs: [] });
      }
      map.get(key)!.blobs.push(blob);
    }
    const groups = Array.from(map.values());
    // Sort blobs within each group newest first
    for (const g of groups) {
      g.blobs.sort((a, b) => (b.last_modified ?? "").localeCompare(a.last_modified ?? ""));
    }
    // Sort groups by their most recent blob's full timestamp, then by suffix
    return groups.sort((a, b) => {
      const aMax = a.blobs[0]?.last_modified ?? "";
      const bMax = b.blobs[0]?.last_modified ?? "";
      if (bMax !== aMax) return bMax.localeCompare(aMax);
      return a.key.suffix.localeCompare(b.key.suffix);
    });
  }, [filtered, groupPrefixMap]);

  // Available dates for the selected resource filter (ordered most recent first)
  const availableDates = useMemo(() => {
    if (resourceFilter === "all") return [];
    const res = resources.find((r) => r.id === resourceFilter);
    if (!res) return [];
    const resourceGroups = allGroups.filter((g) => g.key.suffix === res.technical_name);
    // Collect unique last_modified timestamps from matching blobs, ordered most recent first
    const seen = new Set<string>();
    const dates: { label: string; iso: string }[] = [];
    for (const g of resourceGroups) {
      for (const blob of g.blobs) {
        const iso = blob.last_modified ?? "";
        if (!iso || seen.has(iso)) continue;
        seen.add(iso);
        dates.push({ iso, label: new Date(iso).toLocaleString() });
      }
    }
    dates.sort((a, b) => b.iso.localeCompare(a.iso));
    return dates;
  }, [allGroups, resourceFilter, resources]);

  const groups = useMemo(() => {
    if (resourceFilter === "all") return allGroups;
    const res = resources.find((r) => r.id === resourceFilter);
    if (!res) return allGroups;
    let result = allGroups.filter((g) => g.key.suffix === res.technical_name);
    if (dateFilter !== "all") {
      result = result
        .map((g) => ({
          ...g,
          blobs: g.blobs.filter((b) => b.last_modified === dateFilter),
        }))
        .filter((g) => g.blobs.length > 0);
    }
    return result;
  }, [allGroups, resourceFilter, resources, dateFilter]);

  // All unique prefixes available as move targets
  const availablePrefixes = useMemo(() => {
    const set = new Set<string>();
    for (const g of allGroups) set.add(g.key.suffix);
    return Array.from(set).sort();
  }, [allGroups]);

  const toggleOne = (name: string) => {
    const next = new Set(selected);
    if (next.has(name)) next.delete(name);
    else next.add(name);
    onSelectionChange(next);
  };

  const toggleGroup = (group: BlobGroup) => {
    const allSel = group.blobs.every((b) => selected.has(b.name));
    const next = new Set(selected);
    if (allSel) group.blobs.forEach((b) => next.delete(b.name));
    else group.blobs.forEach((b) => next.add(b.name));
    onSelectionChange(next);
  };

  const toggleAll = () => {
    const visibleBlobs = groups.flatMap((g) => g.blobs);
    if (visibleBlobs.every((b) => selected.has(b.name))) {
      const next = new Set(selected);
      visibleBlobs.forEach((b) => next.delete(b.name));
      onSelectionChange(next);
    } else {
      const next = new Set(selected);
      visibleBlobs.forEach((b) => next.add(b.name));
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

  const openAssignForm = (groupKeyStr: string, suffix: string) => {
    setAssigningGroup(groupKeyStr);
    setAssignTechnical(suffix);
    setAssignBusiness("");
    setAssignExisting("");
  };

  const cancelAssign = () => {
    setAssigningGroup(null);
    setAssignBusiness("");
    setAssignTechnical("");
    setAssignExisting("");
  };

  const saveAssign = async () => {
    const techName = assignTechnical.trim();
    const busName = assignBusiness.trim();
    if (!techName || !busName) return;
    setAssignSaving(true);
    try {
      await onAssignResource(techName, busName);
      cancelAssign();
    } finally {
      setAssignSaving(false);
    }
  };

  const linkExisting = async () => {
    if (!assignExisting) return;
    const res = resources.find((r) => r.id === assignExisting);
    if (!res) return;
    setAssignSaving(true);
    try {
      // Update the resource's technical_name to this group's suffix
      await onAssignResource(assignTechnical.trim(), res.business_name);
      cancelAssign();
    } finally {
      setAssignSaving(false);
    }
  };

  const moveToGroup = (blobName: string, targetPrefix: string) => {
    const next = new Map(manualGroupOverrides);
    next.set(blobName, targetPrefix);
    setManualGroupOverrides(next);
    saveOverrides(next);
    setMovingBlob(null);
    setMoveTargetPrefix("");
  };

  const resetGroupOverride = (blobName: string) => {
    const next = new Map(manualGroupOverrides);
    next.delete(blobName);
    setManualGroupOverrides(next);
    saveOverrides(next);
  };

  const startMoveBlob = (e: React.MouseEvent, blobName: string, currentPrefix: string) => {
    e.stopPropagation();
    setMovingBlob(blobName);
    setMoveTargetPrefix(currentPrefix);
  };

  const cancelMove = (e: React.MouseEvent) => {
    e.stopPropagation();
    setMovingBlob(null);
    setMoveTargetPrefix("");
  };

  const confirmMove = (e: React.MouseEvent, blobName: string) => {
    e.stopPropagation();
    if (moveTargetPrefix) moveToGroup(blobName, moveTargetPrefix);
  };

  const visibleBlobs = groups.flatMap((g) => g.blobs);
  const allFilteredSelected =
    visibleBlobs.length > 0 && visibleBlobs.every((b) => selected.has(b.name));

  return (
    <div className="flex flex-col h-full">
      {/* Toolbar */}
      <div className="flex items-center justify-between mb-4 flex-wrap gap-2">
        <div className="flex items-center gap-2 flex-1 min-w-0 flex-wrap">
          <input
            type="text"
            placeholder="Search files…"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 flex-1 min-w-32"
          />
          <select
            value={stageFilter}
            onChange={(e) => setStageFilter(e.target.value)}
            className="border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            {stages.map((s) => (
              <option key={s} value={s}>{s === "all" ? "All stages" : s}</option>
            ))}
          </select>
          <select
            value={resourceFilter}
            onChange={(e) => { setResourceFilter(e.target.value); setDateFilter("all"); }}
            className="border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            <option value="all">All resources</option>
            {resources.map((r) => (
              <option key={r.id} value={r.id}>{r.business_name}</option>
            ))}
          </select>
          {resourceFilter !== "all" && availableDates.length > 0 && (
            <select
              value={dateFilter}
              onChange={(e) => setDateFilter(e.target.value)}
              className="border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <option value="all">All dates</option>
              {availableDates.map((d) => (
                <option key={d.iso} value={d.iso}>{d.label}</option>
              ))}
            </select>
          )}
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={onManageResources}
            className="text-xs text-gray-500 hover:text-gray-700 px-3 py-2 rounded-lg border border-gray-200 hover:border-gray-300 transition-colors"
          >
            Manage resources
          </button>
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
          const linkedResource = resourceByTechnical.get(group.key.suffix);
          const isAssigning = assigningGroup === groupKeyStr;

          return (
            <div key={groupKeyStr} className="border border-gray-200 rounded-xl overflow-hidden">
              {/* Group header */}
              <div className="bg-gray-50 px-4 py-2.5 flex items-center justify-between gap-3 border-b border-gray-200">
                <div className="flex items-center gap-3 min-w-0">
                  <input
                    type="checkbox"
                    checked={allGroupSelected}
                    ref={(el) => {
                      if (el) el.indeterminate = !allGroupSelected && someGroupSelected;
                    }}
                    onChange={() => toggleGroup(group)}
                    className="rounded flex-shrink-0"
                    onClick={(e) => e.stopPropagation()}
                  />
                  <button
                    onClick={() => toggleCollapse(groupKeyStr)}
                    className="flex items-center gap-2 text-left min-w-0"
                  >
                    <span className="text-gray-400 text-xs flex-shrink-0">{isCollapsed ? "▶" : "▼"}</span>
                    <span className="text-sm font-medium text-gray-700 truncate">
                      {group.key.suffix.split("/").pop() || "—"}
                    </span>
                    <span className="text-xs text-gray-400 flex-shrink-0">·</span>
                    <span className="text-xs text-gray-500 flex-shrink-0">{formatDate(group.blobs[0].last_modified)}</span>
                  </button>

                  {/* Resource badge or assign button */}
                  {linkedResource ? (
                    <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-teal-100 text-teal-700 flex-shrink-0">
                      {linkedResource.business_name}
                    </span>
                  ) : (
                    !isAssigning && (
                      <button
                        onClick={(e) => { e.stopPropagation(); openAssignForm(groupKeyStr, group.key.suffix); }}
                        className="text-xs text-gray-400 hover:text-teal-600 border border-dashed border-gray-300 hover:border-teal-400 rounded-full px-2 py-0.5 transition-colors flex-shrink-0"
                      >
                        + assign resource
                      </button>
                    )
                  )}
                </div>

                <div className="flex items-center gap-2 flex-shrink-0">
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

              {/* Inline assign form */}
              {isAssigning && (
                <div className="bg-teal-50 border-b border-teal-100 px-4 py-3 flex flex-col gap-2">
                  <p className="text-xs font-medium text-teal-800">Assign a resource to this group</p>

                  {/* Select existing */}
                  {resources.length > 0 && (
                    <div className="flex items-center gap-2">
                      <select
                        value={assignExisting}
                        onChange={(e) => setAssignExisting(e.target.value)}
                        className="border border-teal-300 rounded-lg px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-teal-400 flex-1"
                      >
                        <option value="">— select existing resource —</option>
                        {resources.map((r) => (
                          <option key={r.id} value={r.id}>{r.business_name}</option>
                        ))}
                      </select>
                      <button
                        onClick={linkExisting}
                        disabled={!assignExisting || assignSaving}
                        className="px-3 py-1.5 text-sm bg-teal-600 hover:bg-teal-700 disabled:bg-teal-300 text-white rounded-lg transition-colors"
                      >
                        Link
                      </button>
                    </div>
                  )}

                  <div className="flex items-center gap-1 text-xs text-teal-600">
                    <span className="flex-1 border-t border-teal-200" />
                    <span>or create new</span>
                    <span className="flex-1 border-t border-teal-200" />
                  </div>

                  {/* Create new */}
                  <div className="flex items-center gap-2">
                    <input
                      type="text"
                      placeholder="Business name"
                      value={assignBusiness}
                      onChange={(e) => setAssignBusiness(e.target.value)}
                      className="border border-teal-300 rounded-lg px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-teal-400 flex-1"
                      autoFocus
                    />
                    <input
                      type="text"
                      placeholder="Technical name (prefix)"
                      value={assignTechnical}
                      onChange={(e) => setAssignTechnical(e.target.value)}
                      className="border border-teal-300 rounded-lg px-2 py-1.5 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-teal-400 flex-1"
                    />
                    <button
                      onClick={saveAssign}
                      disabled={!assignBusiness.trim() || !assignTechnical.trim() || assignSaving}
                      className="px-3 py-1.5 text-sm bg-teal-600 hover:bg-teal-700 disabled:bg-teal-300 text-white rounded-lg transition-colors"
                    >
                      {assignSaving ? "Saving…" : "Save"}
                    </button>
                    <button
                      onClick={cancelAssign}
                      className="px-3 py-1.5 text-sm text-gray-500 hover:text-gray-700 border border-gray-200 rounded-lg transition-colors"
                    >
                      Cancel
                    </button>
                  </div>
                </div>
              )}

              {/* Group rows */}
              {!isCollapsed && (
                <table className="w-full text-sm">
                  <tbody>
                    {group.blobs.map((blob) => {
                      const stage = blob.detected_stage ?? "unknown";
                      const isSelected = selected.has(blob.name);
                      const isMoving = movingBlob === blob.name;
                      const hasOverride = manualGroupOverrides.has(blob.name);
                      const currentPrefix = groupPrefixMap.get(blob.name) ?? "";

                      return (
                        <tr
                          key={blob.name}
                          onClick={() => !isMoving && toggleOne(blob.name)}
                          className={`group/row border-t border-gray-100 cursor-pointer hover:bg-blue-50 transition-colors ${isSelected ? "bg-blue-50" : ""} ${isMoving ? "bg-amber-50" : ""}`}
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
                            <span className="flex items-center gap-1.5">
                              {blob.name}
                              {hasOverride && (
                                <span
                                  className="inline-block w-1.5 h-1.5 rounded-full bg-amber-400 flex-shrink-0"
                                  title="Manually assigned to this group"
                                />
                              )}
                            </span>
                          </td>
                          <td className="px-3 py-2.5 whitespace-nowrap">
                            <span className={`inline-block px-2 py-0.5 rounded-full text-xs font-medium ${STAGE_COLORS[stage] ?? STAGE_COLORS.unknown}`}>
                              {stage}
                            </span>
                          </td>
                          <td className="px-3 py-2.5 text-right text-gray-500 tabular-nums whitespace-nowrap">
                            {formatBytes(blob.size)}
                          </td>
                          <td className="px-3 py-2.5 text-gray-500 text-xs whitespace-nowrap">
                            {blob.last_modified ? new Date(blob.last_modified).toLocaleString() : "—"}
                          </td>
                          {/* Move to group column */}
                          <td className="px-3 py-2.5 w-56" onClick={(e) => e.stopPropagation()}>
                            {isMoving ? (
                              <div className="flex items-center gap-1">
                                <select
                                  value={moveTargetPrefix}
                                  onChange={(e) => setMoveTargetPrefix(e.target.value)}
                                  className="border border-amber-300 rounded px-1.5 py-1 text-xs focus:outline-none focus:ring-2 focus:ring-amber-400 flex-1 min-w-0"
                                  autoFocus
                                >
                                  {availablePrefixes.map((p) => (
                                    <option key={p} value={p}>
                                      {p === currentPrefix ? `${p.split("/").pop()} (current)` : p.split("/").pop()}
                                    </option>
                                  ))}
                                </select>
                                <button
                                  onClick={(e) => confirmMove(e, blob.name)}
                                  disabled={!moveTargetPrefix || moveTargetPrefix === currentPrefix}
                                  className="px-1.5 py-1 text-xs bg-amber-500 hover:bg-amber-600 disabled:bg-amber-200 text-white rounded transition-colors flex-shrink-0"
                                  title="Confirm move"
                                >
                                  ✓
                                </button>
                                {hasOverride && (
                                  <button
                                    onClick={(e) => { e.stopPropagation(); resetGroupOverride(blob.name); setMovingBlob(null); }}
                                    className="px-1.5 py-1 text-xs text-gray-500 hover:text-gray-700 border border-gray-200 rounded transition-colors flex-shrink-0"
                                    title="Reset to auto-detected group"
                                  >
                                    ↺
                                  </button>
                                )}
                                <button
                                  onClick={cancelMove}
                                  className="px-1.5 py-1 text-xs text-gray-400 hover:text-gray-600 flex-shrink-0"
                                  title="Cancel"
                                >
                                  ✕
                                </button>
                              </div>
                            ) : (
                              <button
                                onClick={(e) => startMoveBlob(e, blob.name, currentPrefix)}
                                className="opacity-0 group-hover/row:opacity-100 text-xs text-gray-400 hover:text-amber-600 border border-gray-200 hover:border-amber-300 rounded px-2 py-0.5 transition-all"
                                title="Move to a different group"
                              >
                                move →
                              </button>
                            )}
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

      {/* Footer */}
      {groups.length > 0 && (
        <div className="mt-3 flex items-center justify-between text-xs text-gray-400">
          <span>
            {groups.length} group{groups.length !== 1 ? "s" : ""} · {visibleBlobs.length} file{visibleBlobs.length !== 1 ? "s" : ""}
            {manualGroupOverrides.size > 0 && (
              <span className="ml-2 text-amber-500">
                · {manualGroupOverrides.size} manual override{manualGroupOverrides.size !== 1 ? "s" : ""}
              </span>
            )}
          </span>
          <button onClick={toggleAll} className="hover:text-gray-600 transition-colors">
            {allFilteredSelected ? "Deselect all" : "Select all"}
          </button>
        </div>
      )}
    </div>
  );
}
