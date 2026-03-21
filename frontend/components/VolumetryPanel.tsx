import { useState } from "react";
import type { BlobInfo, BlobProfile, Resource } from "../lib/api";

const STAGE_ORDER = [
  "extract",
  "clean_cleaned",
  "clean_incoherent",
  "compare_and_identify_in_flow_only",
  "compare_and_identify_in_flow_and_db_different",
  "compare_and_identify_not_linked_mandatory",
  "compare_and_identify_not_linked_optional",
  "load",
  "transform",
];

export interface VolumetryEntry {
  status: "idle" | "loading" | "loaded" | "error";
  profiles: BlobProfile[];
  error?: string;
  lastUpdated?: string;
}

interface Props {
  open: boolean;
  onToggle: () => void;
  resources: Resource[];
  blobs: BlobInfo[];
  volumetryData: Record<string, VolumetryEntry>;
  onRefresh: (resourceId: string) => void;
}

function groupProfilesByStage(profiles: BlobProfile[]) {
  const map = new Map<string, BlobProfile[]>();
  for (const bp of profiles) {
    const stage = bp.detected_stage ?? "unknown";
    if (!map.has(stage)) map.set(stage, []);
    map.get(stage)!.push(bp);
  }
  const orderedKeys = [
    ...STAGE_ORDER.filter((s) => map.has(s)),
    ...[...map.keys()].filter((s) => !STAGE_ORDER.includes(s)),
  ];
  return orderedKeys.map((stage) => ({
    stage,
    profiles: map.get(stage)!,
    totalRows: map.get(stage)!.reduce((s, p) => s + Math.max(p.row_count, 0), 0),
  }));
}

export default function VolumetryPanel({
  open,
  onToggle,
  resources,
  blobs,
  volumetryData,
  onRefresh,
}: Props) {
  const [expandedBlobs, setExpandedBlobs] = useState<Set<string>>(new Set());

  const toggleBlob = (key: string) => {
    setExpandedBlobs((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  };

  return (
    <>
      {/* Tab button on right edge */}
      <button
        onClick={onToggle}
        className={`fixed top-1/2 -translate-y-1/2 z-50 bg-white border border-gray-200 shadow-md px-1.5 py-4 text-xs text-gray-500 hover:text-gray-700 hover:bg-gray-50 transition-all duration-300 rounded-l-lg ${
          open ? "right-80" : "right-0"
        }`}
        style={{ writingMode: "vertical-rl", textOrientation: "mixed" }}
      >
        Volumetry
      </button>

      {/* Sliding panel */}
      <div
        className={`fixed top-0 right-0 h-screen w-80 bg-white border-l border-gray-200 shadow-xl z-40 flex flex-col transition-transform duration-300 ${
          open ? "translate-x-0" : "translate-x-full"
        }`}
      >
        {/* Header */}
        <div className="px-4 py-3 border-b border-gray-200 flex items-center justify-between flex-shrink-0">
          <h3 className="text-sm font-semibold text-gray-800">Volumetry</h3>
          <span className="text-xs text-gray-400">
            {blobs.length > 0 ? `${blobs.length} files` : "No container"}
          </span>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto">
          {resources.length === 0 ? (
            <div className="p-4 text-xs text-gray-400 text-center leading-relaxed">
              No resources defined yet. Assign resources to file groups in the file browser.
            </div>
          ) : (
            <div className="divide-y divide-gray-100">
              {resources.map((resource) => {
                const matchingBlobs = blobs.filter((b) =>
                  b.name.startsWith(resource.technical_name)
                );
                const entry = volumetryData[resource.id];
                const isLoading = entry?.status === "loading";
                const isLoaded = entry?.status === "loaded";
                const hasError = entry?.status === "error";
                const stageGroups = isLoaded ? groupProfilesByStage(entry.profiles) : [];

                return (
                  <div key={resource.id} className="p-3">
                    {/* Resource header */}
                    <div className="flex items-center gap-2 mb-1.5">
                      <div className="flex-1 min-w-0">
                        <div className="text-xs font-semibold text-gray-800 truncate">
                          {resource.business_name}
                        </div>
                        <div
                          className="text-xs text-gray-400 font-mono truncate"
                          title={resource.technical_name}
                        >
                          {resource.technical_name}
                        </div>
                      </div>
                      <button
                        onClick={() => onRefresh(resource.id)}
                        disabled={isLoading || matchingBlobs.length === 0}
                        title={
                          matchingBlobs.length === 0
                            ? "No matching files in container"
                            : isLoaded
                            ? "Refresh profile"
                            : "Load profile"
                        }
                        className="flex-shrink-0 text-sm px-2 py-1 bg-indigo-50 hover:bg-indigo-100 text-indigo-600 disabled:text-gray-300 disabled:bg-gray-50 rounded-md transition-colors"
                      >
                        {isLoading ? "…" : "↻"}
                      </button>
                    </div>

                    {matchingBlobs.length === 0 && (
                      <p className="text-xs text-gray-400 italic">No matching files in container.</p>
                    )}
                    {hasError && (
                      <p className="text-xs text-red-500">{entry.error}</p>
                    )}
                    {isLoading && (
                      <p className="text-xs text-gray-400 animate-pulse">Profiling {matchingBlobs.length} file(s)…</p>
                    )}
                    {isLoaded && entry.lastUpdated && (
                      <p className="text-xs text-gray-300 mb-1.5">
                        Updated {new Date(entry.lastUpdated).toLocaleTimeString()}
                      </p>
                    )}

                    {/* Stage groups */}
                    {isLoaded && stageGroups.length > 0 && (
                      <div className="space-y-3 mt-1">
                        {stageGroups.map((group, gi) => {
                          const prevGroup = gi > 0 ? stageGroups[gi - 1] : null;
                          const pct =
                            prevGroup && prevGroup.totalRows > 0
                              ? ((group.totalRows - prevGroup.totalRows) /
                                  prevGroup.totalRows) *
                                100
                              : null;

                          return (
                            <div key={group.stage}>
                              {/* Stage label */}
                              <div className="flex items-center gap-2 mb-1">
                                <span className="text-xs font-semibold text-gray-400 uppercase tracking-wide">
                                  {group.stage}
                                </span>
                                <span className="text-xs font-mono font-medium text-gray-700">
                                  {group.totalRows.toLocaleString()}
                                </span>
                                {pct !== null && (
                                  <span
                                    className={`text-xs px-1 py-0.5 rounded-full font-medium ${
                                      Math.abs(pct) < 0.05
                                        ? "text-gray-400 bg-gray-100"
                                        : pct < 0
                                        ? "text-red-600 bg-red-50"
                                        : "text-green-600 bg-green-50"
                                    }`}
                                  >
                                    {Math.abs(pct) < 0.05
                                      ? "="
                                      : `${pct > 0 ? "+" : ""}${pct.toFixed(1)}%`}
                                  </span>
                                )}
                              </div>

                              {/* Files in this stage */}
                              <div className="space-y-1 ml-1">
                                {group.profiles.map((bp) => {
                                  const filename =
                                    bp.blob_name.split("/").pop() ?? bp.blob_name;
                                  const blobKey = `${resource.id}__${bp.blob_name}`;
                                  const isExpanded = expandedBlobs.has(blobKey);

                                  return (
                                    <div
                                      key={bp.blob_name}
                                      className="border border-gray-100 rounded-md overflow-hidden"
                                    >
                                      <button
                                        onClick={() => toggleBlob(blobKey)}
                                        className="w-full flex items-center gap-1.5 px-2 py-1.5 bg-gray-50 hover:bg-gray-100 transition-colors text-left"
                                      >
                                        <span className="text-gray-400 text-xs w-2.5 flex-shrink-0">
                                          {isExpanded ? "▼" : "▶"}
                                        </span>
                                        <span
                                          className="text-xs font-mono text-gray-700 flex-1 truncate min-w-0"
                                          title={bp.blob_name}
                                        >
                                          {filename}
                                        </span>
                                        {bp.error ? (
                                          <span className="text-xs text-red-400 flex-shrink-0">
                                            err
                                          </span>
                                        ) : (
                                          <span className="text-xs text-gray-500 flex-shrink-0 tabular-nums">
                                            {bp.row_count.toLocaleString()}
                                          </span>
                                        )}
                                      </button>

                                      {isExpanded && bp.error && (
                                        <div className="px-2 py-1.5 text-xs text-red-600 bg-red-50">
                                          {bp.error}
                                        </div>
                                      )}

                                      {isExpanded && !bp.error && (
                                        <div className="divide-y divide-gray-50">
                                          {bp.columns.map((col) => (
                                            <div key={col.name} className="px-2 py-1.5">
                                              <div className="flex items-center gap-2">
                                                <span className="text-xs font-mono text-gray-700 flex-1 truncate min-w-0">
                                                  {col.name}
                                                </span>
                                                <span className="text-xs text-gray-400 flex-shrink-0 tabular-nums">
                                                  {col.distinct_count.toLocaleString()} distinct
                                                </span>
                                              </div>
                                              {col.value_counts && (
                                                <div className="flex flex-wrap gap-1 mt-1">
                                                  {Object.entries(col.value_counts)
                                                    .sort(([, a], [, b]) => b - a)
                                                    .map(([val, count]) => (
                                                      <span
                                                        key={val}
                                                        className="inline-flex items-center gap-0.5 px-1 py-0.5 bg-gray-100 rounded text-xs"
                                                      >
                                                        <span className="font-mono text-gray-600">
                                                          {val}
                                                        </span>
                                                        <span className="text-gray-400">
                                                          ({count.toLocaleString()})
                                                        </span>
                                                      </span>
                                                    ))}
                                                </div>
                                              )}
                                            </div>
                                          ))}
                                        </div>
                                      )}
                                    </div>
                                  );
                                })}
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>
    </>
  );
}
