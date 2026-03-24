import { useState, useMemo } from "react";
import type { BlobInfo, Resource } from "../lib/api";

interface Props {
  resources: Resource[];
  blobs: BlobInfo[];
  filteredBlobsByResource: Record<string, BlobInfo[]>;
  autoDateByResource: Record<string, string>;
  dateOverrides: Record<string, string>;
  onDateOverridesChange: (overrides: Record<string, string>) => void;
  onCreate: (technicalName: string, businessName: string, extractPrefixes?: string[]) => Promise<void>;
  onUpdate: (id: string, technicalName: string, businessName: string, extractPrefixes?: string[]) => Promise<void>;
  onDelete: (id: string) => Promise<void>;
  onBack: () => void;
  onRefreshBlobs?: () => void;
}

interface EditState {
  id: string;
  technical_name: string;
  business_name: string;
  extract_prefixes_text: string;
}

const GAP_MINUTES = 15;

/**
 * Given a list of blobs, find the start of the most recent "batch":
 * sort by last_modified, walk backwards, and when the gap between two
 * consecutive files exceeds GAP_MINUTES, the later file is the batch boundary.
 * Returns the ISO string of that boundary, or "" if no gap found.
 */
function detectBatchStart(matchingBlobs: BlobInfo[]): string {
  const sorted = matchingBlobs
    .filter((b) => b.last_modified)
    .sort((a, b) => (a.last_modified ?? "").localeCompare(b.last_modified ?? ""));

  if (sorted.length < 2) return "";

  let lastGapIndex = -1;
  for (let i = 1; i < sorted.length; i++) {
    const prev = new Date(sorted[i - 1].last_modified).getTime();
    const curr = new Date(sorted[i].last_modified).getTime();
    if ((curr - prev) / (1000 * 60) > GAP_MINUTES) {
      lastGapIndex = i;
    }
  }

  return lastGapIndex >= 0 ? sorted[lastGapIndex].last_modified : "";
}

interface ValidationResult {
  valid: boolean;
  issues: string[];
}

function validateBatch(filteredBlobs: BlobInfo[]): ValidationResult {
  const stageCounts: Record<string, number> = {};
  for (const b of filteredBlobs) {
    const stage = b.detected_stage ?? "unknown";
    stageCounts[stage] = (stageCounts[stage] ?? 0) + 1;
  }

  const issues: string[] = [];

  if (!stageCounts["extract"] || stageCounts["extract"] < 1) {
    issues.push("missing extract");
  }
  if (!stageCounts["transform"] || stageCounts["transform"] < 1) {
    issues.push("missing transform");
  }

  for (const [stage, count] of Object.entries(stageCounts)) {
    if (stage === "extract" || stage === "transform") continue;
    if (count !== 1) {
      issues.push(`${stage}: ${count} (expected 1)`);
    }
  }

  return { valid: issues.length === 0, issues };
}

export default function ResourceManager({ resources, blobs, filteredBlobsByResource, autoDateByResource, dateOverrides, onDateOverridesChange, onCreate, onUpdate, onDelete, onBack, onRefreshBlobs }: Props) {
  const [editState, setEditState] = useState<EditState | null>(null);
  const [saving, setSaving] = useState(false);
  const [deletingId, setDeletingId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const [showNew, setShowNew] = useState(false);
  const [newTechnical, setNewTechnical] = useState("");
  const [newBusiness, setNewBusiness] = useState("");
  const [newExtractPrefixesText, setNewExtractPrefixesText] = useState("");

  // Matching blobs per resource (unfiltered — used for date dropdown options)
  const matchingBlobsByResource = useMemo(() => {
    const result: Record<string, BlobInfo[]> = {};
    for (const r of resources) {
      result[r.id] = blobs.filter((b) => b.name.startsWith(r.technical_name));
    }
    return result;
  }, [resources, blobs]);

  // Available dates per resource (sorted most recent first)
  const datesByResource = useMemo(() => {
    const result: Record<string, { iso: string; label: string }[]> = {};
    for (const r of resources) {
      const matching = matchingBlobsByResource[r.id] ?? [];
      const seen = new Set<string>();
      const dates: { iso: string; label: string }[] = [];
      for (const b of matching) {
        const iso = b.last_modified ?? "";
        if (!iso || seen.has(iso)) continue;
        seen.add(iso);
        dates.push({ iso, label: iso.slice(0, 16).replace("T", " ") });
      }
      dates.sort((a, b) => b.iso.localeCompare(a.iso));
      result[r.id] = dates;
    }
    return result;
  }, [resources, matchingBlobsByResource]);

  // Effective filter: auto unless manually overridden
  const effectiveFilter = (resourceId: string): string => {
    if (resourceId in dateOverrides) return dateOverrides[resourceId];
    return autoDateByResource[resourceId] ?? "";
  };

  // Validation per resource
  const validationByResource = useMemo(() => {
    const result: Record<string, ValidationResult> = {};
    for (const r of resources) {
      result[r.id] = validateBatch(filteredBlobsByResource[r.id] ?? []);
    }
    return result;
  }, [resources, filteredBlobsByResource]);

  const startEdit = (r: Resource) => {
    setEditState({
      id: r.id,
      technical_name: r.technical_name,
      business_name: r.business_name,
      extract_prefixes_text: (r.extract_prefixes ?? []).join("\n"),
    });
    setError(null);
  };

  const cancelEdit = () => { setEditState(null); setError(null); };

  const saveEdit = async () => {
    if (!editState) return;
    setSaving(true);
    setError(null);
    try {
      const extractPrefixes = editState.extract_prefixes_text
        .split("\n").map((s) => s.trim()).filter(Boolean);
      await onUpdate(editState.id, editState.technical_name.trim(), editState.business_name.trim(), extractPrefixes);
      setEditState(null);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Save failed");
    } finally {
      setSaving(false);
    }
  };

  const handleDelete = async (id: string) => {
    setDeletingId(id);
    setError(null);
    try {
      await onDelete(id);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Delete failed");
    } finally {
      setDeletingId(null);
    }
  };

  const handleCreate = async () => {
    if (!newTechnical.trim() || !newBusiness.trim()) return;
    setSaving(true);
    setError(null);
    try {
      const extractPrefixes = newExtractPrefixesText
        .split("\n").map((s) => s.trim()).filter(Boolean);
      await onCreate(newTechnical.trim(), newBusiness.trim(), extractPrefixes.length > 0 ? extractPrefixes : undefined);
      setNewTechnical("");
      setNewBusiness("");
      setNewExtractPrefixesText("");
      setShowNew(false);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Create failed");
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Top bar */}
      <div className="bg-white border-b border-gray-200 px-6 py-3 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <button
            onClick={onBack}
            className="text-sm text-gray-400 hover:text-gray-700 transition-colors"
          >
            ←
          </button>
          <span className="text-gray-200">|</span>
          <h2 className="text-sm font-semibold text-gray-800">Resources</h2>
          <span className="text-xs text-gray-400 bg-gray-100 px-2 py-0.5 rounded-full">
            {resources.length} resource{resources.length !== 1 ? "s" : ""}
          </span>
        </div>
        <div className="flex gap-2">
          {onRefreshBlobs && (
            <button
              onClick={onRefreshBlobs}
              className="text-sm bg-gray-600 hover:bg-gray-700 text-white px-3 py-1.5 rounded-lg transition-colors"
            >
              Refresh blobs
            </button>
          )}
          <button
            onClick={() => { setShowNew(true); setError(null); }}
            className="text-sm bg-teal-600 hover:bg-teal-700 text-white px-3 py-1.5 rounded-lg transition-colors"
          >
            + New resource
          </button>
        </div>
      </div>

      <div className="max-w-3xl mx-auto px-6 py-6">
        {error && (
          <div className="mb-4 bg-red-50 border border-red-200 rounded-lg px-4 py-3 text-sm text-red-700">
            {error}
          </div>
        )}

        <div className="bg-white border border-gray-200 rounded-xl overflow-hidden">
          {/* Table header */}
          <div className="grid grid-cols-[1fr_1fr_auto] gap-4 px-4 py-2.5 bg-gray-50 border-b border-gray-200 text-xs font-medium text-gray-500">
            <span>Business name</span>
            <span>Technical name (prefix)</span>
            <span>Actions</span>
          </div>

          {/* New resource row */}
          {showNew && (
            <div className="px-4 py-3 border-b border-teal-100 bg-teal-50">
              <div className="grid grid-cols-[1fr_1fr_auto] gap-4 items-start">
                <input
                  type="text"
                  placeholder="Business name"
                  value={newBusiness}
                  onChange={(e) => setNewBusiness(e.target.value)}
                  autoFocus
                  className="border border-teal-300 rounded-lg px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-teal-400"
                />
                <input
                  type="text"
                  placeholder="Technical name (prefix)"
                  value={newTechnical}
                  onChange={(e) => setNewTechnical(e.target.value)}
                  className="border border-teal-300 rounded-lg px-2 py-1.5 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-teal-400"
                />
                <div className="flex gap-2">
                  <button
                    onClick={handleCreate}
                    disabled={saving || !newBusiness.trim() || !newTechnical.trim()}
                    className="text-sm px-3 py-1.5 bg-teal-600 hover:bg-teal-700 disabled:bg-teal-300 text-white rounded-lg transition-colors"
                  >
                    {saving ? "Saving…" : "Save"}
                  </button>
                  <button
                    onClick={() => { setShowNew(false); setNewBusiness(""); setNewTechnical(""); setNewExtractPrefixesText(""); }}
                    className="text-sm px-3 py-1.5 text-gray-500 hover:text-gray-700 border border-gray-200 rounded-lg transition-colors"
                  >
                    Cancel
                  </button>
                </div>
              </div>
              <div className="mt-2">
                <label className="block text-xs text-teal-700 font-medium mb-1">
                  Extract-only prefixes <span className="font-normal text-teal-500">(optional — one per line)</span>
                </label>
                <textarea
                  rows={2}
                  placeholder={"e.g.\nother/exports/\nraw/dump/"}
                  value={newExtractPrefixesText}
                  onChange={(e) => setNewExtractPrefixesText(e.target.value)}
                  className="w-full border border-teal-300 rounded-lg px-2 py-1.5 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-teal-400 resize-none"
                />
                <p className="text-xs text-teal-500 mt-0.5">Additional Azure prefixes for this resource — only extract-stage files are kept.</p>
              </div>
            </div>
          )}

          {/* Resource rows */}
          {resources.length === 0 && !showNew && (
            <div className="px-4 py-10 text-center text-sm text-gray-400">
              No resources yet. Assign one from the file browser or create one here.
            </div>
          )}

          {resources.map((r) => {
            const isEditing = editState?.id === r.id;
            const isDeleting = deletingId === r.id;
            const dates = datesByResource[r.id] ?? [];
            const filteredBlobs = filteredBlobsByResource[r.id] ?? [];
            const autoDate = autoDateByResource[r.id] ?? "";
            const isManual = r.id in dateOverrides;
            const currentFilter = effectiveFilter(r.id);
            const validation = validationByResource[r.id];

            return (
              <div key={r.id} className={`border-b border-gray-100 last:border-b-0 ${isEditing ? "bg-blue-50" : ""}`}>
                <div className="grid grid-cols-[1fr_1fr_auto] gap-4 px-4 py-3 items-center">
                  {isEditing ? (
                    <>
                      <input
                        type="text"
                        value={editState.business_name}
                        onChange={(e) => setEditState({ ...editState, business_name: e.target.value })}
                        autoFocus
                        className="border border-blue-300 rounded-lg px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-400"
                      />
                      <input
                        type="text"
                        value={editState.technical_name}
                        onChange={(e) => setEditState({ ...editState, technical_name: e.target.value })}
                        className="border border-blue-300 rounded-lg px-2 py-1.5 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-blue-400"
                      />
                      <div className="flex gap-2">
                        <button
                          onClick={saveEdit}
                          disabled={saving}
                          className="text-sm px-3 py-1.5 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-300 text-white rounded-lg transition-colors"
                        >
                          {saving ? "Saving…" : "Save"}
                        </button>
                        <button
                          onClick={cancelEdit}
                          className="text-sm px-3 py-1.5 text-gray-500 hover:text-gray-700 border border-gray-200 rounded-lg transition-colors"
                        >
                          Cancel
                        </button>
                      </div>
                    </>
                  ) : (
                    <>
                      <span className="text-sm font-medium text-gray-800">{r.business_name}</span>
                      <span className="text-sm font-mono text-gray-500 truncate" title={r.technical_name}>
                        {r.technical_name}
                      </span>
                      <div className="flex gap-2">
                        <button
                          onClick={() => startEdit(r)}
                          className="text-xs text-gray-500 hover:text-blue-600 px-2 py-1 rounded-md border border-gray-200 hover:border-blue-300 transition-colors"
                        >
                          Edit
                        </button>
                        <button
                          onClick={() => handleDelete(r.id)}
                          disabled={isDeleting}
                          className="text-xs text-gray-500 hover:text-red-600 px-2 py-1 rounded-md border border-gray-200 hover:border-red-300 transition-colors disabled:opacity-50"
                        >
                          {isDeleting ? "…" : "Delete"}
                        </button>
                      </div>
                    </>
                  )}
                </div>

                {/* Extract-only prefixes — edit mode */}
                {isEditing && (
                  <div className="px-4 pb-3">
                    <label className="block text-xs text-blue-700 font-medium mb-1">
                      Extract-only prefixes <span className="font-normal text-blue-400">(optional — one per line)</span>
                    </label>
                    <textarea
                      rows={2}
                      placeholder={"e.g.\nother/exports/\nraw/dump/"}
                      value={editState.extract_prefixes_text}
                      onChange={(e) => setEditState({ ...editState, extract_prefixes_text: e.target.value })}
                      className="w-full border border-blue-300 rounded-lg px-2 py-1.5 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-blue-400 resize-none"
                    />
                    <p className="text-xs text-blue-400 mt-0.5">Additional Azure prefixes for this resource — only extract-stage files are kept.</p>
                  </div>
                )}

                {/* Extract-only prefixes — view mode (when configured) */}
                {!isEditing && (r.extract_prefixes ?? []).length > 0 && (
                  <div className="px-4 pb-1 flex flex-wrap gap-1">
                    {(r.extract_prefixes ?? []).map((p) => (
                      <span key={p} className="text-xs font-mono bg-indigo-50 text-indigo-600 border border-indigo-200 px-2 py-0.5 rounded">
                        {p}
                      </span>
                    ))}
                  </div>
                )}

                {/* Date filter */}
                {!isEditing && dates.length > 0 && (
                  <div className="px-4 pb-2 flex items-center gap-3 flex-wrap">
                    <select
                      value={isManual ? currentFilter : "__auto__"}
                      onChange={(e) => {
                        const val = e.target.value;
                        if (val === "__auto__") {
                          const next = { ...dateOverrides };
                          delete next[r.id];
                          onDateOverridesChange(next);
                        } else {
                          onDateOverridesChange({ ...dateOverrides, [r.id]: val });
                        }
                      }}
                      className="border border-gray-200 rounded-lg px-2 py-1 text-xs focus:outline-none focus:ring-2 focus:ring-blue-400"
                    >
                      <option value="__auto__">
                        Auto{autoDate ? ` (${autoDate.slice(0, 16).replace("T", " ")})` : ""}
                      </option>
                      <option value="">All dates</option>
                      {dates.map((d) => (
                        <option key={d.iso} value={d.iso}>{d.label}</option>
                      ))}
                    </select>
                    <span className="text-xs text-gray-400">
                      {filteredBlobs.length} file{filteredBlobs.length !== 1 ? "s" : ""}
                    </span>
                    {/* Validation badge */}
                    {validation.valid ? (
                      <span className="text-xs px-2 py-0.5 rounded-full bg-green-100 text-green-700 font-medium">
                        OK
                      </span>
                    ) : (
                      <span
                        className="text-xs px-2 py-0.5 rounded-full bg-red-100 text-red-600 font-medium cursor-help"
                        title={validation.issues.join(", ")}
                      >
                        {validation.issues.length} issue{validation.issues.length !== 1 ? "s" : ""}
                      </span>
                    )}
                  </div>
                )}

                {/* Stage counts */}
                {!isEditing && filteredBlobs.length > 0 && (
                  <div className="px-4 pb-3">
                    <div className="flex flex-wrap gap-2">
                      {Object.entries(
                        filteredBlobs.reduce<Record<string, number>>((acc, b) => {
                          const stage = b.detected_stage ?? "unknown";
                          acc[stage] = (acc[stage] ?? 0) + 1;
                          return acc;
                        }, {})
                      )
                        .sort(([a], [b]) => a.localeCompare(b))
                        .map(([stage, count]) => {
                          const isOk =
                            (stage === "extract" || stage === "transform")
                              ? count >= 1
                              : count === 1;
                          return (
                            <span
                              key={stage}
                              className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs ${
                                isOk
                                  ? "bg-gray-100 text-gray-600"
                                  : "bg-red-50 text-red-600"
                              }`}
                            >
                              <span className="font-medium">{count}</span> {stage}
                            </span>
                          );
                        })}
                    </div>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
