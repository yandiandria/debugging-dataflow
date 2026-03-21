import { useState } from "react";
import type { Resource } from "../lib/api";

interface Props {
  resources: Resource[];
  onCreate: (technicalName: string, businessName: string) => Promise<void>;
  onUpdate: (id: string, technicalName: string, businessName: string) => Promise<void>;
  onDelete: (id: string) => Promise<void>;
  onBack: () => void;
}

interface EditState {
  id: string;
  technical_name: string;
  business_name: string;
}

export default function ResourceManager({ resources, onCreate, onUpdate, onDelete, onBack }: Props) {
  const [editState, setEditState] = useState<EditState | null>(null);
  const [saving, setSaving] = useState(false);
  const [deletingId, setDeletingId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  // New resource form state ("new" is a sentinel id)
  const [showNew, setShowNew] = useState(false);
  const [newTechnical, setNewTechnical] = useState("");
  const [newBusiness, setNewBusiness] = useState("");

  const startEdit = (r: Resource) => {
    setEditState({ id: r.id, technical_name: r.technical_name, business_name: r.business_name });
    setError(null);
  };

  const cancelEdit = () => { setEditState(null); setError(null); };

  const saveEdit = async () => {
    if (!editState) return;
    setSaving(true);
    setError(null);
    try {
      await onUpdate(editState.id, editState.technical_name.trim(), editState.business_name.trim());
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
      await onCreate(newTechnical.trim(), newBusiness.trim());
      setNewTechnical("");
      setNewBusiness("");
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
        <button
          onClick={() => { setShowNew(true); setError(null); }}
          className="text-sm bg-teal-600 hover:bg-teal-700 text-white px-3 py-1.5 rounded-lg transition-colors"
        >
          + New resource
        </button>
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
            <div className="grid grid-cols-[1fr_1fr_auto] gap-4 px-4 py-3 border-b border-teal-100 bg-teal-50 items-center">
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
                  onClick={() => { setShowNew(false); setNewBusiness(""); setNewTechnical(""); }}
                  className="text-sm px-3 py-1.5 text-gray-500 hover:text-gray-700 border border-gray-200 rounded-lg transition-colors"
                >
                  Cancel
                </button>
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
            return (
              <div
                key={r.id}
                className={`grid grid-cols-[1fr_1fr_auto] gap-4 px-4 py-3 border-b border-gray-100 last:border-b-0 items-center ${isEditing ? "bg-blue-50" : ""}`}
              >
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
            );
          })}
        </div>
      </div>
    </div>
  );
}
