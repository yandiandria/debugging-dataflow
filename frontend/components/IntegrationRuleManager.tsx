import { useState, useEffect } from "react";
import type { IntegrationRule, Resource } from "../lib/api";
import { getRules, createRule, updateRule, deleteRule } from "../lib/api";

interface Props {
  resources: Resource[];
  onBack: () => void;
}

export default function IntegrationRuleManager({ resources, onBack }: Props) {
  const [rules, setRules] = useState<IntegrationRule[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [deletingId, setDeletingId] = useState<string | null>(null);

  const [showNew, setShowNew] = useState(false);
  const [newDescription, setNewDescription] = useState("");
  const [newResourceIds, setNewResourceIds] = useState<string[]>([]);

  const [editingId, setEditingId] = useState<string | null>(null);
  const [editDescription, setEditDescription] = useState("");
  const [editResourceIds, setEditResourceIds] = useState<string[]>([]);

  useEffect(() => {
    getRules().then(setRules).catch(() => {});
  }, []);

  const refresh = async () => {
    const r = await getRules();
    setRules(r);
  };

  const handleCreate = async () => {
    if (!newDescription.trim()) return;
    setSaving(true);
    setError(null);
    try {
      await createRule(newDescription.trim(), newResourceIds);
      await refresh();
      setNewDescription("");
      setNewResourceIds([]);
      setShowNew(false);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Create failed");
    } finally {
      setSaving(false);
    }
  };

  const handleSaveEdit = async () => {
    if (!editingId) return;
    setSaving(true);
    setError(null);
    try {
      await updateRule(editingId, { description: editDescription.trim(), resource_ids: editResourceIds });
      await refresh();
      setEditingId(null);
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
      await deleteRule(id);
      await refresh();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Delete failed");
    } finally {
      setDeletingId(null);
    }
  };

  const toggleResourceId = (ids: string[], setIds: (v: string[]) => void, resourceId: string) => {
    if (ids.includes(resourceId)) {
      setIds(ids.filter((id) => id !== resourceId));
    } else {
      setIds([...ids, resourceId]);
    }
  };

  const resourceName = (id: string) => {
    const r = resources.find((res) => res.id === id);
    return r ? r.business_name : id.slice(0, 8);
  };

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="bg-white border-b border-gray-200 px-6 py-3 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <button onClick={onBack} className="text-sm text-gray-400 hover:text-gray-700 transition-colors">
            &larr;
          </button>
          <span className="text-gray-200">|</span>
          <h2 className="text-sm font-semibold text-gray-800">Integration Rules</h2>
          <span className="text-xs text-gray-400 bg-gray-100 px-2 py-0.5 rounded-full">
            {rules.length} rule{rules.length !== 1 ? "s" : ""}
          </span>
        </div>
        <button
          onClick={() => { setShowNew(true); setError(null); }}
          className="text-sm bg-amber-600 hover:bg-amber-700 text-white px-3 py-1.5 rounded-lg transition-colors"
        >
          + New rule
        </button>
      </div>

      <div className="max-w-3xl mx-auto px-6 py-6">
        {error && (
          <div className="mb-4 bg-red-50 border border-red-200 rounded-lg px-4 py-3 text-sm text-red-700">
            {error}
          </div>
        )}

        <div className="bg-white border border-gray-200 rounded-xl overflow-hidden">
          <div className="px-4 py-2.5 bg-gray-50 border-b border-gray-200 text-xs font-medium text-gray-500">
            Rules are linked to resources and shown on the Resource Dashboard as a checklist.
          </div>

          {showNew && (
            <div className="px-4 py-3 border-b border-amber-100 bg-amber-50 space-y-3">
              <textarea
                placeholder="Rule description (e.g. 'If contract=CDD, set end_date to renewal_date')"
                value={newDescription}
                onChange={(e) => setNewDescription(e.target.value)}
                autoFocus
                rows={2}
                className="w-full border border-amber-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-amber-400 resize-none"
              />
              <div>
                <label className="block text-xs text-gray-500 mb-1">Link to resources:</label>
                <div className="flex flex-wrap gap-2">
                  {resources.map((r) => (
                    <button
                      key={r.id}
                      onClick={() => toggleResourceId(newResourceIds, setNewResourceIds, r.id)}
                      className={`text-xs px-2 py-1 rounded-full border transition-colors ${
                        newResourceIds.includes(r.id)
                          ? "bg-amber-200 border-amber-400 text-amber-800"
                          : "bg-white border-gray-200 text-gray-500 hover:border-amber-300"
                      }`}
                    >
                      {r.business_name}
                    </button>
                  ))}
                  {resources.length === 0 && <span className="text-xs text-gray-400">No resources yet</span>}
                </div>
              </div>
              <div className="flex gap-2">
                <button
                  onClick={handleCreate}
                  disabled={saving || !newDescription.trim()}
                  className="text-sm px-3 py-1.5 bg-amber-600 hover:bg-amber-700 disabled:bg-amber-300 text-white rounded-lg"
                >
                  {saving ? "Saving..." : "Save"}
                </button>
                <button
                  onClick={() => { setShowNew(false); setNewDescription(""); setNewResourceIds([]); }}
                  className="text-sm px-3 py-1.5 text-gray-500 hover:text-gray-700 border border-gray-200 rounded-lg"
                >
                  Cancel
                </button>
              </div>
            </div>
          )}

          {rules.length === 0 && !showNew && (
            <div className="px-4 py-10 text-center text-sm text-gray-400">
              No integration rules yet. Add one to get started.
            </div>
          )}

          {rules.map((rule) => {
            const isEditing = editingId === rule.id;
            const isDeleting = deletingId === rule.id;

            return (
              <div key={rule.id} className={`px-4 py-3 border-b border-gray-100 last:border-b-0 ${isEditing ? "bg-blue-50" : ""}`}>
                {isEditing ? (
                  <div className="space-y-3">
                    <textarea
                      value={editDescription}
                      onChange={(e) => setEditDescription(e.target.value)}
                      rows={2}
                      autoFocus
                      className="w-full border border-blue-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-400 resize-none"
                    />
                    <div>
                      <label className="block text-xs text-gray-500 mb-1">Link to resources:</label>
                      <div className="flex flex-wrap gap-2">
                        {resources.map((r) => (
                          <button
                            key={r.id}
                            onClick={() => toggleResourceId(editResourceIds, setEditResourceIds, r.id)}
                            className={`text-xs px-2 py-1 rounded-full border transition-colors ${
                              editResourceIds.includes(r.id)
                                ? "bg-blue-200 border-blue-400 text-blue-800"
                                : "bg-white border-gray-200 text-gray-500 hover:border-blue-300"
                            }`}
                          >
                            {r.business_name}
                          </button>
                        ))}
                      </div>
                    </div>
                    <div className="flex gap-2">
                      <button onClick={handleSaveEdit} disabled={saving} className="text-sm px-3 py-1.5 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-300 text-white rounded-lg">
                        {saving ? "Saving..." : "Save"}
                      </button>
                      <button onClick={() => setEditingId(null)} className="text-sm px-3 py-1.5 text-gray-500 hover:text-gray-700 border border-gray-200 rounded-lg">
                        Cancel
                      </button>
                    </div>
                  </div>
                ) : (
                  <div className="flex items-start justify-between gap-4">
                    <div className="flex-1">
                      <p className="text-sm text-gray-800">{rule.description}</p>
                      {rule.resource_ids.length > 0 && (
                        <div className="flex flex-wrap gap-1 mt-1">
                          {rule.resource_ids.map((rid) => (
                            <span key={rid} className="text-xs bg-gray-100 text-gray-600 px-2 py-0.5 rounded-full">
                              {resourceName(rid)}
                            </span>
                          ))}
                        </div>
                      )}
                    </div>
                    <div className="flex gap-2 flex-shrink-0">
                      <button
                        onClick={() => {
                          setEditingId(rule.id);
                          setEditDescription(rule.description);
                          setEditResourceIds(rule.resource_ids);
                        }}
                        className="text-xs text-gray-500 hover:text-blue-600 px-2 py-1 rounded-md border border-gray-200 hover:border-blue-300 transition-colors"
                      >
                        Edit
                      </button>
                      <button
                        onClick={() => handleDelete(rule.id)}
                        disabled={isDeleting}
                        className="text-xs text-gray-500 hover:text-red-600 px-2 py-1 rounded-md border border-gray-200 hover:border-red-300 transition-colors disabled:opacity-50"
                      >
                        {isDeleting ? "..." : "Delete"}
                      </button>
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
