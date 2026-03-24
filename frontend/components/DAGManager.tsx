import { useState, useEffect } from "react";
import type { DAG, DAGConfig } from "../lib/api";
import { getDags, createDag, updateDag, deleteDag, getDagConfig, updateDagConfig, listAirflowDags } from "../lib/api";

function DagIdInput({
  value,
  onChange,
  suggestions,
  className,
  placeholder = "airflow_dag_id",
}: {
  value: string;
  onChange: (v: string) => void;
  suggestions: string[];
  className?: string;
  placeholder?: string;
}) {
  const [open, setOpen] = useState(false);

  const filtered = suggestions.filter(
    (id) => id.toLowerCase().includes(value.toLowerCase()) && id !== value
  );

  return (
    <div className="relative">
      <input
        type="text"
        value={value}
        placeholder={placeholder}
        onChange={(e) => { onChange(e.target.value); setOpen(true); }}
        onFocus={() => setOpen(true)}
        onBlur={() => setTimeout(() => setOpen(false), 150)}
        className={className}
      />
      {open && filtered.length > 0 && (
        <ul className="absolute z-20 left-0 right-0 mt-1 bg-white border border-gray-200 rounded-lg shadow-lg max-h-48 overflow-y-auto">
          {filtered.map((id) => (
            <li
              key={id}
              onMouseDown={() => { onChange(id); setOpen(false); }}
              className="px-3 py-1.5 text-sm font-mono cursor-pointer hover:bg-indigo-50 hover:text-indigo-700 truncate"
              title={id}
            >
              {id}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

interface Props {
  onBack: () => void;
}

interface EditState {
  id: string;
  dag_id: string;
  display_name: string;
}

export default function DAGManager({ onBack }: Props) {
  const [dags, setDags] = useState<DAG[]>([]);
  const [config, setConfig] = useState<DAGConfig>({ container_name: "", environments: ["integration", "snap", "recette", "prod"] });
  const [editState, setEditState] = useState<EditState | null>(null);
  const [saving, setSaving] = useState(false);
  const [deletingId, setDeletingId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [showNew, setShowNew] = useState(false);
  const [newDagId, setNewDagId] = useState("");
  const [newDisplayName, setNewDisplayName] = useState("");
  const [editingConfig, setEditingConfig] = useState(false);
  const [configDraft, setConfigDraft] = useState("");
  const [configEnvsDraft, setConfigEnvsDraft] = useState("");
  const [airflowDags, setAirflowDags] = useState<string[]>([]);

  useEffect(() => {
    getDags().then(setDags).catch(() => {});
    getDagConfig().then((c) => setConfig(c)).catch(() => {});
    listAirflowDags().then(setAirflowDags).catch(() => {});
  }, []);

  const refresh = async () => {
    const d = await getDags();
    setDags(d);
  };

  const handleCreate = async () => {
    if (!newDagId.trim() || !newDisplayName.trim()) return;
    setSaving(true);
    setError(null);
    try {
      const created = await createDag(newDagId.trim(), newDisplayName.trim());
      setDags((prev) => [...prev, created]);
      setNewDagId("");
      setNewDisplayName("");
      setShowNew(false);
      refresh().catch(() => {});
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Create failed");
    } finally {
      setSaving(false);
    }
  };

  const saveEdit = async () => {
    if (!editState) return;
    setSaving(true);
    setError(null);
    try {
      const updated = await updateDag(editState.id, editState.dag_id.trim(), editState.display_name.trim());
      setDags((prev) => prev.map((d) => (d.id === updated.id ? updated : d)));
      setEditState(null);
      refresh().catch(() => {});
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
      await deleteDag(id);
      setDags((prev) => prev.filter((d) => d.id !== id));
      refresh().catch(() => {});
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Delete failed");
    } finally {
      setDeletingId(null);
    }
  };

  const saveConfig = async () => {
    try {
      const envs = configEnvsDraft.split(",").map((s) => s.trim()).filter(Boolean);
      const updated = await updateDagConfig({ container_name: configDraft, environments: envs.length > 0 ? envs : ["integration", "snap", "recette", "prod"] });
      setConfig(updated);
      setEditingConfig(false);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Config save failed");
    }
  };

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Top bar */}
      <div className="bg-white border-b border-gray-200 px-6 py-3 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <button onClick={onBack} className="text-sm text-gray-400 hover:text-gray-700 transition-colors">
            &larr;
          </button>
          <span className="text-gray-200">|</span>
          <h2 className="text-sm font-semibold text-gray-800">DAG Manager</h2>
          <span className="text-xs text-gray-400 bg-gray-100 px-2 py-0.5 rounded-full">
            {dags.length} DAG{dags.length !== 1 ? "s" : ""}
          </span>
        </div>
        <button
          onClick={() => { setShowNew(true); setError(null); }}
          className="text-sm bg-indigo-600 hover:bg-indigo-700 text-white px-3 py-1.5 rounded-lg transition-colors"
        >
          + New DAG
        </button>
      </div>

      <div className="max-w-3xl mx-auto px-6 py-6 space-y-6">
        {/* Docker config */}
        <div className="bg-white border border-gray-200 rounded-xl p-4">
          <div className="flex items-center justify-between mb-2">
            <h3 className="text-sm font-semibold text-gray-700">Docker Configuration</h3>
            {!editingConfig && (
              <button
                onClick={() => { setEditingConfig(true); setConfigDraft(config.container_name); setConfigEnvsDraft(config.environments.join(", ")); }}
                className="text-xs text-indigo-600 hover:text-indigo-800"
              >
                Edit
              </button>
            )}
          </div>
          {editingConfig ? (
            <div className="space-y-3">
              <div>
                <label className="block text-xs text-gray-500 mb-1">Docker container name</label>
                <input
                  type="text"
                  value={configDraft}
                  onChange={(e) => setConfigDraft(e.target.value)}
                  placeholder="e.g. airflow-worker"
                  className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-indigo-400"
                />
              </div>
              <div>
                <label className="block text-xs text-gray-500 mb-1">Environments (comma-separated)</label>
                <input
                  type="text"
                  value={configEnvsDraft}
                  onChange={(e) => setConfigEnvsDraft(e.target.value)}
                  placeholder="dev, staging, prod"
                  className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-400"
                />
              </div>
              <div className="flex gap-2">
                <button onClick={saveConfig} className="text-sm px-3 py-1.5 bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg">Save</button>
                <button onClick={() => setEditingConfig(false)} className="text-sm px-3 py-1.5 text-gray-500 hover:text-gray-700 border border-gray-200 rounded-lg">Cancel</button>
              </div>
            </div>
          ) : (
            <div className="text-sm space-y-1">
              <p className="text-gray-600">
                Container: <span className="font-mono text-gray-800">{config.container_name || "(not set)"}</span>
              </p>
              <p className="text-gray-600">
                Environments: {config.environments.map((e) => (
                  <span key={e} className="inline-block bg-gray-100 text-gray-700 text-xs px-2 py-0.5 rounded-full mr-1">{e}</span>
                ))}
              </p>
              <p className="text-xs text-gray-400 mt-2">
                Trigger: <code className="bg-gray-100 px-1 rounded">docker exec {config.container_name || "<container>"} airflow dags trigger {"<dag_id>"} --conf {`'{"padoa_env": "<env>"}'`}</code>
              </p>
            </div>
          )}
        </div>

        {error && (
          <div className="bg-red-50 border border-red-200 rounded-lg px-4 py-3 text-sm text-red-700">
            {error}
          </div>
        )}

        {/* DAG list */}
        <div className="bg-white border border-gray-200 rounded-xl overflow-hidden">
          <div className="grid grid-cols-[1fr_1fr_auto] gap-4 px-4 py-2.5 bg-gray-50 border-b border-gray-200 text-xs font-medium text-gray-500">
            <span>Display name</span>
            <span>Airflow DAG ID</span>
            <span>Actions</span>
          </div>

          {showNew && (
            <div className="grid grid-cols-[1fr_1fr_auto] gap-4 px-4 py-3 border-b border-indigo-100 bg-indigo-50 items-center">
              <input
                type="text"
                placeholder="Display name"
                value={newDisplayName}
                onChange={(e) => setNewDisplayName(e.target.value)}
                autoFocus
                className="border border-indigo-300 rounded-lg px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-400"
              />
              <DagIdInput
                value={newDagId}
                onChange={setNewDagId}
                suggestions={airflowDags}
                className="border border-indigo-300 rounded-lg px-2 py-1.5 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-indigo-400 w-full"
              />
              <div className="flex gap-2">
                <button
                  onClick={handleCreate}
                  disabled={saving || !newDagId.trim() || !newDisplayName.trim()}
                  className="text-sm px-3 py-1.5 bg-indigo-600 hover:bg-indigo-700 disabled:bg-indigo-300 text-white rounded-lg transition-colors"
                >
                  {saving ? "Saving..." : "Save"}
                </button>
                <button
                  onClick={() => { setShowNew(false); setNewDagId(""); setNewDisplayName(""); }}
                  className="text-sm px-3 py-1.5 text-gray-500 hover:text-gray-700 border border-gray-200 rounded-lg transition-colors"
                >
                  Cancel
                </button>
              </div>
            </div>
          )}

          {dags.length === 0 && !showNew && (
            <div className="px-4 py-10 text-center text-sm text-gray-400">
              No DAGs yet. Add one to get started.
            </div>
          )}

          {dags.map((d) => {
            const isEditing = editState?.id === d.id;
            const isDeleting = deletingId === d.id;
            const cmd = config.container_name
              ? `docker exec ${config.container_name} airflow dags trigger ${d.dag_id} --conf '{"padoa_env": "<env>"}'`
              : null;

            return (
              <div key={d.id} className={`border-b border-gray-100 last:border-b-0 ${isEditing ? "bg-blue-50" : ""}`}>
                <div className="grid grid-cols-[1fr_1fr_auto] gap-4 px-4 py-3 items-center">
                {isEditing ? (
                  <>
                    <input
                      type="text"
                      value={editState.display_name}
                      onChange={(e) => setEditState({ ...editState, display_name: e.target.value })}
                      autoFocus
                      className="border border-blue-300 rounded-lg px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-400"
                    />
                    <DagIdInput
                      value={editState.dag_id}
                      onChange={(v) => setEditState({ ...editState, dag_id: v })}
                      suggestions={airflowDags}
                      className="border border-blue-300 rounded-lg px-2 py-1.5 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-blue-400 w-full"
                    />
                    <div className="flex gap-2">
                      <button onClick={saveEdit} disabled={saving} className="text-sm px-3 py-1.5 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-300 text-white rounded-lg">
                        {saving ? "Saving..." : "Save"}
                      </button>
                      <button onClick={() => setEditState(null)} className="text-sm px-3 py-1.5 text-gray-500 hover:text-gray-700 border border-gray-200 rounded-lg">
                        Cancel
                      </button>
                    </div>
                  </>
                ) : (
                  <>
                    <span className="text-sm font-medium text-gray-800">{d.display_name}</span>
                    <span className="text-sm font-mono text-gray-500 truncate" title={d.dag_id}>{d.dag_id}</span>
                    <div className="flex gap-2">
                      <button
                        onClick={() => setEditState({ id: d.id, dag_id: d.dag_id, display_name: d.display_name })}
                        className="text-xs text-gray-500 hover:text-blue-600 px-2 py-1 rounded-md border border-gray-200 hover:border-blue-300 transition-colors"
                      >
                        Edit
                      </button>
                      <button
                        onClick={() => handleDelete(d.id)}
                        disabled={isDeleting}
                        className="text-xs text-gray-500 hover:text-red-600 px-2 py-1 rounded-md border border-gray-200 hover:border-red-300 transition-colors disabled:opacity-50"
                      >
                        {isDeleting ? "..." : "Delete"}
                      </button>
                    </div>
                  </>
                )}
                </div>
                {!isEditing && (
                  <div className="px-4 pb-2.5">
                    <code className={`text-xs font-mono ${cmd ? "text-gray-400" : "text-gray-300 italic"}`}>
                      {cmd ?? "docker exec <container> airflow dags trigger " + d.dag_id + " --conf '{\"padoa_env\": \"<env>\"}'"}
                    </code>
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
