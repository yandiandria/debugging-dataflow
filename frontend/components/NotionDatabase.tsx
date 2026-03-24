import { useState, useEffect } from "react";
import type { NotionConfig, NotionSchema, NotionRow } from "../lib/api";
import {
  getNotionConfig, saveNotionConfig,
  getNotionSchema, getNotionRows,
  createNotionRow, updateNotionRow, deleteNotionRow,
} from "../lib/api";

interface Props {
  onBack: () => void;
}

const EDITABLE_TYPES = new Set([
  "title", "rich_text", "number", "select", "multi_select",
  "date", "checkbox", "url", "email", "phone_number",
]);

function CellValue({ value, type }: { value: unknown; type: string }) {
  if (value === null || value === undefined || value === "") {
    return <span className="text-gray-400">—</span>;
  }
  if (type === "checkbox") {
    return <span className={value ? "text-green-600" : "text-gray-400"}>{value ? "✓" : "✗"}</span>;
  }
  if (type === "multi_select" && Array.isArray(value)) {
    return (
      <div className="flex flex-wrap gap-1">
        {(value as string[]).map((v) => (
          <span key={v} className="bg-gray-200 text-gray-700 text-xs px-1.5 py-0.5 rounded">{v}</span>
        ))}
      </div>
    );
  }
  if (type === "select") {
    return <span className="bg-indigo-100 text-indigo-700 text-xs px-1.5 py-0.5 rounded">{String(value)}</span>;
  }
  if (type === "url") {
    return (
      <a href={String(value)} target="_blank" rel="noopener noreferrer"
        className="text-blue-600 underline truncate max-w-[160px] block text-xs">
        {String(value)}
      </a>
    );
  }
  if (type === "date" || type === "created_time" || type === "last_edited_time") {
    try {
      return <span>{new Date(String(value)).toLocaleDateString()}</span>;
    } catch {
      return <span>{String(value)}</span>;
    }
  }
  return <span className="truncate max-w-[200px] block">{String(value)}</span>;
}

function CellInput({
  type, value, options, onChange,
}: {
  type: string;
  value: unknown;
  options?: string[];
  onChange: (v: unknown) => void;
}) {
  const cls = "border border-gray-300 rounded px-1.5 py-0.5 text-sm w-full focus:outline-none focus:ring-1 focus:ring-indigo-400";

  if (type === "checkbox") {
    return (
      <input type="checkbox" checked={Boolean(value)}
        onChange={(e) => onChange(e.target.checked)}
        className="w-4 h-4 accent-indigo-600" />
    );
  }
  if (type === "select") {
    return (
      <select value={String(value ?? "")} onChange={(e) => onChange(e.target.value || null)} className={cls}>
        <option value="">—</option>
        {options?.map((o) => <option key={o} value={o}>{o}</option>)}
      </select>
    );
  }
  if (type === "number") {
    return (
      <input type="number" value={value === null || value === undefined ? "" : String(value)}
        onChange={(e) => onChange(e.target.value === "" ? null : Number(e.target.value))}
        className={cls} />
    );
  }
  if (type === "date") {
    return (
      <input type="date" value={String(value ?? "")}
        onChange={(e) => onChange(e.target.value || null)}
        className={cls} />
    );
  }
  if (type === "multi_select") {
    const current = Array.isArray(value) ? (value as string[]).join(", ") : String(value ?? "");
    return (
      <input type="text" value={current} placeholder="val1, val2"
        onChange={(e) => onChange(e.target.value.split(",").map((v) => v.trim()).filter(Boolean))}
        className={cls} />
    );
  }
  return (
    <input type="text" value={String(value ?? "")}
      onChange={(e) => onChange(e.target.value)}
      className={cls} />
  );
}

export default function NotionDatabase({ onBack }: Props) {
  const [config, setConfig] = useState<NotionConfig>({ token: "", database_id: "" });
  const [showConfig, setShowConfig] = useState(false);
  const [configDraft, setConfigDraft] = useState<NotionConfig>({ token: "", database_id: "" });
  const [schema, setSchema] = useState<NotionSchema | null>(null);
  const [rows, setRows] = useState<NotionRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [editValues, setEditValues] = useState<Record<string, unknown>>({});
  const [showNewRow, setShowNewRow] = useState(false);
  const [newValues, setNewValues] = useState<Record<string, unknown>>({});
  const [saving, setSaving] = useState(false);
  const [deleteConfirmId, setDeleteConfirmId] = useState<string | null>(null);

  useEffect(() => {
    getNotionConfig().then((cfg) => {
      setConfig(cfg);
      setConfigDraft(cfg);
    }).catch(() => {});
  }, []);

  useEffect(() => {
    if (config.token && config.database_id) {
      loadData();
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [config]);

  const loadData = async () => {
    setLoading(true);
    setError(null);
    try {
      const [s, r] = await Promise.all([getNotionSchema(), getNotionRows()]);
      setSchema(s);
      setRows(r);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load Notion data");
    } finally {
      setLoading(false);
    }
  };

  const handleSaveConfig = async () => {
    setSaving(true);
    setError(null);
    try {
      const saved = await saveNotionConfig(configDraft);
      setConfig(saved);
      setShowConfig(false);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to save config");
    } finally {
      setSaving(false);
    }
  };

  const startEdit = (row: NotionRow) => {
    setEditingId(row.id);
    setEditValues({ ...row.properties });
    setShowNewRow(false);
  };

  const cancelEdit = () => { setEditingId(null); setEditValues({}); };

  const saveEdit = async () => {
    if (!editingId) return;
    setSaving(true);
    setError(null);
    try {
      const updated = await updateNotionRow(editingId, editValues);
      setRows((prev) => prev.map((r) => r.id === editingId ? updated : r));
      setEditingId(null);
      setEditValues({});
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to update row");
    } finally {
      setSaving(false);
    }
  };

  const handleDelete = async (pageId: string) => {
    setSaving(true);
    setError(null);
    try {
      await deleteNotionRow(pageId);
      setRows((prev) => prev.filter((r) => r.id !== pageId));
      setDeleteConfirmId(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to delete row");
    } finally {
      setSaving(false);
    }
  };

  const startNewRow = () => {
    if (!schema) return;
    const initial: Record<string, unknown> = {};
    for (const name of schema.property_order) {
      const { type } = schema.properties[name];
      initial[name] = type === "checkbox" ? false : type === "multi_select" ? [] : "";
    }
    setNewValues(initial);
    setShowNewRow(true);
    setEditingId(null);
  };

  const saveNewRow = async () => {
    setSaving(true);
    setError(null);
    try {
      const created = await createNotionRow(newValues);
      setRows((prev) => [...prev, created]);
      setShowNewRow(false);
      setNewValues({});
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to create row");
    } finally {
      setSaving(false);
    }
  };

  const configured = Boolean(config.token && config.database_id);
  const allCols = schema?.property_order ?? [];

  return (
    <div className="min-h-screen bg-gray-50 p-6">
      <div className="max-w-screen-xl mx-auto">

        {/* Header */}
        <div className="mb-4 flex items-center justify-between flex-wrap gap-2">
          <div className="flex items-center gap-3">
            <button onClick={onBack} className="text-sm text-gray-500 hover:text-gray-700">← Back</button>
            <h1 className="text-2xl font-bold text-gray-800">Injection Tracker</h1>
            <span className="text-xs text-gray-400 bg-gray-100 px-2 py-0.5 rounded">Notion</span>
          </div>
          <div className="flex items-center gap-2">
            {configured && (
              <>
                <button onClick={loadData} disabled={loading}
                  className="text-sm bg-gray-600 hover:bg-gray-700 disabled:bg-gray-400 text-white px-3 py-1.5 rounded-lg transition-colors">
                  {loading ? "Loading…" : "Refresh"}
                </button>
                <button onClick={startNewRow} disabled={!schema || loading}
                  className="text-sm bg-green-600 hover:bg-green-700 disabled:bg-gray-400 text-white px-3 py-1.5 rounded-lg transition-colors">
                  + Add row
                </button>
              </>
            )}
            <button onClick={() => setShowConfig((p) => !p)}
              className="text-sm bg-gray-200 hover:bg-gray-300 text-gray-700 px-3 py-1.5 rounded-lg transition-colors">
              ⚙ Settings
            </button>
          </div>
        </div>

        {/* Config panel */}
        {showConfig && (
          <div className="mb-4 bg-white border rounded-xl p-4 shadow-sm">
            <h2 className="font-semibold text-gray-700 mb-3">Notion Configuration</h2>
            <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
              <div>
                <label className="block text-xs text-gray-500 mb-1">Integration Token</label>
                <input type="password" value={configDraft.token}
                  onChange={(e) => setConfigDraft((p) => ({ ...p, token: e.target.value }))}
                  placeholder="secret_..."
                  className="w-full border rounded-lg px-3 py-2 text-sm font-mono" />
              </div>
              <div>
                <label className="block text-xs text-gray-500 mb-1">Database ID</label>
                <input type="text" value={configDraft.database_id}
                  onChange={(e) => setConfigDraft((p) => ({ ...p, database_id: e.target.value }))}
                  placeholder="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                  className="w-full border rounded-lg px-3 py-2 text-sm font-mono" />
              </div>
            </div>
            <div className="mt-3 flex gap-2">
              <button onClick={handleSaveConfig} disabled={saving}
                className="text-sm bg-indigo-600 hover:bg-indigo-700 disabled:bg-indigo-300 text-white px-4 py-1.5 rounded-lg">
                {saving ? "Saving…" : "Save & connect"}
              </button>
              <button onClick={() => { setShowConfig(false); setConfigDraft(config); }}
                className="text-sm text-gray-500 hover:text-gray-700 px-3 py-1.5">
                Cancel
              </button>
            </div>
          </div>
        )}

        {/* Not configured prompt */}
        {!configured && !showConfig && (
          <div className="text-center py-24">
            <p className="text-gray-500 mb-4">Connect to a Notion database to track data injections.</p>
            <button onClick={() => setShowConfig(true)}
              className="text-sm bg-indigo-600 hover:bg-indigo-700 text-white px-5 py-2 rounded-lg">
              Configure Notion
            </button>
          </div>
        )}

        {/* Error */}
        {error && (
          <div className="mb-4 bg-red-50 border border-red-200 text-red-700 rounded-lg px-4 py-3 text-sm flex items-start justify-between gap-3">
            <span>{error}</span>
            <button onClick={() => setError(null)} className="text-red-400 hover:text-red-600 flex-shrink-0">✕</button>
          </div>
        )}

        {/* Loading */}
        {loading && !schema && (
          <div className="text-center py-20 text-gray-400">Loading database…</div>
        )}

        {/* Delete confirmation */}
        {deleteConfirmId && (
          <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50">
            <div className="bg-white rounded-xl p-6 shadow-xl max-w-sm w-full mx-4">
              <h3 className="font-semibold text-gray-800 mb-2">Archive this row?</h3>
              <p className="text-sm text-gray-500 mb-4">
                The page will be archived in Notion. You can recover it from the Notion trash.
              </p>
              <div className="flex gap-2 justify-end">
                <button onClick={() => setDeleteConfirmId(null)}
                  className="text-sm text-gray-500 hover:text-gray-700 px-3 py-1.5">
                  Cancel
                </button>
                <button onClick={() => handleDelete(deleteConfirmId)} disabled={saving}
                  className="text-sm bg-red-600 hover:bg-red-700 disabled:bg-red-300 text-white px-4 py-1.5 rounded-lg">
                  {saving ? "Archiving…" : "Archive"}
                </button>
              </div>
            </div>
          </div>
        )}

        {/* Table */}
        {schema && (
          <div className="bg-white rounded-xl shadow-sm overflow-hidden border">
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="bg-gray-50 border-b">
                    {allCols.map((name) => (
                      <th key={name} className="text-left px-4 py-3 font-medium text-gray-600 whitespace-nowrap">
                        {name}
                        <span className="ml-1 text-xs text-gray-400 font-normal">
                          {schema.properties[name].type}
                        </span>
                      </th>
                    ))}
                    <th className="px-4 py-3 w-28 text-right font-medium text-gray-600">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map((row) => (
                    <tr key={row.id} className="border-b hover:bg-gray-50 group">
                      {allCols.map((name) => {
                        const { type, options } = schema.properties[name];
                        const isEditing = editingId === row.id;
                        return (
                          <td key={name} className="px-4 py-2 align-middle">
                            {isEditing && EDITABLE_TYPES.has(type) ? (
                              <CellInput type={type} value={editValues[name]} options={options}
                                onChange={(v) => setEditValues((prev) => ({ ...prev, [name]: v }))} />
                            ) : (
                              <CellValue value={row.properties[name]} type={type} />
                            )}
                          </td>
                        );
                      })}
                      <td className="px-4 py-2 text-right align-middle">
                        {editingId === row.id ? (
                          <div className="flex gap-1 justify-end">
                            <button onClick={saveEdit} disabled={saving}
                              className="text-xs bg-indigo-600 hover:bg-indigo-700 disabled:bg-indigo-300 text-white px-2.5 py-1 rounded">
                              {saving ? "…" : "Save"}
                            </button>
                            <button onClick={cancelEdit}
                              className="text-xs text-gray-500 hover:text-gray-700 px-2 py-1">
                              Cancel
                            </button>
                          </div>
                        ) : (
                          <div className="flex gap-1 justify-end opacity-0 group-hover:opacity-100 transition-opacity">
                            <button onClick={() => startEdit(row)}
                              className="text-xs text-indigo-600 hover:text-indigo-800 px-2 py-1">
                              Edit
                            </button>
                            <button onClick={() => setDeleteConfirmId(row.id)}
                              className="text-xs text-red-500 hover:text-red-700 px-2 py-1">
                              Del
                            </button>
                          </div>
                        )}
                      </td>
                    </tr>
                  ))}

                  {/* New row */}
                  {showNewRow && (
                    <tr className="border-b bg-green-50">
                      {allCols.map((name) => {
                        const { type, options } = schema.properties[name];
                        return (
                          <td key={name} className="px-4 py-2 align-middle">
                            {EDITABLE_TYPES.has(type) ? (
                              <CellInput type={type} value={newValues[name]} options={options}
                                onChange={(v) => setNewValues((prev) => ({ ...prev, [name]: v }))} />
                            ) : (
                              <span className="text-gray-400 text-xs italic">auto</span>
                            )}
                          </td>
                        );
                      })}
                      <td className="px-4 py-2 text-right align-middle">
                        <div className="flex gap-1 justify-end">
                          <button onClick={saveNewRow} disabled={saving}
                            className="text-xs bg-green-600 hover:bg-green-700 disabled:bg-green-300 text-white px-2.5 py-1 rounded">
                            {saving ? "…" : "Create"}
                          </button>
                          <button onClick={() => { setShowNewRow(false); setNewValues({}); }}
                            className="text-xs text-gray-500 hover:text-gray-700 px-2 py-1">
                            Cancel
                          </button>
                        </div>
                      </td>
                    </tr>
                  )}

                  {rows.length === 0 && !showNewRow && !loading && (
                    <tr>
                      <td colSpan={allCols.length + 1} className="px-4 py-12 text-center text-gray-400">
                        No rows in this database.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
            <div className="px-4 py-2 bg-gray-50 border-t text-xs text-gray-400">
              {rows.length} row{rows.length !== 1 ? "s" : ""}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
