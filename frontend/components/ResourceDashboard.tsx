import { useState, useEffect, useCallback, useMemo, useRef } from "react";
import LogPanel from "./LogPanel";
import type { VolumetryEntry } from "./VolumetryPanel";
import type {
  Resource, DAG, DAGConfig, DAGRun, IntegrationRule, MappingIssue, QAExampleID,
  BlobInfo, LogEntry, AirflowConfig,
} from "../lib/api";
import {
  getDags, getDagConfig, getDagRuns, getDagRun, updateDagRun,
  getRules, updateRule,
  getMappingIssues, saveMappingIssues, resolveMappingIssue,
  getQAExamples, createQAExample, deleteQAExample,
  getIdColumns, getColumnValues,
  triggerDagStream, fetchDagLogsRestApi,
  linkResourceDags,
  profileBlobsStream,
  getResourceBlobs,
} from "../lib/api";

interface Props {
  resources: Resource[];
  containerUrl: string;
  onBack: () => void;
  /** Called when the user clicks "Trace" on a QA example. */
  onAnalyzeExample?: (resource: Resource, blobNames: string[], idColumn: string, idValue: string) => void;
  /** Called when the user clicks "Trace latest batch" — passes the blob names of the latest batch. */
  onTraceBatch?: (blobNames: string[]) => void;
}

export default function ResourceDashboard({ resources, containerUrl, onBack, onAnalyzeExample, onTraceBatch }: Props) {
  // Selection — persisted across reloads
  const [selectedResourceId, setSelectedResourceId] = useState<string>(() => {
    const saved = localStorage.getItem("dashboard_selectedResourceId");
    return (saved && resources.some((r) => r.id === saved)) ? saved : (resources[0]?.id ?? "");
  });
  const [selectedEnv, setSelectedEnv] = useState<string>(
    () => localStorage.getItem("dashboard_selectedEnv") ?? "dev"
  );

  useEffect(() => {
    if (selectedResourceId) localStorage.setItem("dashboard_selectedResourceId", selectedResourceId);
  }, [selectedResourceId]);

  useEffect(() => {
    localStorage.setItem("dashboard_selectedEnv", selectedEnv);
  }, [selectedEnv]);

  // Data
  const [dags, setDags] = useState<DAG[]>([]);
  const [dagConfig, setDagConfig] = useState<DAGConfig>({ container_name: "", environments: ["integration", "snap", "recette", "prod"] });
  const [dagRuns, setDagRuns] = useState<DAGRun[]>([]);
  const [rules, setRules] = useState<IntegrationRule[]>([]);
  const [mappingIssues, setMappingIssues] = useState<MappingIssue[]>([]);
  const [qaExamples, setQAExamples] = useState<QAExampleID[]>([]);

  // Resource-specific DAG linking
  const [resourceDagIds, setResourceDagIds] = useState<string[]>([]);
  const [linkingDags, setLinkingDags] = useState(false);
  const [showDagLinkDropdown, setShowDagLinkDropdown] = useState(false);

  // DAG runner
  const [dagLogs, setDagLogs] = useState<LogEntry[]>([]);
  const [taskLogs, setTaskLogs] = useState<Record<string, LogEntry[]>>({});
  const [taskStates, setTaskStates] = useState<Record<string, string>>({});
  const [collapsedTasks, setCollapsedTasks] = useState<Set<string>>(new Set());
  const [runningDagId, setRunningDagId] = useState<string | null>(null);
  const [loadingLogsForDagId, setLoadingLogsForDagId] = useState<string | null>(null);
  const [runElapsedS, setRunElapsedS] = useState<number>(0);
  const restoredLogsForRef = useRef<string | null>(null);
  const elapsedTimerRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Tick elapsed time while a DAG is running
  useEffect(() => {
    if (runningDagId !== null) {
      elapsedTimerRef.current = setInterval(() => {
        setRunElapsedS((prev) => prev + 1);
      }, 1000);
    } else {
      if (elapsedTimerRef.current !== null) {
        clearInterval(elapsedTimerRef.current);
        elapsedTimerRef.current = null;
      }
    }
    return () => {
      if (elapsedTimerRef.current !== null) {
        clearInterval(elapsedTimerRef.current);
        elapsedTimerRef.current = null;
      }
    };
  }, [runningDagId]);

  // Airflow REST API config
  const [airflowConfig, setAirflowConfig] = useState<AirflowConfig>(() => {
    const saved = localStorage.getItem("airflow_config");
    return saved ? JSON.parse(saved) : { base_url: "", username: "", password: "", verify_tls: true };
  });
  const [showAirflowConfig, setShowAirflowConfig] = useState(false);

  // Restore logs/states from most recent run when resource/DAG data loads.
  // dagRuns is now log-stripped, so we fetch the full run by ID lazily.
  useEffect(() => {
    if (restoredLogsForRef.current === selectedResourceId) return;
    if (resourceDagIds.length === 0 || dagRuns.length === 0) return;
    restoredLogsForRef.current = selectedResourceId;

    // Find the most recent run for this resource (lightweight list is enough for this)
    const recent = dagRuns
      .filter((r) => resourceDagIds.includes(r.dag_id))
      .sort((a, b) => b.triggered_at.localeCompare(a.triggered_at))[0];
    if (!recent) return;

    // Fetch full run data (with logs) only for this one run
    getDagRun(recent.id).then((full) => {
      if (!full.task_logs?.length && !full.task_sub_logs?.length) return;
      setDagLogs(full.task_logs ?? []);
      if (full.task_sub_logs) {
        const m: Record<string, LogEntry[]> = {};
        full.task_sub_logs.forEach(({ task_id, logs }) => { m[task_id] = logs; });
        setTaskLogs(m);
      }
      if (full.task_final_states) {
        const m: Record<string, string> = {};
        full.task_final_states.forEach(({ task_id, state }) => { m[task_id] = state; });
        setTaskStates(m);
        setCollapsedTasks(new Set(full.task_final_states.map((t) => t.task_id)));
      }
    }).catch(() => {});
  }, [dagRuns, resourceDagIds, selectedResourceId]);

  // Volumetry
  const [volumetryData, setVolumetryData] = useState<Record<string, VolumetryEntry>>({});

  // QA Example form
  const [idColumns, setIdColumns] = useState<string[]>([]);
  const [selectedIdColumn, setSelectedIdColumn] = useState("");
  const [columnValues, setColumnValues] = useState<string[]>([]);
  const [valueSearch, setValueSearch] = useState("");
  const [newLabel, setNewLabel] = useState("");
  const [selectedValue, setSelectedValue] = useState("");
  const [loadingIdCols, setLoadingIdCols] = useState(false);

  const selectedResource = resources.find((r) => r.id === selectedResourceId);

  // Lazily fetch blobs for the selected resource from the backend
  const [resourceBlobs, setResourceBlobs] = useState<BlobInfo[]>([]);
  const [loadingBlobs, setLoadingBlobs] = useState(false);

  useEffect(() => {
    if (!selectedResource || !containerUrl) { setResourceBlobs([]); return; }
    setLoadingBlobs(true);
    getResourceBlobs(selectedResource.id, containerUrl)
      .then(setResourceBlobs)
      .catch(() => setResourceBlobs([]))
      .finally(() => setLoadingBlobs(false));
  }, [selectedResource?.id, containerUrl]);

  // Load data in two phases:
  // Phase 1 (critical, fast) — getDags + getDagConfig fire first so linkedDags
  //   renders immediately and never shows "No DAGs linked" while waiting for runs.
  // Phase 2 (secondary, can be slow) — getDagRuns (potentially large), rules,
  //   mapping issues, QA examples are loaded after phase 1 completes.
  const loadData = useCallback(async () => {
    try {
      // ── Phase 1: critical data needed to render the DAG runner ──────────
      const [d, dc] = await Promise.all([getDags(), getDagConfig()]);
      setDags(d);
      setDagConfig(dc);
      if (dc.environments.length > 0 && !dc.environments.includes(selectedEnv)) {
        setSelectedEnv(dc.environments[0]);
      }
    } catch { /* ignore */ }

    try {
      // ── Phase 2: secondary data (runs list is stripped of logs — lightweight) ──
      const [runs, r, mi, qa] = await Promise.all([
        getDagRuns(),   // logs stripped by default — response is tiny now
        getRules(),
        getMappingIssues(selectedResourceId || undefined),
        getQAExamples(selectedResourceId || undefined),
      ]);
      setDagRuns(runs);
      setRules(r);
      setMappingIssues(mi);
      setQAExamples(qa);
    } catch { /* ignore */ }
  }, [selectedResourceId, selectedEnv]);

  useEffect(() => {
    loadData();
  }, [loadData]);

  // Load resource DAG links
  useEffect(() => {
    if (!selectedResource) return;
    const dagIds = selectedResource.dag_ids ?? [];
    setResourceDagIds(dagIds);
  }, [selectedResource]);

  // Load ID columns when resource changes
  useEffect(() => {
    if (!selectedResource || resourceBlobs.length === 0 || !containerUrl) return;
    setLoadingIdCols(true);
    getIdColumns(containerUrl, resourceBlobs.map((b) => b.name))
      .then((cols) => { setIdColumns(cols); setSelectedIdColumn(cols[0] ?? ""); })
      .catch(() => {})
      .finally(() => setLoadingIdCols(false));
  }, [selectedResource, resourceBlobs, containerUrl]);

  // Load column values when ID column changes
  useEffect(() => {
    if (!selectedIdColumn || resourceBlobs.length === 0 || !containerUrl) {
      setColumnValues([]);
      return;
    }
    getColumnValues(containerUrl, resourceBlobs.map((b) => b.name), selectedIdColumn)
      .then((data) => setColumnValues(data.values))
      .catch(() => setColumnValues([]));
  }, [selectedIdColumn, resourceBlobs, containerUrl]);

  // Volumetry
  const handleRefreshVolumetry = async () => {
    if (!selectedResourceId || resourceBlobs.length === 0) return;
    setVolumetryData((prev) => ({ ...prev, [selectedResourceId]: { profiles: [], status: "loading" } }));

    await profileBlobsStream(
      containerUrl,
      resourceBlobs.map((b) => b.name),
      (progress) => {
        setVolumetryData((prev) => ({
          ...prev,
          [selectedResourceId]: { ...prev[selectedResourceId], status: "loading", progress: { current: progress.current, total: progress.total, currentBlob: progress.blob_name } },
        }));
      },
      (profile) => {
        setVolumetryData((prev) => ({
          ...prev,
          [selectedResourceId]: { ...prev[selectedResourceId], status: "loading", profiles: [...(prev[selectedResourceId]?.profiles ?? []), profile] },
        }));
      },
      () => {
        setVolumetryData((prev) => ({
          ...prev,
          [selectedResourceId]: { ...prev[selectedResourceId], status: "loaded", progress: undefined, lastUpdated: new Date().toISOString() },
        }));
      },
      (message) => {
        setVolumetryData((prev) => ({
          ...prev,
          [selectedResourceId]: { status: "error", profiles: prev[selectedResourceId]?.profiles ?? [], error: message, progress: undefined },
        }));
      },
    );
  };

  // Trace latest batch — detects the last gap > GAP_MINUTES and selects blobs after it
  const handleTraceBatch = () => {
    if (!onTraceBatch || resourceBlobs.length === 0) return;
    const GAP_MINUTES = 15;
    const sorted = [...resourceBlobs]
      .filter((b) => b.last_modified)
      .sort((a, b) => a.last_modified.localeCompare(b.last_modified));
    let batchStartIndex = 0;
    for (let i = 1; i < sorted.length; i++) {
      const prev = new Date(sorted[i - 1].last_modified).getTime();
      const curr = new Date(sorted[i].last_modified).getTime();
      if ((curr - prev) / (1000 * 60) > GAP_MINUTES) batchStartIndex = i;
    }
    onTraceBatch(sorted.slice(batchStartIndex).map((b) => b.name));
  };

  // DAG runner
  const linkedDags = useMemo(() => {
    return resourceDagIds.map((id) => dags.find((d) => d.id === id)).filter(Boolean) as DAG[];
  }, [resourceDagIds, dags]);

  const handleTriggerDag = async (dag: DAG) => {
    setRunningDagId(dag.dag_id);
    setRunElapsedS(0);
    setDagLogs([]);
    setTaskLogs({});
    setTaskStates({});
    setCollapsedTasks(new Set());
    restoredLogsForRef.current = selectedResourceId; // don't overwrite with history while running

    const collectedLogs: LogEntry[] = [];
    const collectedTaskLogs: Record<string, LogEntry[]> = {};
    const collectedTaskStates: Record<string, string> = {};

    const addLog = (entry: LogEntry) => {
      collectedLogs.push(entry);
      setDagLogs((prev) => [...prev, entry]);
    };
    const addTaskLog = (taskId: string, entry: LogEntry) => {
      if (!collectedTaskLogs[taskId]) collectedTaskLogs[taskId] = [];
      collectedTaskLogs[taskId].push(entry);
      setTaskLogs((prev) => ({ ...prev, [taskId]: [...(prev[taskId] ?? []), entry] }));
    };
    const updateStates = (tasks: Array<{ task_id: string; state: string }>, elapsedS?: number) => {
      tasks.forEach((t) => { collectedTaskStates[t.task_id] = t.state; });
      setTaskStates(Object.fromEntries(tasks.map((t) => [t.task_id, t.state])));
      if (elapsedS !== undefined) setRunElapsedS(elapsedS);
    };

    await triggerDagStream(
      dag.dag_id,
      selectedEnv,
      addLog,
      async (result) => {
        setRunningDagId(null);
        if (result.run_id && airflowConfig.base_url && airflowConfig.username && airflowConfig.password) {
          await fetchDagLogsRestApi(
            airflowConfig,
            dag.dag_id,
            addLog,
            (taskId: string, logs: string) => {
              const taskEntries = logs.split("\n")
                .filter((l) => l.trim())
                .map((line) => ({
                  level: "info" as const,
                  message: line,
                  timestamp: new Date().toISOString(),
                }));
              addTaskLog(taskId, taskEntries[0] ?? { level: "info", message: logs, timestamp: new Date().toISOString() });
              if (!collectedTaskLogs[taskId]) collectedTaskLogs[taskId] = [];
              collectedTaskLogs[taskId].push(...taskEntries);
            },
            async (taskLogs) => {
              const states: Record<string, string> = {};
              for (const log of taskLogs) {
                states[log.task_id] = log.state;
                collectedTaskStates[log.task_id] = log.state;
              }
              setTaskStates(states);
              await updateDagRun(result.record_id, {
                task_logs: collectedLogs,
                task_sub_logs: Object.entries(collectedTaskLogs).map(([task_id, logs]) => ({ task_id, logs })),
                task_final_states: Object.entries(collectedTaskStates).map(([task_id, state]) => ({ task_id, state })),
              });
            },
            (message) => {
              addLog({ level: "error", message, timestamp: new Date().toISOString() });
            },
          );
        } else if (result.run_id) {
          addLog({ level: "warning", message: "Configure Airflow REST API to fetch logs after trigger", timestamp: new Date().toISOString() });
        }
        const runs = await getDagRuns();
        setDagRuns(runs);
      },
      () => setRunningDagId(null),
    );
  };

  const handleLoadLastRun = async (dag: DAG) => {
    setLoadingLogsForDagId(dag.dag_id);
    setRunElapsedS(0);
    setDagLogs([]);
    setTaskLogs({});
    setTaskStates({});
    setCollapsedTasks(new Set());
    restoredLogsForRef.current = selectedResourceId;

    try {
      // Airflow config is now required
      if (!airflowConfig.base_url || !airflowConfig.username || !airflowConfig.password) {
        throw new Error("Please configure Airflow REST API connection first (click Configure button)");
      }

      const addLog = (entry: LogEntry) => setDagLogs((prev) => [...prev, entry]);

      await fetchDagLogsRestApi(
        airflowConfig,
        dag.dag_id,
        addLog,
        (taskId: string, logs: string) => {
          setTaskLogs((prev) => {
            const taskEntries = logs.split("\n")
              .filter((l) => l.trim())
              .map((line) => ({
                level: "info" as const,
                message: line,
                timestamp: new Date().toISOString(),
              }));
            return { ...prev, [taskId]: taskEntries };
          });
        },
        async (taskLogs) => {
          // Update task states from the fetched logs
          const states: Record<string, string> = {};
          let allLogText = "";
          for (const log of taskLogs) {
            states[log.task_id] = log.state;
            allLogText += log.logs + "\n";
          }
          setTaskStates(states);

          // Parse mapping issues from loaded logs
          const mappingIssuePattern = /\} WARNING - Les valeurs (\(.+?\)) \((\d+)\) ne sont pas présentes dans le mapping (.+?)\./g;
          const foundIssues: Array<{ unmapped_value: string; count: number; column: string }> = [];
          let match;
          while ((match = mappingIssuePattern.exec(allLogText)) !== null) {
            foundIssues.push({
              unmapped_value: match[1],
              count: parseInt(match[2]),
              column: match[3],
            });
          }

          // Save mapping issues if any found
          if (foundIssues.length > 0 && selectedResourceId) {
            const localRecord = dagRuns.find((r) => r.dag_id === dag.dag_id);
            if (localRecord) {
              await saveMappingIssues(foundIssues, selectedResourceId, localRecord.id);
              const updated = await getMappingIssues(selectedResourceId);
              setMappingIssues(updated);
              addLog({ level: "success", message: `Found ${foundIssues.length} mapping issue(s)`, timestamp: new Date().toISOString() });
            }
          } else if (foundIssues.length === 0) {
            addLog({ level: "info", message: "No mapping issues found", timestamp: new Date().toISOString() });
          }

          const runs = await getDagRuns();
          setDagRuns(runs);
        },
        (message) => {
          setDagLogs((prev) => [...prev, { level: "error", message, timestamp: new Date().toISOString() }]);
        },
      );
    } catch (e: unknown) {
      const message = e instanceof Error ? e.message : String(e);
      setDagLogs((prev) => [...prev, { level: "error", message, timestamp: new Date().toISOString() }]);
    } finally {
      setLoadingLogsForDagId(null);
    }
  };

  const getLastRun = (dagId: string): DAGRun | undefined => {
    return dagRuns
      .filter((r) => r.dag_id === dagId && r.padoa_env === selectedEnv)
      .sort((a, b) => b.triggered_at.localeCompare(a.triggered_at))[0];
  };

  // DAG linking
  const handleToggleDagLink = async (dagId: string) => {
    const newIds = resourceDagIds.includes(dagId)
      ? resourceDagIds.filter((id) => id !== dagId)
      : [...resourceDagIds, dagId];
    setLinkingDags(true);
    try {
      await linkResourceDags(selectedResourceId, newIds);
      setResourceDagIds(newIds);
    } catch { /* ignore */ }
    setLinkingDags(false);
  };

  // Rule toggle
  const handleToggleRule = async (ruleId: string, checked: boolean) => {
    await updateRule(ruleId, { checked });
    setRules((prev) => prev.map((r) => r.id === ruleId ? { ...r, checked } : r));
  };

  // QA examples
  const handleAddQAExample = async () => {
    if (!selectedValue || !selectedIdColumn || !selectedResourceId) return;
    await createQAExample({
      resource_id: selectedResourceId,
      id_column: selectedIdColumn,
      id_value: selectedValue,
      label: newLabel || "Example ID",
    });
    const updated = await getQAExamples(selectedResourceId);
    setQAExamples(updated);
    setSelectedValue("");
    setNewLabel("");
  };

  const handleDeleteQAExample = async (id: string) => {
    await deleteQAExample(id);
    const updated = await getQAExamples(selectedResourceId);
    setQAExamples(updated);
  };

  // Mapping issue resolve
  const handleResolve = async (issueId: string, resolved: boolean) => {
    await resolveMappingIssue(issueId, resolved);
    const updated = await getMappingIssues(selectedResourceId);
    setMappingIssues(updated);
  };

  // Volumetry stats
  const volEntry = volumetryData[selectedResourceId];
  const stageRowCounts = useMemo(() => {
    if (!volEntry?.profiles) return {};
    const counts: Record<string, number> = {};
    for (const p of volEntry.profiles) {
      const stage = p.detected_stage;
      counts[stage] = (counts[stage] ?? 0) + p.row_count;
    }
    return counts;
  }, [volEntry]);

  const resourceRules = useMemo(() => {
    return rules.filter((r) => r.resource_ids.includes(selectedResourceId));
  }, [rules, selectedResourceId]);

  const openIssues = useMemo(() => {
    return mappingIssues.filter((i) => !i.resolved);
  }, [mappingIssues]);

  const issuesByColumn = useMemo(() => {
    const grouped: Record<string, MappingIssue[]> = {};
    for (const issue of mappingIssues) {
      if (!grouped[issue.column]) grouped[issue.column] = [];
      grouped[issue.column].push(issue);
    }
    return grouped;
  }, [mappingIssues]);

  const filteredValues = useMemo(() => {
    if (!valueSearch) return columnValues.slice(0, 50);
    return columnValues.filter((v) => v.toLowerCase().includes(valueSearch.toLowerCase())).slice(0, 50);
  }, [columnValues, valueSearch]);

  if (resources.length === 0) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <p className="text-gray-500 mb-4">No resources defined yet. Create resources first.</p>
          <button onClick={onBack} className="text-sm text-indigo-600 hover:text-indigo-800">Go back</button>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Top bar */}
      <div className="bg-white border-b border-gray-200 px-6 py-3 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <button onClick={onBack} className="text-sm text-gray-400 hover:text-gray-700 transition-colors">
            &larr;
          </button>
          <span className="text-gray-200">|</span>
          <h2 className="text-sm font-semibold text-gray-800">Resource Dashboard</h2>
        </div>
        <div className="flex items-center gap-3">
          <select
            value={selectedResourceId}
            onChange={(e) => setSelectedResourceId(e.target.value)}
            className="border border-gray-200 rounded-lg px-3 py-1.5 text-sm font-medium focus:outline-none focus:ring-2 focus:ring-indigo-400"
          >
            {resources.map((r) => (
              <option key={r.id} value={r.id}>{r.business_name}</option>
            ))}
          </select>
          <select
            value={selectedEnv}
            onChange={(e) => setSelectedEnv(e.target.value)}
            className="border border-gray-200 rounded-lg px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-400"
          >
            {dagConfig.environments.map((env) => (
              <option key={env} value={env}>{env}</option>
            ))}
          </select>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-6 py-6 space-y-6">
        {/* Top row: Volumetry + Mapping Issues */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Volumetry */}
          <div className="bg-white border border-gray-200 rounded-xl p-4">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-sm font-semibold text-gray-700">Volumetry</h3>
              <button
                onClick={handleRefreshVolumetry}
                disabled={volEntry?.status === "loading"}
                className="text-xs text-indigo-600 hover:text-indigo-800 disabled:text-gray-400"
              >
                {volEntry?.status === "loading" ? "Loading..." : "Refresh"}
              </button>
            </div>
            {volEntry?.status === "loading" && volEntry.progress && (
              <p className="text-xs text-gray-400 mb-2">
                Profiling {volEntry.progress.current}/{volEntry.progress.total}...
              </p>
            )}
            {Object.keys(stageRowCounts).length > 0 ? (
              <div className="space-y-2">
                {Object.entries(stageRowCounts).sort(([a], [b]) => a.localeCompare(b)).map(([stage, count]) => (
                  <div key={stage} className="flex items-center justify-between">
                    <span className="text-sm text-gray-600">{stage}</span>
                    <span className="text-sm font-mono font-medium text-gray-800">{count.toLocaleString()}</span>
                  </div>
                ))}
                {Object.keys(stageRowCounts).length >= 2 && (
                  <div className="pt-2 border-t border-gray-100">
                    <div className="flex items-center justify-between text-xs text-gray-500">
                      <span>Drop rate (extract &rarr; last)</span>
                      <span>
                        {(() => {
                          const vals = Object.values(stageRowCounts);
                          const first = vals[0];
                          const last = vals[vals.length - 1];
                          if (first === 0) return "N/A";
                          return ((1 - last / first) * 100).toFixed(1) + "%";
                        })()}
                      </span>
                    </div>
                  </div>
                )}
              </div>
            ) : (
              <p className="text-sm text-gray-400">
                {volEntry?.status === "error" ? volEntry.error : "Click Refresh to load volumetry data."}
              </p>
            )}
          </div>

          {/* Mapping Issues */}
          <div className="bg-white border border-gray-200 rounded-xl p-4">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-sm font-semibold text-gray-700">
                Mapping Issues
                {openIssues.length > 0 && (
                  <span className="ml-2 text-xs bg-red-100 text-red-600 px-2 py-0.5 rounded-full font-medium">
                    {openIssues.length} open
                  </span>
                )}
              </h3>
            </div>
            {Object.keys(issuesByColumn).length > 0 ? (
              <div className="space-y-3 max-h-64 overflow-y-auto">
                {Object.entries(issuesByColumn).map(([column, issues]) => (
                  <div key={column}>
                    <h4 className="text-xs font-medium text-gray-500 mb-1">column: {column}</h4>
                    {issues.map((issue) => (
                      <div key={issue.id} className="flex items-center justify-between py-1 pl-3">
                        <div className="flex items-center gap-2">
                          <span className={`text-sm ${issue.resolved ? "text-gray-400 line-through" : "text-gray-800"}`}>
                            {issue.unmapped_value}
                          </span>
                          {issue.count != null && (
                            <span className="text-xs bg-amber-100 text-amber-700 px-1.5 py-0.5 rounded">
                              {issue.count}×
                            </span>
                          )}
                          <span className="text-xs text-gray-400">
                            (since {new Date(issue.first_seen).toLocaleDateString()})
                          </span>
                          {issue.fixed_in_rerun && !issue.resolved && (
                            <span className="text-xs bg-green-100 text-green-700 px-1.5 py-0.5 rounded">
                              Fixed in re-run {issue.fixed_in_rerun}
                            </span>
                          )}
                        </div>
                        <button
                          onClick={() => handleResolve(issue.id, !issue.resolved)}
                          className={`text-xs px-2 py-0.5 rounded transition-colors ${
                            issue.resolved
                              ? "text-gray-500 hover:text-amber-600"
                              : "text-green-600 hover:text-green-800 border border-green-200"
                          }`}
                        >
                          {issue.resolved ? "Unresolve" : "Resolve"}
                        </button>
                      </div>
                    ))}
                  </div>
                ))}
              </div>
            ) : (
              <p className="text-sm text-gray-400">
                {dagLogs.length === 0
                  ? "Load logs first to check for mapping issues."
                  : "No mapping issues found."}
              </p>
            )}
          </div>
        </div>

        {/* Middle row: QA Example IDs + Integration Rules */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* QA Example IDs */}
          <div className="bg-white border border-gray-200 rounded-xl p-4">
            <h3 className="text-sm font-semibold text-gray-700 mb-3">QA Example IDs</h3>

            {/* Column selector */}
            <div className="flex items-center gap-2 mb-3">
              <label className="text-xs text-gray-500">Column:</label>
              <select
                value={selectedIdColumn}
                onChange={(e) => setSelectedIdColumn(e.target.value)}
                disabled={loadingIdCols}
                className="border border-gray-200 rounded-lg px-2 py-1 text-xs focus:outline-none focus:ring-2 focus:ring-indigo-400"
              >
                {idColumns.map((col) => (
                  <option key={col} value={col}>{col}</option>
                ))}
              </select>
              {idColumns.length > 1 && (
                <span className="text-xs text-gray-400">
                  (also: {idColumns.filter((c) => c !== selectedIdColumn).join(", ")})
                </span>
              )}
            </div>

            {/* Saved examples */}
            {qaExamples.length > 0 && (
              <div className="space-y-1 mb-3">
                {qaExamples.map((ex) => (
                  <div key={ex.id} className="flex items-center justify-between py-1 group">
                    <div>
                      <span className="text-sm font-mono text-gray-800">{ex.id_value}</span>
                      <span className="text-xs text-gray-400 ml-2">({ex.id_column})</span>
                      <span className="text-xs text-gray-500 ml-2">— {ex.label}</span>
                    </div>
                    <div className="flex items-center gap-2">
                      {onAnalyzeExample && (
                        <button
                          onClick={() => onAnalyzeExample(selectedResource!, resourceBlobs.map((b) => b.name), ex.id_column, ex.id_value)}
                          disabled={resourceBlobs.length === 0 || !containerUrl}
                          title={
                            !containerUrl
                              ? "Connect to a container first to trace this record"
                              : resourceBlobs.length === 0
                              ? "No blobs matched for this resource"
                              : `Trace ${ex.id_column} = ${ex.id_value} through all pipeline stages`
                          }
                          className="text-xs text-indigo-600 hover:text-indigo-800 disabled:text-gray-300 border border-indigo-200 hover:border-indigo-400 disabled:border-gray-200 px-2 py-0.5 rounded font-medium transition-colors"
                        >
                          Trace →
                        </button>
                      )}
                      <button
                        onClick={() => handleDeleteQAExample(ex.id)}
                        className="text-xs text-gray-300 hover:text-red-500 transition-colors"
                      >
                        ×
                      </button>
                    </div>
                  </div>
                ))}
              </div>
            )}

            {/* Add from values */}
            <div className="border-t border-gray-100 pt-3 space-y-2">
              <input
                type="text"
                placeholder="Search values..."
                value={valueSearch}
                onChange={(e) => setValueSearch(e.target.value)}
                className="w-full border border-gray-200 rounded-lg px-2 py-1.5 text-xs focus:outline-none focus:ring-2 focus:ring-indigo-400"
              />
              {filteredValues.length > 0 && (
                <div className="max-h-32 overflow-y-auto border border-gray-100 rounded-lg">
                  {filteredValues.map((v) => (
                    <button
                      key={v}
                      onClick={() => setSelectedValue(v)}
                      className={`block w-full text-left px-2 py-1 text-xs hover:bg-indigo-50 ${selectedValue === v ? "bg-indigo-100 text-indigo-800" : "text-gray-700"}`}
                    >
                      {v}
                    </button>
                  ))}
                </div>
              )}
              {selectedValue && (
                <div className="flex items-center gap-2">
                  <span className="text-xs font-mono text-gray-800">{selectedValue}</span>
                  <input
                    type="text"
                    placeholder="Label (e.g. 'dropped at clean')"
                    value={newLabel}
                    onChange={(e) => setNewLabel(e.target.value)}
                    className="flex-1 border border-gray-200 rounded-lg px-2 py-1 text-xs focus:outline-none focus:ring-2 focus:ring-indigo-400"
                  />
                  <button
                    onClick={handleAddQAExample}
                    className="text-xs bg-indigo-600 hover:bg-indigo-700 text-white px-2 py-1 rounded-lg"
                  >
                    Add
                  </button>
                </div>
              )}
            </div>
          </div>

          {/* Integration Rules */}
          <div className="bg-white border border-gray-200 rounded-xl p-4">
            <h3 className="text-sm font-semibold text-gray-700 mb-3">
              Integration Rules
              {resourceRules.length > 0 && (
                <span className="ml-2 text-xs text-gray-400">
                  {resourceRules.filter((r) => r.checked).length}/{resourceRules.length} verified
                </span>
              )}
            </h3>
            {resourceRules.length > 0 ? (
              <div className="space-y-2">
                {resourceRules.map((rule) => (
                  <label key={rule.id} className="flex items-start gap-2 cursor-pointer group">
                    <input
                      type="checkbox"
                      checked={rule.checked}
                      onChange={(e) => handleToggleRule(rule.id, e.target.checked)}
                      className="mt-1 rounded border-gray-300 text-indigo-600 focus:ring-indigo-500"
                    />
                    <span className={`text-sm ${rule.checked ? "text-gray-400 line-through" : "text-gray-800"}`}>
                      {rule.description}
                    </span>
                  </label>
                ))}
              </div>
            ) : (
              <p className="text-sm text-gray-400">No rules linked to this resource. Link rules in the Integration Rule Manager.</p>
            )}
          </div>
        </div>

        {/* Bottom: DAG Runner */}
        <div className="bg-white border border-gray-200 rounded-xl p-4">
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-sm font-semibold text-gray-700">
              DAG Runner
              <span className="ml-2 text-xs text-gray-400 font-normal">padoa_env: {selectedEnv}</span>
              {runningDagId !== null && (
                <span className="ml-3 text-xs font-mono text-blue-500 animate-pulse">
                  {Math.floor(runElapsedS / 60) > 0
                    ? `${Math.floor(runElapsedS / 60)}m ${runElapsedS % 60}s`
                    : `${runElapsedS}s`}
                </span>
              )}
            </h3>
            <div className="flex items-center gap-2">
              {onTraceBatch && (
                <button
                  onClick={handleTraceBatch}
                  disabled={resourceBlobs.length === 0 || loadingBlobs}
                  className="text-xs bg-indigo-100 text-indigo-700 hover:bg-indigo-200 disabled:bg-gray-100 disabled:text-gray-400 px-2 py-1 rounded transition-colors"
                  title={loadingBlobs ? "Loading blobs…" : resourceBlobs.length === 0 ? "No blobs found for this resource" : `Trace ${resourceBlobs.length} blob(s) from latest batch`}
                >
                  {loadingBlobs ? "Loading…" : "Trace latest batch →"}
                </button>
              )}
              <button
                onClick={() => setShowAirflowConfig(!showAirflowConfig)}
                className={`text-xs px-2 py-1 rounded ${
                  airflowConfig.base_url && airflowConfig.username && airflowConfig.password
                    ? "bg-green-100 text-green-700 hover:bg-green-200"
                    : "bg-gray-100 text-gray-600 hover:bg-gray-200"
                }`}
              >
                {airflowConfig.base_url ? "✓ REST API" : "Configure"}
              </button>
              <div className="relative">
                <button
                  onClick={() => setShowDagLinkDropdown(!showDagLinkDropdown)}
                  disabled={linkingDags}
                  className="text-xs text-indigo-600 hover:text-indigo-800"
                >
                  {linkingDags ? "Saving..." : "Link DAGs"}
                </button>
              {showDagLinkDropdown && (
                <div className="absolute right-0 top-6 z-10 bg-white border border-gray-200 rounded-lg shadow-lg p-2 min-w-[200px]">
                  {dags.length === 0 && <p className="text-xs text-gray-400 p-2">No DAGs defined. Add them in DAG Manager.</p>}
                  {dags.map((d) => (
                    <label key={d.id} className="flex items-center gap-2 px-2 py-1 hover:bg-gray-50 cursor-pointer rounded">
                      <input
                        type="checkbox"
                        checked={resourceDagIds.includes(d.id)}
                        onChange={() => handleToggleDagLink(d.id)}
                        className="rounded border-gray-300 text-indigo-600 focus:ring-indigo-500"
                      />
                      <span className="text-xs text-gray-700">{d.display_name}</span>
                      <span className="text-xs text-gray-400 font-mono">{d.dag_id}</span>
                    </label>
                  ))}
                  <button
                    onClick={() => setShowDagLinkDropdown(false)}
                    className="mt-1 w-full text-xs text-gray-500 hover:text-gray-700 text-center py-1 border-t border-gray-100"
                  >
                    Done
                  </button>
                </div>
              )}
              </div>
            </div>
          </div>

          {/* Airflow REST API Config */}
          {showAirflowConfig && (
            <div className="mb-4 p-3 bg-blue-50 border border-blue-200 rounded-lg">
              <p className="text-xs text-blue-700 mb-2">Airflow REST API Configuration</p>
              <div className="space-y-2">
                <input
                  type="text"
                  placeholder="Base URL (e.g., http://localhost:8888)"
                  value={airflowConfig.base_url}
                  onChange={(e) => {
                    const updated = { ...airflowConfig, base_url: e.target.value };
                    setAirflowConfig(updated);
                    localStorage.setItem("airflow_config", JSON.stringify(updated));
                  }}
                  className="w-full px-2 py-1 text-xs border border-gray-300 rounded"
                />
                <input
                  type="text"
                  placeholder="Username"
                  value={airflowConfig.username}
                  onChange={(e) => {
                    const updated = { ...airflowConfig, username: e.target.value };
                    setAirflowConfig(updated);
                    localStorage.setItem("airflow_config", JSON.stringify(updated));
                  }}
                  className="w-full px-2 py-1 text-xs border border-gray-300 rounded"
                />
                <input
                  type="password"
                  placeholder="Password"
                  value={airflowConfig.password}
                  onChange={(e) => {
                    const updated = { ...airflowConfig, password: e.target.value };
                    setAirflowConfig(updated);
                    localStorage.setItem("airflow_config", JSON.stringify(updated));
                  }}
                  className="w-full px-2 py-1 text-xs border border-gray-300 rounded"
                />
                <label className="flex items-center gap-2 text-xs">
                  <input
                    type="checkbox"
                    checked={airflowConfig.verify_tls !== false}
                    onChange={(e) => {
                      const updated = { ...airflowConfig, verify_tls: e.target.checked };
                      setAirflowConfig(updated);
                      localStorage.setItem("airflow_config", JSON.stringify(updated));
                    }}
                    className="rounded border-gray-300"
                  />
                  <span className="text-gray-600">Verify TLS</span>
                </label>
                <button
                  onClick={() => setShowAirflowConfig(false)}
                  className="w-full text-xs bg-blue-600 hover:bg-blue-700 text-white px-2 py-1 rounded"
                >
                  Done
                </button>
              </div>
            </div>
          )}

          {linkedDags.length > 0 ? (
            <div className="space-y-2">
              {linkedDags.map((dag, idx) => {
                const lastRun = getLastRun(dag.dag_id);
                const isRunning = runningDagId === dag.dag_id;
                const isLoadingLogs = loadingLogsForDagId === dag.dag_id;
                const isBusy = isRunning || isLoadingLogs || runningDagId !== null || loadingLogsForDagId !== null;

                return (
                  <div key={dag.id} className="flex items-center justify-between py-2 px-3 bg-gray-50 rounded-lg">
                    <div className="flex items-center gap-3">
                      <span className="text-xs text-gray-400 font-medium">{idx + 1}.</span>
                      <span className="text-sm font-medium text-gray-800">{dag.display_name}</span>
                      <span className="text-xs font-mono text-gray-400">{dag.dag_id}</span>
                    </div>
                    <div className="flex items-center gap-3">
                      {lastRun && (
                        <span className={`text-xs ${lastRun.status === "triggered" ? "text-green-600" : "text-gray-500"}`}>
                          {lastRun.status === "triggered" ? "\u2713" : "\u2717"}{" "}
                          {new Date(lastRun.triggered_at).toLocaleString()}
                        </span>
                      )}
                      {!lastRun && <span className="text-xs text-gray-400">not run yet</span>}
                      {!airflowConfig.base_url && (
                        <span className="text-xs text-amber-600" title="Configure Airflow REST API first">⚠ REST API</span>
                      )}
                      <button
                        onClick={() => handleLoadLastRun(dag)}
                        disabled={isBusy}
                        className="text-xs bg-gray-600 hover:bg-gray-700 disabled:bg-gray-300 text-white px-3 py-1 rounded-lg transition-colors"
                        title={!airflowConfig.base_url ? "Configure Airflow REST API first (click Configure button above)" : "Fetch latest run logs via REST API"}
                      >
                        {isLoadingLogs ? "Loading..." : "Load last run"}
                      </button>
                      <button
                        onClick={() => handleTriggerDag(dag)}
                        disabled={isBusy}
                        className="text-xs bg-indigo-600 hover:bg-indigo-700 disabled:bg-gray-300 text-white px-3 py-1 rounded-lg transition-colors"
                      >
                        {isRunning ? "Running..." : "Run"}
                      </button>
                    </div>
                  </div>
                );
              })}
            </div>
          ) : (
            <p className="text-sm text-gray-400">No DAGs linked. Click &quot;Link DAGs&quot; to add them.</p>
          )}

          {/* DAG logs — main terminal */}
          {dagLogs.length > 0 && (
            <div className="mt-4">
              <LogPanel entries={dagLogs} running={runningDagId !== null} />
            </div>
          )}

          {/* Per-task collapsible sub-terminals */}
          {Object.keys(taskStates).length > 0 && (
            <div className="mt-3 space-y-1">
              {Object.entries(taskStates).map(([taskId, state]) => {
                const isCollapsed = collapsedTasks.has(taskId);
                const stateColor =
                  state === "success" ? "bg-green-900 text-green-300" :
                  state === "failed" ? "bg-red-900 text-red-300" :
                  state === "running" ? "bg-blue-900 text-blue-300" :
                  state === "upstream_failed" ? "bg-amber-900 text-amber-300" :
                  "bg-gray-800 text-gray-400";
                return (
                  <div key={taskId} className="border border-gray-800 rounded-lg overflow-hidden">
                    <button
                      className="w-full flex items-center gap-2 px-3 py-2 bg-gray-900 hover:bg-gray-800 text-left transition-colors"
                      onClick={() => setCollapsedTasks((prev) => {
                        const next = new Set(prev);
                        if (next.has(taskId)) next.delete(taskId); else next.add(taskId);
                        return next;
                      })}
                    >
                      <span className="text-gray-500 text-xs w-3">{isCollapsed ? "▶" : "▼"}</span>
                      <span className="text-xs font-mono text-gray-200 flex-1">{taskId}</span>
                      <span className={`text-xs px-2 py-0.5 rounded font-medium ${stateColor}`}>{state}</span>
                    </button>
                    {!isCollapsed && (
                      <LogPanel entries={taskLogs[taskId] ?? []} running={state === "running"} />
                    )}
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
