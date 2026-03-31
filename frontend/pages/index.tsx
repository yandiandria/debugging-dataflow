import { useState, useEffect, useMemo, useRef } from "react";
import ConnectionForm from "../components/ConnectionForm";
import FileBrowser from "../components/FileBrowser";
import AnalysisConfig from "../components/AnalysisConfig";
import AnalysisHistory from "../components/AnalysisHistory";
import FlowResults from "../components/FlowResults";
import LogPanel from "../components/LogPanel";
import ResourceManager from "../components/ResourceManager";
import VolumetryPanel from "../components/VolumetryPanel";
import DAGManager from "../components/DAGManager";
import IntegrationRuleManager from "../components/IntegrationRuleManager";
import ResourceDashboard from "../components/ResourceDashboard";
import NotionDatabase from "../components/NotionDatabase";
import type { VolumetryEntry } from "../components/VolumetryPanel";
import {
  listBlobs,
  analyzeStream,
  getResources,
  createResource,
  updateResource,
  deleteResource,
  profileBlobsStream,
  createDag,
  getBlobPreview,
  getLatestAirflowRunId,
  getTaskStates,
  getRunningTaskLog,
} from "../lib/api";
import type { BlobInfo, FilterCondition, AnalyzeResultFull, LogEntry, Resource, TaskInstanceState } from "../lib/api";

const STAGE_SHORT: Record<string, string> = {
  extract: "Extraction",
  transform: "Transformation",
  clean_cleaned: "Nettoyage (Clean)",
  clean_incoherent: "Nettoyage (Incohérent)",
  compare_identify: "Comparaison & Identification",
};

const STAGE_ROWS: string[][] = [
  ["extract"],
  ["transform"],
  ["clean_cleaned", "clean_incoherent"],
  ["compare_identify"],
];

type SectionKey = "config" | "logs" | "running";
type LoadStatus = "idle" | "loading" | "loaded" | "error";

type Step = "connect" | "browse" | "config" | "analyzing" | "results" | "resources" | "dags" | "rules" | "dashboard" | "history" | "notion";

const GAP_MINUTES = 15;

function detectBatchStart(matchingBlobs: BlobInfo[]): string {
  const sorted = matchingBlobs
    .filter((b) => b.last_modified)
    .sort((a, b) => (a.last_modified ?? "").localeCompare(b.last_modified ?? ""));
  if (sorted.length < 2) return "";
  let lastGapIndex = -1;
  for (let i = 1; i < sorted.length; i++) {
    const prev = new Date(sorted[i - 1].last_modified).getTime();
    const curr = new Date(sorted[i].last_modified).getTime();
    if ((curr - prev) / (1000 * 60) > GAP_MINUTES) lastGapIndex = i;
  }
  return lastGapIndex >= 0 ? sorted[lastGapIndex].last_modified : "";
}

export default function Home() {
  const [step, setStep] = useState<Step>("connect");
  const [containerUrl, setContainerUrl] = useState(
    () => (typeof window !== 'undefined' ? localStorage.getItem("dataflow_container_url") : null) ?? ""
  );
  const [blobs, setBlobs] = useState<BlobInfo[]>([]);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [result, setResult] = useState<AnalyzeResultFull | null>(null);
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Store params used for the last successful connection so we can refresh blobs
  const [connectionParams, setConnectionParams] = useState<{
    url: string;
    dateFrom?: string;
    dateTo?: string;
    prefix?: string;
  } | null>(null);

  const [resources, setResources] = useState<Resource[]>([]);
  const [initialConfig, setInitialConfig] = useState<{
    keyColumns: string[];
    filters: FilterCondition[];
    deduplicate: boolean;
    filterLogic: "AND" | "OR";
  } | undefined>(undefined);

  // Resource date filtering — shared across ResourceManager, VolumetryPanel, and profile handler
  const [dateOverrides, setDateOverrides] = useState<Record<string, string>>({});

  const matchingBlobsByResource = useMemo(() => {
    const result: Record<string, BlobInfo[]> = {};
    for (const r of resources) {
      const eps = r.extract_prefixes ?? [];
      result[r.id] = blobs.filter(
        (b) =>
          b.name.startsWith(r.technical_name) ||
          eps.some((p) => p.trim() && b.name.startsWith(p.trim()))
      );
    }
    return result;
  }, [resources, blobs]);

  const autoDateByResource = useMemo(() => {
    const result: Record<string, string> = {};
    for (const r of resources) {
      result[r.id] = detectBatchStart(matchingBlobsByResource[r.id] ?? []);
    }
    return result;
  }, [resources, matchingBlobsByResource]);

  const filteredBlobsByResource = useMemo(() => {
    const result: Record<string, BlobInfo[]> = {};
    for (const r of resources) {
      const matching = matchingBlobsByResource[r.id] ?? [];
      const filter = r.id in dateOverrides
        ? dateOverrides[r.id]
        : (autoDateByResource[r.id] ?? "");
      result[r.id] = filter
        ? matching.filter((b) => (b.last_modified ?? "") >= filter)
        : matching;
    }
    return result;
  }, [resources, matchingBlobsByResource, autoDateByResource, dateOverrides]);

  // Volumetry panel state — cleared when container changes
  const [volumetryPanelOpen, setVolumetryPanelOpen] = useState(false);
  const [volumetryData, setVolumetryData] = useState<Record<string, VolumetryEntry>>({});

  // Load resources once on mount
  useEffect(() => {
    getResources().then(setResources).catch(() => {});
  }, []);

  // Persist containerUrl to localStorage
  useEffect(() => {
    if (typeof window !== 'undefined') {
      if (containerUrl) {
        localStorage.setItem("dataflow_container_url", containerUrl);
      } else {
        localStorage.removeItem("dataflow_container_url");
      }
    }
  }, [containerUrl]);

  // Auto-navigate to dashboard on first load when saved containerUrl + resources are available
  const hasAutoNavigated = useRef(false);
  useEffect(() => {
    if (hasAutoNavigated.current || resources.length === 0 || !containerUrl) return;
    hasAutoNavigated.current = true;
    setStep("dashboard");
  }, [resources, containerUrl]);

  const refreshResources = () => getResources().then(setResources).catch(() => {});

  // ── Step 1: connect ────────────────────────────────────────────────────────
  const handleConnect = async (url: string, dateFrom?: string, dateTo?: string, prefix?: string) => {
    setLoading(true);
    setError(null);
    try {
      const allExtractPrefixes = resources.flatMap((r) => r.extract_prefixes ?? []);
      const data = await listBlobs(url, dateFrom, dateTo, prefix, allExtractPrefixes.length > 0 ? allExtractPrefixes : undefined);
      setContainerUrl(url);
      setConnectionParams({ url, dateFrom, dateTo, prefix });
      setBlobs(data);
      setSelected(new Set());
      // Clear volumetry when connecting to a new container
      setVolumetryData({});
      setStep("browse");
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Unknown error");
    } finally {
      setLoading(false);
    }
  };

  // ── Refresh blobs (re-fetch with current resource extract prefixes) ─────────
  const handleRefreshBlobs = async () => {
    if (!connectionParams) return;
    setLoading(true);
    setError(null);
    try {
      const allExtractPrefixes = resources.flatMap((r) => r.extract_prefixes ?? []);
      const data = await listBlobs(
        connectionParams.url,
        connectionParams.dateFrom,
        connectionParams.dateTo,
        connectionParams.prefix,
        allExtractPrefixes.length > 0 ? allExtractPrefixes : undefined,
      );
      setBlobs(data);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Unknown error");
    } finally {
      setLoading(false);
    }
  };

  // ── Step 2 → 3: move to config ─────────────────────────────────────────────
  const handleAnalyzeClick = () => {
    setError(null);
    setStep("config");
  };

  // ── Step 3: run analysis (streaming) ──────────────────────────────────────
  const handleRunAnalysis = async (config: {
    keyColumns: string[];
    filters: FilterCondition[];
    deduplicate: boolean;
    filterLogic: "AND" | "OR";
  }) => {
    setLogs([]);
    setError(null);
    setStep("analyzing");

    await analyzeStream(
      {
        container_url: containerUrl,
        selected_blobs: Array.from(selected),
        key_columns: config.keyColumns,
        filters: config.filters,
        deduplicate: config.deduplicate,
        filter_logic: config.filterLogic,
      },
      (entry) => setLogs((prev) => [...prev, entry]),
      (data) => { setResult(data); setStep("results"); },
      (message) => { setError(message); setStep("config"); }
    );
  };

  // ── Resource handlers ──────────────────────────────────────────────────────
  const handleAssignResource = async (technicalName: string, businessName: string) => {
    await createResource(technicalName, businessName);
    await refreshResources();
  };

  // ── Volumetry handler ──────────────────────────────────────────────────────
  const handleProfileResource = async (resourceId: string) => {
    const resource = resources.find((r) => r.id === resourceId);
    if (!resource || blobs.length === 0) return;

    const matchingBlobNames = (filteredBlobsByResource[resourceId] ?? []).map((b) => b.name);

    if (matchingBlobNames.length === 0) return;

    setVolumetryData((prev) => ({
      ...prev,
      [resourceId]: {
        profiles: [],
        status: "loading",
      },
    }));

    await profileBlobsStream(
      containerUrl,
      matchingBlobNames,
      (progress) => {
        setVolumetryData((prev) => ({
          ...prev,
          [resourceId]: {
            ...prev[resourceId],
            status: "loading",
            progress: {
              current: progress.current,
              total: progress.total,
              currentBlob: progress.blob_name,
            },
          },
        }));
      },
      (profile) => {
        setVolumetryData((prev) => ({
          ...prev,
          [resourceId]: {
            ...prev[resourceId],
            status: "loading",
            profiles: [...(prev[resourceId]?.profiles ?? []), profile],
          },
        }));
      },
      () => {
        setVolumetryData((prev) => ({
          ...prev,
          [resourceId]: {
            ...prev[resourceId],
            status: "loaded",
            progress: undefined,
            lastUpdated: new Date().toISOString(),
          },
        }));
      },
      (message) => {
        setVolumetryData((prev) => ({
          ...prev,
          [resourceId]: {
            status: "error",
            profiles: prev[resourceId]?.profiles ?? [],
            error: message,
            progress: undefined,
          },
        }));
      },
    );
  };

  // ── Resets ─────────────────────────────────────────────────────────────────
  const handleDisconnect = () => {
    setContainerUrl("");
    setConnectionParams(null);
    setBlobs([]);
    setSelected(new Set());
    setResult(null);
    setLogs([]);
    setError(null);
    setVolumetryData({});
    setStep("connect");
  };

  const handleNewAnalysis = () => {
    setSelected(new Set());
    setResult(null);
    setLogs([]);
    setError(null);
    setInitialConfig(undefined);
    setStep("browse");
  };

  // ── QA Example → Analysis ──────────────────────────────────────────────────
  // Called from ResourceDashboard when the user clicks "Trace →" on a QA example.
  // Pre-selects the resource blobs and configures the analysis to filter on the
  // specific record, then navigates to the config step so the user can review
  // before running.
  const handleAnalyzeQAExample = (
    _resource: Resource,
    blobNames: string[],
    idColumn: string,
    idValue: string,
  ) => {
    setSelected(new Set(blobNames));
    setInitialConfig({
      keyColumns: [idColumn],
      filters: [{ column: idColumn, value: idValue, filter_type: "equals" }],
      deduplicate: true,
      filterLogic: "AND",
    });
    setResult(null);
    setLogs([]);
    setError(null);
    setStep("config");
  };

  // ── Trace latest batch ────────────────────────────────────────────────────
  // Called from ResourceDashboard when the user clicks "Trace latest batch".
  // Pre-selects the detected batch blobs and navigates to the config step.
  const handleTraceBatch = (blobNames: string[]) => {
    setSelected(new Set(blobNames));
    setResult(null);
    setLogs([]);
    setError(null);
    setInitialConfig(undefined);
    setStep("config");
  };

  const handleLoadHistoryResult = (data: AnalyzeResultFull) => {
    setResult(data);
    setStep("results");
  };

  const handleUseAsTemplate = (config: {
    containerUrl: string;
    selectedBlobs: string[];
    keyColumns: string[];
    filters: FilterCondition[];
    deduplicate: boolean;
    filterLogic: "AND" | "OR";
  }) => {
    setInitialConfig({
      keyColumns: config.keyColumns,
      filters: config.filters,
      deduplicate: config.deduplicate,
      filterLogic: config.filterLogic,
    });
    // Pre-select the blobs if we're already connected to the same container
    if (config.containerUrl === containerUrl) {
      setSelected(new Set(config.selectedBlobs));
    }
    setStep("config");
  };

  // ── Render ─────────────────────────────────────────────────────────────────
  let content: React.ReactNode = null;

  if (step === "connect") {
    content = <ConnectionForm onConnect={handleConnect} loading={loading} error={error} />;
  } else if (step === "resources") {
    content = (
      <ResourceManager
        resources={resources}
        blobs={blobs}
        filteredBlobsByResource={filteredBlobsByResource}
        autoDateByResource={autoDateByResource}
        dateOverrides={dateOverrides}
        onDateOverridesChange={setDateOverrides}
        onCreate={async (tn, bn, ep) => { await createResource(tn, bn, ep); await refreshResources(); }}
        onUpdate={async (id, tn, bn, ep) => { await updateResource(id, tn, bn, ep); await refreshResources(); }}
        onDelete={async (id) => { await deleteResource(id); await refreshResources(); }}
        onBack={() => setStep("browse")}
        onRefreshBlobs={connectionParams ? handleRefreshBlobs : undefined}
      />
    );
  } else if (step === "dags") {
    content = <DAGManager onBack={() => setStep("browse")} />;
  } else if (step === "rules") {
    content = <IntegrationRuleManager resources={resources} onBack={() => setStep("browse")} />;
  } else if (step === "notion") {
    content = <NotionDatabase onBack={() => setStep("browse")} />;
  } else if (step === "dashboard") {
    content = (
      <ResourceDashboard
        resources={resources}
        containerUrl={containerUrl}
        onBack={() => setStep("browse")}
        onAnalyzeExample={handleAnalyzeQAExample}
        onTraceBatch={handleTraceBatch}
      />
    );
  } else if (step === "browse") {
    content = (
      <div className="min-h-screen bg-gray-50 p-6">
        <div className="max-w-screen-xl mx-auto">
          <div className="mb-4 flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold text-gray-800">Data Flow Debugger</h1>
              <p className="text-sm text-gray-500">
                {blobs.length} CSV file{blobs.length !== 1 ? "s" : ""} found · Select the
                files you want to trace across pipeline stages
              </p>
            </div>
            <div className="flex items-center gap-3">
              <button
                onClick={handleRefreshBlobs}
                disabled={loading || !connectionParams}
                className="text-sm text-gray-500 hover:text-gray-700 disabled:text-gray-300 border border-gray-200 px-3 py-1.5 rounded-lg transition-colors"
              >
                {loading ? "Refreshing…" : "↺ Refresh"}
              </button>
              {/* Unified navigation group — all sections of the tool at equal visual weight */}
              <div className="flex items-center divide-x divide-gray-200 border border-gray-200 rounded-lg overflow-hidden bg-white">
                {(
                  [
                    ["dashboard", "Dashboard"],
                    ["resources", "Resources"],
                    ["dags", "DAGs"],
                    ["rules", "Rules"],
                    ["history", "History"],
                    ["notion", "Notion"],
                  ] as [Step, string][]
                ).map(([s, label]) => (
                  <button
                    key={s}
                    onClick={() => setStep(s)}
                    className="text-sm px-3 py-1.5 text-gray-600 hover:bg-gray-50 hover:text-gray-900 transition-colors"
                  >
                    {label}
                  </button>
                ))}
              </div>
            </div>
          </div>
          <FileBrowser
            blobs={blobs}
            selected={selected}
            onSelectionChange={setSelected}
            onAnalyze={handleAnalyzeClick}
            onDisconnect={handleDisconnect}
            resources={resources}
            onAssignResource={handleAssignResource}
            onManageResources={() => setStep("resources")}
          />
        </div>
      </div>
    );
  } else if (step === "history") {
    content = (
      <AnalysisHistory
        onBack={() => setStep("browse")}
        onLoadResult={handleLoadHistoryResult}
        onUseAsTemplate={handleUseAsTemplate}
      />
    );
  } else if (step === "config") {
    content = (
      <AnalysisConfig
        containerUrl={containerUrl}
        selectedBlobs={Array.from(selected)}
        onSubmit={handleRunAnalysis}
        onBack={() => { setError(null); setStep("browse"); }}
        loading={false}
        error={error}
        initialConfig={initialConfig}
      />
    );
  } else if (step === "analyzing") {
    content = (
      <div className="min-h-screen bg-gray-950 flex flex-col items-center justify-center p-6">
        <div className="w-full max-w-2xl">
          <div className="flex items-center gap-3 mb-4">
            <div className="w-3 h-3 rounded-full bg-blue-500 animate-pulse" />
            <h2 className="text-white font-semibold text-lg">Analyzing pipeline...</h2>
          </div>
          <LogPanel entries={logs} running={true} />
          <p className="text-gray-600 text-xs mt-3 text-center">
            Files are being downloaded to a temporary folder on the server.
            You will be redirected to the results automatically.
          </p>
        </div>
      </div>
    );
  } else if (step === "results" && result) {
    content = (
      <FlowResults
        result={result}
        logs={logs}
        onBack={() => setStep("config")}
        onReset={handleNewAnalysis}
      />
    );
  }

  return (
    <>
      {content}
      <VolumetryPanel
        open={volumetryPanelOpen}
        onToggle={() => setVolumetryPanelOpen((p) => !p)}
        resources={resources}
        hasContainer={blobs.length > 0}
        filteredBlobsByResource={filteredBlobsByResource}
        volumetryData={volumetryData}
        onRefresh={handleProfileResource}
      />
    </>
  );
}
