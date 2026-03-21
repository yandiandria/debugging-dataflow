import { useState, useEffect } from "react";
import ConnectionForm from "../components/ConnectionForm";
import FileBrowser from "../components/FileBrowser";
import AnalysisConfig from "../components/AnalysisConfig";
import FlowResults from "../components/FlowResults";
import LogPanel from "../components/LogPanel";
import ResourceManager from "../components/ResourceManager";
import VolumetryPanel from "../components/VolumetryPanel";
import type { VolumetryEntry } from "../components/VolumetryPanel";
import {
  listBlobs,
  analyzeStream,
  getResources,
  createResource,
  updateResource,
  deleteResource,
  profileBlobsStream,
} from "../lib/api";
import type { BlobInfo, FilterCondition, AnalyzeResultFull, LogEntry, Resource } from "../lib/api";

type Step = "connect" | "browse" | "config" | "analyzing" | "results" | "resources";

export default function Home() {
  const [step, setStep] = useState<Step>("connect");
  const [containerUrl, setContainerUrl] = useState("");
  const [blobs, setBlobs] = useState<BlobInfo[]>([]);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [result, setResult] = useState<AnalyzeResultFull | null>(null);
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [resources, setResources] = useState<Resource[]>([]);

  // Volumetry panel state — cleared when container changes
  const [volumetryPanelOpen, setVolumetryPanelOpen] = useState(false);
  const [volumetryData, setVolumetryData] = useState<Record<string, VolumetryEntry>>({});

  // Load resources once on mount
  useEffect(() => {
    getResources().then(setResources).catch(() => {});
  }, []);

  const refreshResources = () => getResources().then(setResources).catch(() => {});

  // ── Step 1: connect ────────────────────────────────────────────────────────
  const handleConnect = async (url: string, dateFrom?: string, dateTo?: string, prefix?: string) => {
    setLoading(true);
    setError(null);
    try {
      const data = await listBlobs(url, dateFrom, dateTo, prefix);
      setContainerUrl(url);
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

    const matchingBlobNames = blobs
      .filter((b) => b.name.startsWith(resource.technical_name))
      .map((b) => b.name);

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
    setStep("browse");
  };

  // ── Render ─────────────────────────────────────────────────────────────────
  let content: React.ReactNode = null;

  if (step === "connect") {
    content = <ConnectionForm onConnect={handleConnect} loading={loading} error={error} />;
  } else if (step === "resources") {
    content = (
      <ResourceManager
        resources={resources}
        onCreate={async (tn, bn) => { await createResource(tn, bn); await refreshResources(); }}
        onUpdate={async (id, tn, bn) => { await updateResource(id, tn, bn); await refreshResources(); }}
        onDelete={async (id) => { await deleteResource(id); await refreshResources(); }}
        onBack={() => setStep("browse")}
      />
    );
  } else if (step === "browse") {
    content = (
      <div className="min-h-screen bg-gray-50 p-6">
        <div className="max-w-screen-xl mx-auto">
          <div className="mb-4">
            <h1 className="text-2xl font-bold text-gray-800">Data Flow Debugger</h1>
            <p className="text-sm text-gray-500">
              {blobs.length} CSV file{blobs.length !== 1 ? "s" : ""} found · Select the
              files you want to trace across pipeline stages
            </p>
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
  } else if (step === "config") {
    content = (
      <AnalysisConfig
        containerUrl={containerUrl}
        selectedBlobs={Array.from(selected)}
        onSubmit={handleRunAnalysis}
        onBack={() => { setError(null); setStep("browse"); }}
        loading={false}
        error={error}
      />
    );
  } else if (step === "analyzing") {
    content = (
      <div className="min-h-screen bg-gray-950 flex flex-col items-center justify-center p-6">
        <div className="w-full max-w-2xl">
          <div className="flex items-center gap-3 mb-4">
            <div className="w-3 h-3 rounded-full bg-blue-500 animate-pulse" />
            <h2 className="text-white font-semibold text-lg">Analyzing pipeline…</h2>
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
        blobs={blobs}
        volumetryData={volumetryData}
        onRefresh={handleProfileResource}
      />
    </>
  );
}
