import { useState } from "react";
import ConnectionForm from "../components/ConnectionForm";
import FileBrowser from "../components/FileBrowser";
import AnalysisConfig from "../components/AnalysisConfig";
import FlowResults from "../components/FlowResults";
import LogPanel from "../components/LogPanel";
import { listBlobs, analyzeStream } from "../lib/api";
import type { BlobInfo, FilterCondition, AnalyzeResultFull, LogEntry } from "../lib/api";

type Step = "connect" | "browse" | "config" | "analyzing" | "results";

export default function Home() {
  const [step, setStep] = useState<Step>("connect");
  const [containerUrl, setContainerUrl] = useState("");
  const [blobs, setBlobs] = useState<BlobInfo[]>([]);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [result, setResult] = useState<AnalyzeResultFull | null>(null);
  const [logs, setLogs] = useState<LogEntry[]>([]);

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // ── Step 1: connect ────────────────────────────────────────────────────────
  const handleConnect = async (url: string) => {
    setLoading(true);
    setError(null);
    try {
      const data = await listBlobs(url);
      setContainerUrl(url);
      setBlobs(data);
      setSelected(new Set());
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
      (data) => {
        setResult(data);
        setStep("results");
      },
      (message) => {
        setError(message);
        setStep("config");
      }
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
    setStep("connect");
  };

  // ── Render ─────────────────────────────────────────────────────────────────
  if (step === "connect") {
    return (
      <ConnectionForm onConnect={handleConnect} loading={loading} error={error} />
    );
  }

  if (step === "browse") {
    return (
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
          />
        </div>
      </div>
    );
  }

  if (step === "config") {
    return (
      <AnalysisConfig
        containerUrl={containerUrl}
        selectedBlobs={Array.from(selected)}
        onSubmit={handleRunAnalysis}
        onBack={() => { setError(null); setStep("browse"); }}
        loading={false}
        error={error}
      />
    );
  }

  if (step === "analyzing") {
    return (
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
  }

  if (step === "results" && result) {
    return (
      <FlowResults
        result={result}
        logs={logs}
        onBack={() => setStep("config")}
        onReset={handleDisconnect}
      />
    );
  }

  return null;
}
