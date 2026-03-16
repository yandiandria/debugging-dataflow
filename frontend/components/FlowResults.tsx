import { useState } from "react";
import type { AnalyzeResultFull, FlowRow, LogEntry } from "../lib/api";
import LogPanel from "./LogPanel";

const STAGE_COLORS: Record<string, string> = {
  extract: "bg-purple-500",
  transform: "bg-blue-500",
  clean_cleaned: "bg-teal-500",
  clean_incoherent: "bg-rose-500",
  compare_identify_in_flow_only: "bg-indigo-500",
  compare_identify_in_flow_and_db_different: "bg-violet-500",
  compare: "bg-yellow-500",
  clean: "bg-cyan-500",
  load: "bg-green-500",
  unknown: "bg-gray-400",
};

const STAGE_HEADER_COLORS: Record<string, string> = {
  extract: "bg-purple-50 text-purple-800 border-purple-200",
  transform: "bg-blue-50 text-blue-800 border-blue-200",
  clean_cleaned: "bg-teal-50 text-teal-800 border-teal-200",
  clean_incoherent: "bg-rose-50 text-rose-800 border-rose-200",
  compare_identify_in_flow_only: "bg-indigo-50 text-indigo-800 border-indigo-200",
  compare_identify_in_flow_and_db_different: "bg-violet-50 text-violet-800 border-violet-200",
  compare: "bg-yellow-50 text-yellow-800 border-yellow-200",
  clean: "bg-cyan-50 text-cyan-800 border-cyan-200",
  load: "bg-green-50 text-green-800 border-green-200",
  unknown: "bg-gray-50 text-gray-700 border-gray-200",
};

function formatValue(v: unknown): string {
  if (v === null || v === undefined) return "—";
  return String(v);
}

// ── Collapsible section ────────────────────────────────────────────────────────
function Disclosure({
  title,
  badge,
  defaultOpen = false,
  children,
}: {
  title: string;
  badge?: string;
  defaultOpen?: boolean;
  children: React.ReactNode;
}) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <div className="bg-white border border-gray-200 rounded-2xl overflow-hidden mb-3">
      <button
        onClick={() => setOpen((p) => !p)}
        className="w-full flex items-center justify-between px-5 py-3.5 hover:bg-gray-50 transition-colors text-left"
      >
        <div className="flex items-center gap-2.5">
          <span className="font-medium text-sm text-gray-800">{title}</span>
          {badge && (
            <span className="text-xs bg-gray-100 text-gray-500 px-2 py-0.5 rounded-full">
              {badge}
            </span>
          )}
        </div>
        <Chevron open={open} />
      </button>
      {open && <div className="px-5 pb-4 pt-1">{children}</div>}
    </div>
  );
}

function Chevron({ open }: { open: boolean }) {
  return (
    <svg
      className={`w-4 h-4 text-gray-400 transition-transform duration-200 ${open ? "rotate-180" : ""}`}
      fill="none"
      viewBox="0 0 24 24"
      stroke="currentColor"
      strokeWidth={2}
    >
      <path strokeLinecap="round" strokeLinejoin="round" d="M19 9l-7 7-7-7" />
    </svg>
  );
}

// ── Row card ───────────────────────────────────────────────────────────────────
interface RowCardProps {
  row: FlowRow;
  stages: string[];
  columns: Record<string, string[]>;
  showAllColumns: boolean;
}

function RowCard({ row, stages, columns, showAllColumns }: RowCardProps) {
  const [expanded, setExpanded] = useState(true);

  const allCols = Array.from(
    new Set(stages.flatMap((s) => (row.flow[s] ? columns[s] ?? [] : [])))
  );

  const keyLabel = Object.entries(row.key_value)
    .map(([k, v]) => `${k} = ${v}`)
    .join(" · ");

  const hasDedupWarnings = Object.keys(row.dedup_warnings).length > 0;

  return (
    <div className="bg-white border border-gray-200 rounded-2xl overflow-hidden mb-3 shadow-sm">
      <button
        onClick={() => setExpanded((p) => !p)}
        className="w-full flex items-center justify-between px-5 py-3.5 hover:bg-gray-50 transition-colors text-left"
      >
        <div className="flex items-center gap-3 flex-wrap">
          <span className="font-mono text-sm font-semibold text-gray-800">
            {keyLabel}
          </span>
          <div className="flex gap-1">
            {stages.map((s) => (
              <span
                key={s}
                title={s}
                className={`w-2 h-2 rounded-full ${
                  row.flow[s] ? STAGE_COLORS[s] ?? "bg-gray-400" : "bg-gray-200"
                }`}
              />
            ))}
          </div>
          {row.missing_stages.length > 0 && (
            <span className="text-xs text-orange-600 bg-orange-50 border border-orange-200 px-2 py-0.5 rounded-full">
              Missing: {row.missing_stages.join(", ")}
            </span>
          )}
          {row.missing_stages.length > 0 && row.last_seen_stage && (
            <span className="text-xs text-gray-500 bg-gray-100 border border-gray-200 px-2 py-0.5 rounded-full">
              Last seen: <strong>{row.last_seen_stage}</strong>
            </span>
          )}
          {hasDedupWarnings && (
            <span className="text-xs text-amber-700 bg-amber-50 border border-amber-200 px-2 py-0.5 rounded-full">
              Dedup applied
            </span>
          )}
        </div>
        <Chevron open={expanded} />
      </button>

      {expanded && hasDedupWarnings && (
        <div className="mx-5 mb-3 bg-amber-50 border border-amber-100 rounded-xl px-4 py-2 text-xs text-amber-700 flex flex-wrap gap-3">
          {Object.entries(row.dedup_warnings).map(([stage, info]) => (
            <span key={stage}>
              <strong>{stage}</strong>: {info.removed} duplicate
              {info.removed !== 1 ? "s" : ""} removed, {info.kept} kept
            </span>
          ))}
        </div>
      )}

      {expanded && (
        <div className="overflow-x-auto mx-5 mb-4 rounded-xl border border-gray-100">
          <table className="w-full text-xs border-collapse">
            <thead>
              <tr>
                <th className="px-3 py-2 text-left bg-gray-50 border-b border-r border-gray-100 font-medium text-gray-500 sticky left-0 z-10 min-w-[130px]">
                  Column
                </th>
                {stages.map((s) => (
                  <th
                    key={s}
                    className={`px-3 py-2 text-left border-b border-r font-medium min-w-[160px] ${
                      STAGE_HEADER_COLORS[s] ?? STAGE_HEADER_COLORS.unknown
                    }`}
                  >
                    <div className="flex items-center gap-1.5">
                      <span
                        className={`w-2 h-2 rounded-full flex-shrink-0 ${
                          STAGE_COLORS[s] ?? "bg-gray-400"
                        }`}
                      />
                      {s}
                      {row.missing_stages.includes(s) && (
                        <span className="text-orange-400 font-normal">(absent)</span>
                      )}
                    </div>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {allCols
                .filter(
                  (col) =>
                    showAllColumns ||
                    new Set(
                      stages.map((s) =>
                        row.flow[s] ? formatValue(row.flow[s][col]) : "__absent__"
                      )
                    ).size > 1
                )
                .map((col, i) => (
                  <tr key={col} className={i % 2 === 0 ? "bg-white" : "bg-gray-50/50"}>
                    <td className="px-3 py-1.5 font-mono text-gray-500 border-r border-gray-100 sticky left-0 bg-inherit font-medium">
                      {col}
                    </td>
                    {stages.map((s) => {
                      const val = row.flow[s] ? formatValue(row.flow[s][col]) : null;
                      const isAbsent = !row.flow[s];
                      return (
                        <td
                          key={s}
                          className={`px-3 py-1.5 border-r border-gray-100 font-mono ${
                            isAbsent
                              ? "text-gray-300 italic"
                              : val === "—"
                              ? "text-gray-300"
                              : "text-gray-800"
                          }`}
                        >
                          {isAbsent ? "absent" : val}
                        </td>
                      );
                    })}
                  </tr>
                ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// ── Main component ─────────────────────────────────────────────────────────────
interface Props {
  result: AnalyzeResultFull;
  logs: LogEntry[];
  onBack: () => void;
  onReset: () => void;
}

export default function FlowResults({ result, logs, onBack, onReset }: Props) {
  const [showAllColumns, setShowAllColumns] = useState(false);
  const [rowSearch, setRowSearch] = useState("");

  const globalDedupStages = Object.keys(result.dedup_warnings);

  const filteredRows = result.rows.filter((row) => {
    const label = Object.entries(row.key_value)
      .map(([k, v]) => `${k}=${v}`)
      .join(" ")
      .toLowerCase();
    return label.includes(rowSearch.toLowerCase());
  });

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Top bar */}
      <div className="bg-white/80 backdrop-blur border-b border-gray-200 px-6 py-3 flex items-center justify-between flex-wrap gap-2 sticky top-0 z-20">
        <div className="flex items-center gap-3">
          <button
            onClick={onBack}
            className="text-sm text-gray-400 hover:text-gray-700 transition-colors"
          >
            ←
          </button>
          <span className="text-gray-200">|</span>
          <h1 className="text-sm font-semibold text-gray-800">Flow Results</h1>
          <span className="text-xs text-gray-400 bg-gray-100 px-2 py-0.5 rounded-full">
            {result.rows.length} record{result.rows.length !== 1 ? "s" : ""}
          </span>
        </div>
        <div className="flex items-center gap-4">
          <div className="flex items-center gap-2">
            {result.stages.map((s) => (
              <span key={s} className="flex items-center gap-1 text-xs text-gray-500">
                <span className={`w-2 h-2 rounded-full ${STAGE_COLORS[s] ?? "bg-gray-400"}`} />
                {s}
              </span>
            ))}
          </div>
          <button
            onClick={onReset}
            className="text-xs text-gray-500 hover:text-gray-700 border border-gray-200 rounded-lg px-3 py-1.5 transition-colors"
          >
            New analysis
          </button>
        </div>
      </div>

      <div className="max-w-screen-xl mx-auto px-6 py-6">
        {/* Global dedup banner */}
        {globalDedupStages.length > 0 && (
          <div className="mb-4 bg-amber-50 border border-amber-200 rounded-2xl px-5 py-3 text-sm text-amber-800">
            <span className="font-semibold">Deduplication applied</span> in{" "}
            {globalDedupStages
              .map((s) => {
                const info = result.dedup_warnings[s];
                return `${s} (${info.removed} row${info.removed !== 1 ? "s" : ""} removed)`;
              })
              .join(", ")}
            . First occurrence per key was kept.
          </div>
        )}

        {/* ── Analysis log ── */}
        <Disclosure title="Analysis log" badge={`${logs.length} entries`}>
          <LogPanel entries={logs} running={false} />
        </Disclosure>

        {/* ── Downloaded files ── */}
        <Disclosure
          title="Downloaded files"
          badge={`${result.downloaded_files.length} file${result.downloaded_files.length !== 1 ? "s" : ""}`}
        >
          <p className="text-xs text-gray-400 mb-3">
            Stored in <code className="bg-gray-100 px-1.5 py-0.5 rounded text-gray-600">{result.tmp_dir}</code> on the server.
            Files persist until you manually delete them.
          </p>
          <div className="space-y-1.5">
            {result.downloaded_files.map((path) => (
              <div
                key={path}
                className="flex items-center gap-2 bg-gray-50 border border-gray-100 rounded-xl px-3 py-2"
              >
                <span className="text-gray-300 text-base">📄</span>
                <code className="text-xs text-gray-600 font-mono break-all">{path}</code>
              </div>
            ))}
          </div>
        </Disclosure>

        {/* ── Records ── */}
        <div className="flex items-center justify-between mb-4 mt-2 flex-wrap gap-2">
          <input
            type="text"
            placeholder={`Search by ${result.key_columns.join(", ")}…`}
            value={rowSearch}
            onChange={(e) => setRowSearch(e.target.value)}
            className="border border-gray-200 bg-white rounded-xl px-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 w-64 shadow-sm"
          />
          <label className="flex items-center gap-2 text-sm text-gray-500 cursor-pointer select-none">
            <input
              type="checkbox"
              checked={showAllColumns}
              onChange={(e) => setShowAllColumns(e.target.checked)}
              className="rounded"
            />
            Show all columns
          </label>
        </div>

        {filteredRows.length === 0 ? (
          <div className="text-center text-gray-400 py-20 text-sm">
            No records match your search.
          </div>
        ) : (
          filteredRows.map((row, i) => (
            <RowCard
              key={i}
              row={row}
              stages={result.stages}
              columns={result.columns}
              showAllColumns={showAllColumns}
            />
          ))
        )}
      </div>
    </div>
  );
}
