import { useEffect, useRef } from "react";
import type { LogEntry } from "../lib/api";

const LEVEL_COLOR: Record<LogEntry["level"], string> = {
  info: "text-gray-300",
  success: "text-green-400",
  warning: "text-amber-400",
  error: "text-red-400",
};

const LEVEL_ICON: Record<LogEntry["level"], string> = {
  info: "·",
  success: "✓",
  warning: "⚠",
  error: "✕",
};

interface Props {
  entries: LogEntry[];
  running: boolean;
}

export default function LogPanel({ entries, running }: Props) {
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [entries.length]);

  return (
    <div className="bg-gray-950 rounded-xl p-4 font-mono text-xs overflow-y-auto max-h-96 min-h-32">
      {entries.length === 0 && !running && (
        <span className="text-gray-600">No log entries yet.</span>
      )}
      {entries.map((entry, i) => (
        <div key={i} className={`flex gap-2 mb-0.5 leading-5 ${LEVEL_COLOR[entry.level]}`}>
          <span className="text-gray-600 flex-shrink-0 tabular-nums">
            {new Date(entry.timestamp).toLocaleTimeString()}
          </span>
          <span className="flex-shrink-0 w-3">{LEVEL_ICON[entry.level]}</span>
          <span className="break-all">{entry.message}</span>
        </div>
      ))}
      {running && (
        <div className="flex gap-2 items-center text-gray-500 mt-1">
          <span className="animate-pulse">●</span>
          <span>Running…</span>
        </div>
      )}
      <div ref={bottomRef} />
    </div>
  );
}
