import { useEffect, useRef, useState } from "react";
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

// Only render the last N lines to keep the DOM lightweight.
// The full entry list is still used for the copy action.
const MAX_VISIBLE_LINES = 500;

interface Props {
  entries: LogEntry[];
  running: boolean;
}

export default function LogPanel({ entries, running }: Props) {
  const bottomRef = useRef<HTMLDivElement>(null);
  const [copied, setCopied] = useState(false);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [entries.length]);

  const handleCopy = () => {
    const text = entries
      .map((e) => `[${new Date(e.timestamp).toLocaleTimeString()}] [${e.level.toUpperCase()}] ${e.message}`)
      .join("\n");
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  };

  const visibleEntries = entries.length > MAX_VISIBLE_LINES
    ? entries.slice(entries.length - MAX_VISIBLE_LINES)
    : entries;
  const hiddenCount = entries.length - visibleEntries.length;

  return (
    <div className="bg-gray-950 rounded-xl font-mono text-xs overflow-hidden">
      {/* Header bar */}
      <div className="flex items-center justify-between px-4 py-1.5 bg-gray-900 border-b border-gray-800">
        <span className="text-gray-500 text-xs">
          {entries.length > 0 ? `${entries.length} line${entries.length !== 1 ? "s" : ""}` : ""}
        </span>
        {entries.length > 0 && (
          <button
            onClick={handleCopy}
            className="text-xs text-gray-500 hover:text-gray-300 transition-colors"
          >
            {copied ? "✓ copied" : "copy"}
          </button>
        )}
      </div>

      {/* Log body */}
      <div className="p-4 overflow-y-auto max-h-96 min-h-32">
        {entries.length === 0 && !running && (
          <span className="text-gray-600">No log entries yet.</span>
        )}
        {hiddenCount > 0 && (
          <div className="text-gray-600 mb-1">
            … {hiddenCount} older line{hiddenCount !== 1 ? "s" : ""} hidden (showing last {MAX_VISIBLE_LINES})
          </div>
        )}
        {visibleEntries.map((entry, i) => (
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
    </div>
  );
}
