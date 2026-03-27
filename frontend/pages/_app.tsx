import type { AppProps } from "next/app";
import "../styles/globals.css";

export default function App({ Component, pageProps }: AppProps) {
  return (
    <div className="ide-shell">
      {/* Title bar */}
      <div className="ide-titlebar">
        <div className="ide-traffic-lights">
          <span style={{ background: "#ff5f57" }} />
          <span style={{ background: "#ffbd2e" }} />
          <span style={{ background: "#28c840" }} />
        </div>
        <span className="ide-title">dataflow_pipeline.py — pipeline-debugger</span>
      </div>

      {/* Body */}
      <div className="ide-body">
        {/* Activity bar */}
        <div className="ide-activity-bar">
          <div className="ide-activity-icon active" title="Explorer">
            <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6">
              <path d="M3 6h18M3 12h18M3 18h18" />
            </svg>
          </div>
          <div className="ide-activity-icon" title="Search">
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6">
              <circle cx="11" cy="11" r="7" /><line x1="17" y1="17" x2="22" y2="22" />
            </svg>
          </div>
          <div className="ide-activity-icon" title="Source Control">
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6">
              <circle cx="6" cy="6" r="2" /><circle cx="6" cy="18" r="2" /><circle cx="18" cy="12" r="2" />
              <path d="M6 8v8M8 6.5l8 4M8 17.5l8-4" />
            </svg>
          </div>
          <div className="ide-activity-icon" title="Run & Debug">
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6">
              <circle cx="12" cy="12" r="9" /><polygon points="10,8 16,12 10,16" fill="currentColor" stroke="none"/>
            </svg>
          </div>
          <div className="ide-activity-icon" title="Extensions">
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6">
              <rect x="2" y="2" width="9" height="9" rx="1" /><rect x="13" y="2" width="9" height="9" rx="1" />
              <rect x="2" y="13" width="9" height="9" rx="1" /><rect x="13" y="13" width="9" height="9" rx="1" />
            </svg>
          </div>
        </div>

        {/* Editor */}
        <div className="ide-editor">
          {/* Tabs */}
          <div className="ide-tabs">
            <div className="ide-tab active">
              <svg width="12" height="12" viewBox="0 0 24 24" fill="#3b82f6" stroke="none">
                <circle cx="12" cy="12" r="10"/>
              </svg>
              dataflow_pipeline.py
            </div>
            <div className="ide-tab">
              <svg width="12" height="12" viewBox="0 0 24 24" fill="#f59e0b" stroke="none">
                <circle cx="12" cy="12" r="10"/>
              </svg>
              config.yaml
            </div>
            <div className="ide-tab">
              <svg width="12" height="12" viewBox="0 0 24 24" fill="#6b7280" stroke="none">
                <circle cx="12" cy="12" r="10"/>
              </svg>
              utils.py
            </div>
          </div>

          {/* Content */}
          <div className="ide-content">
            <Component {...pageProps} />
          </div>
        </div>
      </div>

      {/* Status bar */}
      <div className="ide-statusbar">
        <div className="ide-statusbar-item" style={{ background: "#16825d" }}>
          <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" style={{ marginRight: 4 }}>
            <path d="M9 19c-5 1.5-5-2.5-7-3m14 6v-3.87a3.37 3.37 0 0 0-.94-2.61c3.14-.35 6.44-1.54 6.44-7A5.44 5.44 0 0 0 20 4.77 5.07 5.07 0 0 0 19.91 1S18.73.65 16 2.48a13.38 13.38 0 0 0-7 0C6.27.65 5.09 1 5.09 1A5.07 5.07 0 0 0 5 4.77a5.44 5.44 0 0 0-1.5 3.78c0 5.42 3.3 6.61 6.44 7A3.37 3.37 0 0 0 9 18.13V22" />
          </svg>
          main
        </div>
        <div className="ide-statusbar-item">
          <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" style={{ marginRight: 4 }}>
            <circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/>
          </svg>
          0 problems
        </div>
        <div className="ide-statusbar-sep" />
        <div className="ide-statusbar-item">Python 3.11.4</div>
        <div className="ide-statusbar-item">UTF-8</div>
        <div className="ide-statusbar-item">Ln 142, Col 8</div>
        <div className="ide-statusbar-item">Spaces: 4</div>
      </div>
    </div>
  );
}
