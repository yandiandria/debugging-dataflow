import { useState, useEffect } from "react";

const STORAGE_KEY = "dataflow_container_url";

interface Props {
  onConnect: (containerUrl: string) => void;
  loading: boolean;
  error: string | null;
}

export default function ConnectionForm({ onConnect, loading, error }: Props) {
  const [containerUrl, setContainerUrl] = useState(() =>
    typeof window !== "undefined"
      ? localStorage.getItem(STORAGE_KEY) ?? ""
      : ""
  );

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const url = containerUrl.trim();
    if (url) {
      localStorage.setItem(STORAGE_KEY, url);
      onConnect(url);
    }
  };

  const handleClear = () => {
    localStorage.removeItem(STORAGE_KEY);
    setContainerUrl("");
  };

  return (
    <div className="min-h-screen bg-gray-50 flex items-center justify-center p-4">
      <div className="bg-white rounded-2xl shadow-lg p-8 w-full max-w-lg">
        <h1 className="text-2xl font-bold text-gray-800 mb-1">Data Flow Debugger</h1>
        <p className="text-sm text-gray-500 mb-6">
          Connect to your Azure Blob Storage container to browse and analyze pipeline files.
        </p>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Container URL (with SAS token)
            </label>
            <textarea
              className="w-full border border-gray-300 rounded-lg p-3 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-blue-500 resize-none"
              rows={4}
              placeholder="https://<account>.blob.core.windows.net/<container>?sv=...&sig=..."
              value={containerUrl}
              onChange={(e) => setContainerUrl(e.target.value)}
            />
            <div className="flex items-center justify-between mt-1">
              <p className="text-xs text-gray-400">
                Saved in your browser. Never sent anywhere except Azure.
              </p>
              {containerUrl && (
                <button
                  type="button"
                  onClick={handleClear}
                  className="text-xs text-gray-400 hover:text-red-500 transition-colors"
                >
                  Clear saved URL
                </button>
              )}
            </div>
          </div>

          {error && (
            <div className="bg-red-50 border border-red-200 rounded-lg p-3 text-sm text-red-700">
              {error}
            </div>
          )}

          <button
            type="submit"
            disabled={loading || !containerUrl.trim()}
            className="w-full bg-blue-600 hover:bg-blue-700 disabled:bg-blue-300 text-white font-medium py-2.5 rounded-lg transition-colors"
          >
            {loading ? "Connecting…" : "Connect"}
          </button>
        </form>
      </div>
    </div>
  );
}
