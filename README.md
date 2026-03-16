# Data Flow Debugger

A local web app for data engineers to trace how individual records flow through
a multi-stage CSV pipeline stored in Azure Blob Storage.

## Pipeline stages detected (in order)

| Keyword in filename | Stage     |
|---------------------|-----------|
| `extract`           | Extract   |
| `transform`         | Transform |
| `clean`             | Clean     |
| `compare`           | Compare   |
| `load`              | Load      |

Multiple batch files per stage are automatically concatenated.

---

## Architecture

```
dataflow-debugger/
├── backend/       FastAPI + azure-storage-blob
└── frontend/      Next.js 14 + Tailwind CSS
```

---

## Running locally

### Backend

Dependencies are managed with [uv](https://docs.astral.sh/uv/) (install once with `curl -LsSf https://astral.sh/uv/install.sh | sh`).

```bash
cd dataflow-debugger/backend
uv sync                              # creates .venv and installs all deps
uv run uvicorn main:app --reload --port 8000
```

To add a new dependency:
```bash
uv add <package>         # production
uv add --dev <package>   # dev-only (e.g. pytest plugins)
```

### Frontend

```bash
cd dataflow-debugger/frontend
npm install
npm run dev          # starts on http://localhost:3001
```

Set `NEXT_PUBLIC_API_URL=http://localhost:8000` in `.env.local` if you want
to use a different backend URL (defaults to `http://localhost:8000`).

---

## How to use

1. **Connect** — Paste your Azure container URL with its SAS token.
   The SAS token is sent directly to Azure; it never goes through any
   intermediate server.

2. **Browse** — All CSV files in the container are listed with their
   auto-detected stage. Use the search box and stage filter to narrow down,
   then tick the files you want to include.

3. **Configure** — Pick the key column(s) that identify a record across
   stages (e.g. `sourceId`). Optionally add column = value filters.
   Enable deduplication to collapse duplicate keys per stage (you will be
   warned whenever this happens).

4. **Results** — A side-by-side table per stage for every matching record.
   - Colour-coded stage headers
   - Absent-stage badges (record not found at that stage)
   - Amber deduplication warning banner with counts
   - Toggle between "changed columns only" and "all columns"
   - Search records by key value
