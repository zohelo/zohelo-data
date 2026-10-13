import { useEffect, useMemo, useState } from "react";
import { Input } from "@/components/ui/input";
import {
  fetchDataCatalog,
  type CatalogDimension,
  type CatalogMetric,
  type CatalogSource,
  type CatalogTable,
  type DataCatalog,
} from "@/services/googleDrive/dataCatalog";

type Section = "metrics" | "tables" | "dimensions" | "sources";

const SECTIONS: { id: Section; label: string }[] = [
  { id: "metrics", label: "Metrics" },
  { id: "tables", label: "Tables" },
  { id: "dimensions", label: "Dimensions" },
  { id: "sources", label: "Bronze sources" },
];

const matches = (query: string, ...values: (string | null | undefined)[]) =>
  !query || values.some((value) => (value ?? "").toLowerCase().includes(query));

const shortId = (id: string) => id.split(".").slice(-2).join(".");

function TableItem({ item }: { item: CatalogTable }) {
  return (
    <details className="rounded border px-3 py-2">
      <summary className="cursor-pointer text-sm font-medium">
        {item.table}
        <span className="ml-2 text-xs text-muted-foreground">{item.columns.length} columns</span>
      </summary>
      {item.description && <p className="mt-2 text-xs text-muted-foreground">{item.description}</p>}
      {item.columns.length > 0 && (
        <ul className="mt-2 space-y-1 text-xs">
          {item.columns.map((column) => (
            <li key={column.name}>
              <span className="font-mono">{column.name}</span>
              {column.description && <span className="text-muted-foreground"> — {column.description}</span>}
            </li>
          ))}
        </ul>
      )}
      <div className="mt-2 space-y-1 text-xs text-muted-foreground">
        {item.upstream.length > 0 && <div>Built from: {item.upstream.map(shortId).join(", ")}</div>}
        {item.downstream.length > 0 && <div>Used by: {item.downstream.map(shortId).join(", ")}</div>}
        {item.tests.length > 0 && <div>Tests: {item.tests.join(", ")}</div>}
      </div>
    </details>
  );
}

function MetricItem({ item }: { item: CatalogMetric }) {
  return (
    <div className="rounded border px-3 py-2">
      <div className="text-sm font-medium">{item.label || item.name}</div>
      <div className="font-mono text-xs text-muted-foreground">{item.name}</div>
      {item.description && <p className="mt-1 text-xs text-muted-foreground">{item.description}</p>}
    </div>
  );
}

function DimensionItem({ item }: { item: CatalogDimension }) {
  return (
    <details className="rounded border px-3 py-2">
      <summary className="cursor-pointer text-sm font-medium">
        {item.name}
        <span className="ml-2 text-xs text-muted-foreground">
          {[item.kind, item.status].filter(Boolean).join(" · ")}
        </span>
      </summary>
      <p className="mt-2 text-xs text-muted-foreground">{item.description}</p>
      {item.grain && <div className="mt-1 text-xs">Grain: {item.grain}</div>}
      {item.used_by.length > 0 && <div className="mt-1 text-xs">Sources: {item.used_by.join(", ")}</div>}
      <div className="mt-1 font-mono text-xs text-muted-foreground">{item.registry}</div>
    </details>
  );
}

function SourceItem({ item }: { item: CatalogSource }) {
  return (
    <div className="rounded border px-3 py-2">
      <div className="text-sm font-medium">{item.table}</div>
      <div className="text-xs text-muted-foreground">{item.source_system}</div>
      {item.description && <p className="mt-1 text-xs text-muted-foreground">{item.description}</p>}
      {item.downstream.length > 0 && (
        <div className="mt-1 text-xs text-muted-foreground">Used by: {item.downstream.map(shortId).join(", ")}</div>
      )}
    </div>
  );
}

export default function CatalogPanel() {
  const [catalog, setCatalog] = useState<DataCatalog | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [section, setSection] = useState<Section>("metrics");
  const [query, setQuery] = useState("");

  useEffect(() => {
    const controller = new AbortController();
    fetchDataCatalog(controller.signal)
      .then(setCatalog)
      .catch((reason: unknown) => {
        if (!controller.signal.aborted) setError(reason instanceof Error ? reason.message : String(reason));
      });
    return () => controller.abort();
  }, []);

  const q = query.trim().toLowerCase();
  const items = useMemo(() => {
    if (!catalog) return null;
    return {
      metrics: catalog.metrics.filter((m) => matches(q, m.name, m.label, m.description)),
      tables: catalog.tables.filter((t) =>
        matches(q, t.table, t.description, ...t.columns.map((c) => `${c.name} ${c.description}`))),
      dimensions: catalog.dimensions.filter((d) => matches(q, d.name, d.description, d.kind)),
      sources: catalog.sources.filter((s) => matches(q, s.table, s.source_system, s.description)),
    };
  }, [catalog, q]);

  if (error) return <div className="p-4 text-sm text-destructive">{error}</div>;
  if (!catalog || !items) return <div className="p-4 text-sm text-muted-foreground">Loading the catalog…</div>;

  return (
    <div className="flex h-full flex-col gap-3 p-4">
      <Input
        value={query}
        onChange={(event) => setQuery(event.target.value)}
        placeholder="Search metrics, tables, columns, dimensions…"
        aria-label="Search the data catalog"
      />
      <div className="flex flex-wrap gap-1">
        {SECTIONS.map(({ id, label }) => (
          <button
            key={id}
            type="button"
            onClick={() => setSection(id)}
            className={`rounded px-2 py-1 text-xs ${section === id ? "bg-secondary font-medium" : "text-muted-foreground hover:bg-muted"}`}
          >
            {label} ({items[id].length})
          </button>
        ))}
      </div>
      <div className="min-h-0 flex-1 space-y-2 overflow-auto">
        {section === "metrics" && items.metrics.map((m) => <MetricItem key={m.name} item={m} />)}
        {section === "tables" && items.tables.map((t) => <TableItem key={t.id} item={t} />)}
        {section === "dimensions" && items.dimensions.map((d) => <DimensionItem key={d.name} item={d} />)}
        {section === "sources" && items.sources.map((s) => <SourceItem key={s.id} item={s} />)}
      </div>
      <div className="text-xs text-muted-foreground">Catalog generated {catalog.generated_at}</div>
    </div>
  );
}
