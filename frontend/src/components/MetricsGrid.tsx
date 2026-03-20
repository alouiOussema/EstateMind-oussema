import { useEffect, useState } from "react";
import { BarChart3, Download, AlertTriangle, Database } from "lucide-react";

interface Metrics {
  total_listings: number;
  last_run_fetched: number;
  inserted: number;
  errors: number;
}

const DEFAULT_METRICS: Metrics = {
  total_listings: 0,
  last_run_fetched: 0,
  inserted: 0,
  errors: 0,
};

export function MetricsGrid() {
  const [metrics, setMetrics] = useState<Metrics>(DEFAULT_METRICS);

  useEffect(() => {
    fetch("/api/metrics/", { credentials: "include" })
      .then((r) => (r.ok ? r.json() : null))
      .then((data) => {
        if (data) {
          setMetrics({
            total_listings: data.total_listings || 0,
            last_run_fetched: data.latest_run?.fetched || 0,
            inserted: data.latest_run?.inserted || 0,
            errors: data.latest_run?.errors || 0,
          });
        }
      })
      .catch(() => {});
  }, []);

  const cards = [
    {
      label: "Total Listings",
      value: metrics.total_listings.toLocaleString(),
      icon: Database,
      accent: "text-primary",
      bgAccent: "bg-primary/10",
    },
    {
      label: "Last Run Fetched",
      value: metrics.last_run_fetched.toLocaleString(),
      icon: Download,
      accent: "text-accent-foreground",
      bgAccent: "bg-accent",
    },
    {
      label: "Inserted",
      value: metrics.inserted.toLocaleString(),
      icon: BarChart3,
      accent: "text-success",
      bgAccent: "bg-success/10",
    },
    {
      label: "Errors",
      value: metrics.errors.toLocaleString(),
      icon: AlertTriangle,
      accent: "text-destructive",
      bgAccent: "bg-destructive/10",
    },
  ];

  return (
    <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
      {cards.map((card, i) => (
        <div
          key={card.label}
          className="glass-panel rounded-xl p-5 animate-slide-up"
          style={{ animationDelay: `${i * 80}ms`, animationFillMode: "both" }}
        >
          <div className="flex items-center justify-between">
            <span className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
              {card.label}
            </span>
            <div className={`flex h-8 w-8 items-center justify-center rounded-lg ${card.bgAccent}`}>
              <card.icon className={`h-4 w-4 ${card.accent}`} />
            </div>
          </div>
          <p className="mt-3 text-3xl font-bold tracking-tight text-foreground">{card.value}</p>
        </div>
      ))}
    </div>
  );
}
