import { promises as fs } from 'node:fs';
import path from 'node:path';

/*
  Los datasets se leen del filesystem en tiempo de build. El tablero es un
  export estatico: no hay fetch en runtime, no hay credenciales en el cliente y
  no hay nada que despierte el warehouse cuando alguien abre la pagina.

  Si un archivo no existe -- un clone limpio en el que todavia no corrio
  export_mart.py -- la lectura devuelve null y la pagina muestra el estado
  vacio. El build no falla por falta de datos.
*/

const DATA_DIR = path.join(process.cwd(), 'public', 'data');

export type Summary = {
  ledger_cutoff_date: string;
  items_total: number;
  items_open: number;
  items_cleared: number;
  open_amount_local: number;
  overdue_amount_local: number;
  items_overdue: number;
  fx_revaluation_local: number;
  vendors_with_items: number;
};

export type AgingBucket = {
  aging_bucket: string;
  aging_bucket_sort: number;
  items: number;
  amount_local: number;
  avg_days_overdue: number;
};

export type VendorRow = {
  lifnr: string;
  vendor_name: string;
  country: string;
  payment_terms: string;
  open_items: number;
  open_amount_local: number;
  overdue_amount_local: number;
  avg_days_overdue: number;
  fx_revaluation_local: number;
};

export type FxExposureRow = {
  currency: string;
  open_items: number;
  amount_document_currency: number;
  amount_local_at_document_date: number;
  amount_local_at_cutoff: number;
  fx_revaluation_local: number;
  avg_fx_drift_pct: number;
  fx_rate_at_cutoff: number;
};

export type PaymentPerformanceRow = {
  clearing_year_month: string;
  cleared_items: number;
  cleared_amount_local: number;
  avg_days_to_pay: number;
  avg_days_vs_net_due: number;
  within_terms_pct: number;
};

export type Meta = {
  generated_at: string;
  ledger_cutoff_date: string | null;
  local_currency: string;
};

async function readDataset<T>(name: string): Promise<T | null> {
  try {
    const raw = await fs.readFile(path.join(DATA_DIR, `${name}.json`), 'utf8');
    return JSON.parse(raw) as T;
  } catch {
    return null;
  }
}

export async function loadDashboard() {
  const [summary, aging, vendors, fxExposure, paymentPerformance, meta] = await Promise.all([
    readDataset<Summary>('summary'),
    readDataset<AgingBucket[]>('aging'),
    readDataset<VendorRow[]>('vendors'),
    readDataset<FxExposureRow[]>('fx_exposure'),
    readDataset<PaymentPerformanceRow[]>('payment_performance'),
    readDataset<Meta>('meta'),
  ]);

  return {
    summary,
    aging: aging ?? [],
    vendors: vendors ?? [],
    fxExposure: fxExposure ?? [],
    paymentPerformance: paymentPerformance ?? [],
    meta,
  };
}

const currencyFormatter = new Intl.NumberFormat('es-AR', {
  style: 'currency',
  currency: 'ARS',
  maximumFractionDigits: 0,
});

const numberFormatter = new Intl.NumberFormat('es-AR');

/* Los importes del ledger son negativos cuando acreditan al proveedor. Para
   leer un saldo a pagar se muestra la magnitud y el signo se explica aparte. */
export function formatAmount(value: number | null | undefined): string {
  if (value === null || value === undefined) return '--';
  return currencyFormatter.format(Math.abs(value));
}

export function formatNumber(value: number | null | undefined): string {
  if (value === null || value === undefined) return '--';
  return numberFormatter.format(value);
}

export function formatDays(value: number | null | undefined): string {
  if (value === null || value === undefined) return '--';
  return `${value > 0 ? '+' : ''}${value}`;
}
