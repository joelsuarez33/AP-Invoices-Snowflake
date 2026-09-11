import { formatAmount, formatNumber, type FxExposureRow } from '@/lib/data';

export function FxExposureTable({ rows }: { rows: FxExposureRow[] }) {
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th className="text">Moneda</th>
            <th>Abiertas</th>
            <th>Contabilizado (ARS)</th>
            <th>A cotizacion de corte (ARS)</th>
            <th>Diferencia</th>
            <th>Variacion FX</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.currency}>
              <td className="text">{row.currency}</td>
              <td>{formatNumber(row.open_items)}</td>
              <td>{formatAmount(row.amount_local_at_document_date)}</td>
              <td>{formatAmount(row.amount_local_at_cutoff)}</td>
              <td style={{ color: row.fx_revaluation_local < 0 ? 'var(--danger)' : undefined }}>
                {formatAmount(row.fx_revaluation_local)}
              </td>
              <td>{row.avg_fx_drift_pct}%</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
