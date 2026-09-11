import { formatAmount, formatDays, formatNumber, type PaymentPerformanceRow } from '@/lib/data';

export function PaymentPerformanceTable({ rows }: { rows: PaymentPerformanceRow[] }) {
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th className="text">Mes</th>
            <th>Compensadas</th>
            <th>Importe</th>
            <th>Dias a pagar</th>
            <th>Vs. vencimiento</th>
            <th>En termino</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.clearing_year_month}>
              <td className="text">{row.clearing_year_month}</td>
              <td>{formatNumber(row.cleared_items)}</td>
              <td>{formatAmount(row.cleared_amount_local)}</td>
              <td>{row.avg_days_to_pay}</td>
              <td>{formatDays(row.avg_days_vs_net_due)}</td>
              <td>{row.within_terms_pct}%</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
