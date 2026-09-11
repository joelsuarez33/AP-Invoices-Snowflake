import { formatAmount, formatDays, formatNumber, type VendorRow } from '@/lib/data';

export function VendorTable({ rows }: { rows: VendorRow[] }) {
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th className="text">Acreedor</th>
            <th className="text">Pais</th>
            <th className="text">Condicion</th>
            <th>Abiertas</th>
            <th>Saldo</th>
            <th>Vencido</th>
            <th>Dias prom.</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.lifnr}>
              <td className="text">
                {row.vendor_name} <span style={{ opacity: 0.5 }}>({row.lifnr})</span>
              </td>
              <td className="text">{row.country}</td>
              <td className="text">{row.payment_terms}</td>
              <td>{formatNumber(row.open_items)}</td>
              <td>{formatAmount(row.open_amount_local)}</td>
              <td>{formatAmount(row.overdue_amount_local)}</td>
              <td>{formatDays(row.avg_days_overdue)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
