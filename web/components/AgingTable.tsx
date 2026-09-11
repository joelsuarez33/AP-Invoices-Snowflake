import { formatAmount, formatNumber, type AgingBucket } from '@/lib/data';

export function AgingTable({ rows }: { rows: AgingBucket[] }) {
  const maxAmount = Math.max(...rows.map((r) => Math.abs(r.amount_local)), 1);

  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th className="text">Tramo</th>
            <th>Partidas</th>
            <th>Saldo</th>
            <th>Dias promedio</th>
            <th style={{ width: '28%' }}>Peso</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.aging_bucket}>
              <td className="text">{row.aging_bucket}</td>
              <td>{formatNumber(row.items)}</td>
              <td>{formatAmount(row.amount_local)}</td>
              <td>{row.avg_days_overdue}</td>
              <td>
                <span
                  className="bar"
                  style={{ width: `${(Math.abs(row.amount_local) / maxAmount) * 100}%` }}
                />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
