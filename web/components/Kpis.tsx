import { formatAmount, formatNumber, type Summary } from '@/lib/data';

export function Kpis({ summary }: { summary: Summary }) {
  const overduePct =
    summary.items_open > 0 ? (summary.items_overdue / summary.items_open) * 100 : 0;

  /* fx_revaluation_local negativo significa que el pasivo en moneda local
     crecio desde que se contabilizaron las facturas: es una perdida. */
  const fxIsLoss = summary.fx_revaluation_local < 0;

  return (
    <div className="kpi-grid">
      <div className="card">
        <div className="label">Partidas abiertas</div>
        <div className="value">{formatNumber(summary.items_open)}</div>
        <div className="footnote">
          de {formatNumber(summary.items_total)} documentos en el ledger
        </div>
      </div>

      <div className="card">
        <div className="label">Saldo pendiente</div>
        <div className="value">{formatAmount(summary.open_amount_local)}</div>
        <div className="footnote">
          {formatNumber(summary.vendors_with_items)} acreedores con movimiento
        </div>
      </div>

      <div className="card">
        <div className="label">Vencido</div>
        <div className="value warn">{formatAmount(summary.overdue_amount_local)}</div>
        <div className="footnote">
          {formatNumber(summary.items_overdue)} partidas ({overduePct.toFixed(1)}% de las abiertas)
        </div>
      </div>

      <div className="card">
        <div className="label">Exposicion cambiaria</div>
        <div className={fxIsLoss ? 'value danger' : 'value'}>
          {formatAmount(summary.fx_revaluation_local)}
        </div>
        <div className="footnote">
          {fxIsLoss ? 'mayor pasivo' : 'menor pasivo'} en ARS por partidas abiertas en USD y EUR
        </div>
      </div>
    </div>
  );
}
