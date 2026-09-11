import { AgingTable } from '@/components/AgingTable';
import { FxExposureTable } from '@/components/FxExposureTable';
import { Kpis } from '@/components/Kpis';
import { PaymentPerformanceTable } from '@/components/PaymentPerformanceTable';
import { VendorTable } from '@/components/VendorTable';
import { loadDashboard } from '@/lib/data';

export default async function Page() {
  const { summary, aging, vendors, fxExposure, paymentPerformance, meta } = await loadDashboard();

  if (!summary) {
    return (
      <main>
        <header className="page">
          <h1>Cuentas a pagar</h1>
          <p>Ledger de acreedores &mdash; sin datos publicados</p>
        </header>
        <div className="empty">
          <p>
            Todavia no hay datasets en <code>web/public/data/</code>. Los genera el ultimo paso del
            pipeline:
          </p>
          <p>
            <code>python export_mart.py</code>
          </p>
        </div>
      </main>
    );
  }

  return (
    <main>
      <header className="page">
        <h1>Cuentas a pagar</h1>
        <p>
          Ledger de acreedores al <strong>{summary.ledger_cutoff_date}</strong>. Importes en pesos:
          el ledger opera en tres monedas y solo el importe en moneda local es agregable.
        </p>
      </header>

      <section>
        <Kpis summary={summary} />
      </section>

      <section>
        <h2>Antiguedad de la deuda</h2>
        <p className="hint">
          Calculada contra la fecha del ultimo extracto, no contra la fecha de hoy: el mismo corte
          consultado dentro de un mes devuelve los mismos numeros. El tramo &ldquo;No vencida&rdquo;
          tiene dias negativos porque incluye partidas que todavia no llegaron a su vencimiento.
        </p>
        <AgingTable rows={aging} />
      </section>

      <section>
        <h2>Exposicion cambiaria</h2>
        <p className="hint">
          Diferencia entre el importe con el que se contabilizo cada factura en moneda extranjera y
          lo que representa la misma obligacion a la cotizacion de la fecha de corte. No aparece en
          el extracto de origen.
        </p>
        {fxExposure.length > 0 ? (
          <FxExposureTable rows={fxExposure} />
        ) : (
          <div className="empty">No hay partidas abiertas en moneda extranjera.</div>
        )}
      </section>

      <section>
        <h2>Concentracion por acreedor</h2>
        <p className="hint">Los 25 acreedores con mayor saldo pendiente.</p>
        <VendorTable rows={vendors} />
      </section>

      <section>
        <h2>Comportamiento de pago</h2>
        <p className="hint">
          Dias reales entre la fecha del documento y la compensacion, y desvio respecto del
          vencimiento neto. Negativo en &ldquo;Vs. vencimiento&rdquo; significa que se pago antes de
          vencer.
        </p>
        <PaymentPerformanceTable rows={paymentPerformance} />
      </section>

      <footer className="page">
        Datos generados el {meta?.generated_at ?? '--'} por <code>export_mart.py</code>. Origen:
        change feed diario de la transaccion FBL1N, ingestado append-only en Snowflake y modelado
        con dbt.
      </footer>
    </main>
  );
}
