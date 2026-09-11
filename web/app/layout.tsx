import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'Cuentas a pagar | Ledger de acreedores',
  description:
    'Tablero estatico del ledger de acreedores construido sobre el change feed diario de FBL1N.',
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="es">
      <body>{children}</body>
    </html>
  );
}
