/** @type {import('next').NextConfig} */
const nextConfig = {
  // Export estatico: el tablero no tiene backend ni consulta Snowflake.
  // Los datos son los JSON que export_mart.py deja en public/data/.
  output: 'export',
  reactStrictMode: true,
  images: { unoptimized: true },
};

export default nextConfig;
