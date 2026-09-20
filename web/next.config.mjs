// En GitHub Pages el sitio no cuelga de la raiz del dominio sino de
// /<repo>/, asi que los assets necesitan ese prefijo. Va por variable de
// entorno para que en local (npm run dev) y en cualquier hosting que sirva
// desde la raiz quede vacio y no haya que tocar nada.
const basePath = process.env.NEXT_PUBLIC_BASE_PATH ?? '';

/** @type {import('next').NextConfig} */
const nextConfig = {
  // Export estatico: el tablero no tiene backend ni consulta Snowflake.
  // Los datos son los JSON que export_mart.py deja en public/data/.
  output: 'export',
  basePath,
  assetPrefix: basePath || undefined,
  // Pages sirve /ruta/ como /ruta/index.html.
  trailingSlash: true,
  reactStrictMode: true,
  images: { unoptimized: true },
};

export default nextConfig;
