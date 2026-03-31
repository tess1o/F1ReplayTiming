/** @type {import('next').NextConfig} */
const INTERNAL_PLACEHOLDER_URL = "http://__BACKEND_INTERNAL_URL__";
const backendOrigin = process.env.BACKEND_INTERNAL_URL
  || (process.env.NODE_ENV === "development" ? "http://localhost:8000" : INTERNAL_PLACEHOLDER_URL);

const nextConfig = {
  output: "standalone",
  async rewrites() {
    return [
      {
        source: "/api/:path*",
        destination: `${backendOrigin}/api/:path*`,
      },
      {
        source: "/ws/:path*",
        destination: `${backendOrigin}/ws/:path*`,
      },
    ];
  },
};

module.exports = nextConfig;
