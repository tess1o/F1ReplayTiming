/** @type {import('next').NextConfig} */
const backendOrigin = process.env.BACKEND_INTERNAL_URL
  || (process.env.NODE_ENV === "development" ? "http://localhost:8000" : "http://backend:8000");

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
