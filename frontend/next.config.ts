import type { NextConfig } from "next";
import { Redirect } from "next/dist/lib/load-custom-routes";

const nextConfig: NextConfig = {
  pageExtensions: ["ts", "tsx"],

  // Configure internationalization (i18n)
  i18n: {
    locales: ["en-US"],
    defaultLocale: "en-US",
  },

  async redirects(): Promise<Redirect[]> {
    return [
      {
        source: "/",
        destination: "/person_match",
        permanent: false,
      },
    ];
  },

  experimental: {
    forceSwcTransforms: true,
  },
};

export default nextConfig;
