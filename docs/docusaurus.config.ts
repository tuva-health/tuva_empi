import { themes as prismThemes } from "prism-react-renderer";
import type { Config } from "@docusaurus/types";
import type * as Preset from "@docusaurus/preset-classic";

// This runs in Node.js - Don't use client-side code here (browser APIs, JSX...)

const config: Config = {
  title: "Tuva EMPI Docs",
  tagline: "",
  favicon: "img/favicon.ico",

  // Set the production url of your site here
  url: "https://your-docusaurus-site.example.com",
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: "/tuva_empi/",

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: "tuva-health", // Usually your GitHub org/user name.
  projectName: "tuva_empi", // Usually your repo name.

  onBrokenLinks: "throw",
  onBrokenMarkdownLinks: "warn",

  // Even if you don't use internationalization, you can use this field to set
  // useful metadata like html lang. For example, if your site is Chinese, you
  // may want to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: "en",
    locales: ["en"],
  },

  presets: [
    [
      "classic",
      {
        docs: {
          sidebarPath: "./sidebars.ts",
        },
        theme: {
          customCss: "./src/css/custom.css",
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    navbar: {
      title: "",
      logo: {
        alt: "Tuva EMPI Docs",
        src: "img/tuva-empi-logo.svg",
      },
      items: [
        {
          type: "doc",
          docId: "tuva-empi/landing",
          position: "left",
          label: "Docs",
        },
        {
          type: "custom-NavbarExternalLink",
          href: "/api-docs",
          position: "left",
          label: "API",
        },
        {
          href: "https://www.tuvahealth.com",
          position: "right",
          className: "header-tuva-link",
          "aria-label": "Tuva Health",
        },
        {
          href: "https://join.slack.com/t/thetuvaproject/shared_invite/zt-16iz61187-G522Mc2WGA2mHF57e0il0Q",
          position: "right",
          className: "header-slack-link",
          "aria-label": "Slack Community",
        },
        {
          href: "https://github.com/tuva-health/tuva_empi",
          position: "right",
          className: "header-github-link",
          "aria-label": "GitHub repository",
        },
      ],
    },
    footer: {
      style: "light",
      copyright: `Copyright © ${new Date().getFullYear()} Tuva Health`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
