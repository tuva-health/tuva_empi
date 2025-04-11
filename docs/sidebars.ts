import type { SidebarsConfig } from "@docusaurus/plugin-content-docs";

// This runs in Node.js - Don't use client-side code here (browser APIs, JSX...)

/**
 * Creating a sidebar enables you to:
 - create an ordered group of docs
 - render a sidebar for each doc of that group
 - provide next/previous navigation

 The sidebars can be generated from the filesystem, or explicitly defined here.

 Create as many sidebars as you want.
 */
const sidebars: SidebarsConfig = {
  tutorialSidebar: [
    "tuva-empi",
    {
      type: "category",
      label: "Getting Started",
      items: [
        "getting-started/index",
        "getting-started/local-demo-environment",
        "getting-started/production-environment",
      ],
    },
    {
      type: "category",
      label: "Releases",
      items: [
        "releases/index",
        "releases/release-process",
        "releases/additional-details",
      ],
    },
    {
      type: "category",
      label: "Contribute",
      items: [
        "contribute/index",
        {
          type: "category",
          label: "Local Development",
          items: [
            "contribute/local-development/common-setup",
            "contribute/local-development/backend-installation",
            "contribute/local-development/frontend-installation",
          ],
        },
      ],
    },
  ],
};

export default sidebars;
