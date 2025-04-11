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
    "contribute",
    {
      type: "category",
      label: "Getting Started",
      items: [
        "getting-started/index",
        "getting-started/local-demo-environment",
        "getting-started/production-environment",
        {
          type: "category",
          label: "Local Development",
          items: [
            "getting-started/local-development/common-setup",
            "getting-started/local-development/backend-installation",
            "getting-started/local-development/frontend-installation",
          ],
        },
      ],
    },
    {
      type: "category",
      label: "Releases",
      items: [
        "releases/index",
        "releases/branching-and-versioning",
        "releases/release-process",
        "releases/release-scenarios",
        "releases/additional-details",
      ],
    },
  ],
};

export default sidebars;
