import tseslint from "typescript-eslint";
import importPlugin from "eslint-plugin-import";
import eslintConfigPrettier from "eslint-config-prettier/flat";
import eslint from "@eslint/js";

export default tseslint.config(
  {
    ignores: [".docusaurus/**", "build/**"],
  },
  tseslint.configs.recommended,
  eslint.configs.recommended,
  eslintConfigPrettier,
  {
    files: ["**/*.{ts,tsx}"],
    extends: [
      importPlugin.flatConfigs.recommended,
      importPlugin.flatConfigs.typescript,
    ],
    languageOptions: {
      globals: {
        React: "readonly",
      },
    },
    settings: {
      "import/core-modules": ["@theme/Heading", "@docusaurus/router"],
    },
    rules: {
      "@typescript-eslint/explicit-function-return-type": "error",
      "@typescript-eslint/consistent-type-definitions": ["error", "interface"],
      "@typescript-eslint/explicit-member-accessibility": "error",
      "@typescript-eslint/no-explicit-any": "error",
      "@typescript-eslint/explicit-module-boundary-types": "error",
    },
  },
);
