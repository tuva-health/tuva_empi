module.exports = {
  preset: "ts-jest",
  testEnvironment: "jsdom",
  moduleNameMapper: {
    "^@/(.*)$": "<rootDir>/src/$1",
    "^lucide-react$":
      "<rootDir>/node_modules/lucide-react/dist/cjs/lucide-react.js",
  },
  setupFilesAfterEnv: ["<rootDir>/src/setupTests.ts"],
  testMatch: ["**/*.test.(ts|tsx)"],
  testPathIgnorePatterns: ["/node_modules/", "/.next/"],
  transform: {
    "^.+\\.(ts|tsx)$": "babel-jest",
  },
  transformIgnorePatterns: [
    "/node_modules/(?!(react-dnd|dnd-core|@react-dnd|lucide-react)/)",
  ],
  globals: {
    "ts-jest": {
      tsconfig: "tsconfig.json",
      jsx: "react-jsx",
    },
  },
};
