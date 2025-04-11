---
id: release-scenarios
title: "Scenarios"
hide_title: true
hide_table_of_contents: true
sidebar_position: 2
---

## Release Scenarios

### 1. Production Release from Main

- Used for major/minor version releases
- Creates a new release branch
- Updates version number
- Creates Git tag and GitHub Release

### 2. Bugfix Release from Release Branch

- Used for patch releases
- Made directly on release branch
- Increments patch version
- Creates Git tag and GitHub Release

### 3. Backport from Main to Release Branch

- Used to port critical fixes from main
- Cherry-picks specific commits
- Increments patch version
- Creates Git tag and GitHub Release
