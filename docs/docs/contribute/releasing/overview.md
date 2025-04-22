---
id: overview
title: Overview
sidebar_position: 0
---

# Release Process

This document outlines the release process for the Tuva EMPI project.

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

## Branching and Versioning Strategy

![Tuva EMPI Branching and Versioning Strategy](/img/branching-strategy.png)
