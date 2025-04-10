---
id: release-process
title: "Release Process"
hide_title: true
hide_table_of_contents: false
sidebar_position: 2
---

# Release Process

This document outlines the release process for the Tuva EMPI project.

## Branching and Versioning Strategy

![Tuva EMPI Branching and Versioning Strategy](/img/branching-strategy.png)

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

## Release Process Steps

### Production Release Process

```bash
# Create release branch
git checkout main
git checkout -b release/v1.2.0

# Create feature branch for version bump
git checkout -b prepare-v1.2.0

# Update VERSION file (remove -dev suffix for release)
echo "1.2.0" > VERSION

# Verify version
./scripts/version.sh get
./scripts/version.sh is-dev  # Should return false

# Commit and create PR
git add VERSION
git commit -m "Prepare release v1.2.0"
git push origin prepare-v1.2.0
```

### Bugfix Release Process

```bash
# Checkout release branch
git checkout release/v1.2.0

# Create bugfix branch
git checkout -b fix/issue-123

# Update VERSION file for bugfix
echo "1.2.1" > VERSION

# Verify version
./scripts/version.sh get

# Commit and create PR against release branch
git add .
git commit -m "Fix issue #123"
git push origin fix/issue-123
```

### Backport Process

```bash
# Identify commit to backport
git log main --oneline

# Checkout release branch
git checkout release/v1.2.0

# Cherry-pick the commit
git cherry-pick <commit-hash>

# Update VERSION file
echo "1.2.1" > VERSION

# Verify version
./scripts/version.sh get

# Create PR against release branch
git checkout -b backport-feature-xyz
git push origin backport-feature-xyz
```

## Version Management

The project includes a version management script at `scripts/version.sh` with the following commands:

```bash
# Get current version
./scripts/version.sh get

# Check if current version is a dev version
./scripts/version.sh is-dev

# Get next dev version (e.g., 1.2.0 -> 1.3.0-dev)
./scripts/version.sh bump-dev
```

## CI/CD Workflows

The project uses reusable GitHub Actions workflows for consistent building and releasing:

### Build Workflows

- Located in `.github/workflows/build-{backend,frontend}.yml`
- Reusable workflows for building Docker images
- Support both development and release builds
- Handle image tagging and pushing to GitHub Container Registry
- Used by both dev builds and releases

### Release Workflow

- Located in `.github/workflows/release.yml`
- Triggers on VERSION file changes in release branches
- Creates Git tags and GitHub Releases
- Reuses build workflows for consistent image building
- Handles version bumping on main after releases

## Automated Actions

The following actions happen automatically:

1. On push to `main` or `build-dev/*`:
   - Triggers build workflows
   - Images tagged with commit SHA
   - Images pushed to [GitHub Container Registry](https://github.com/orgs/tuva-health/packages)

2. On merge to `release/*` branch with VERSION change:
   - Creates [Git tag](https://github.com/tuva-health/tuva_empi/tags) and [GitHub Release](https://github.com/tuva-health/tuva_empi/releases)
   - Triggers build workflows with version tag
   - If previous version was `-dev`:
     - Creates PR to bump main to next dev version

## Version Format

- Development: `X.Y.Z-dev`
- Release: `X.Y.Z`
- Bugfix: `X.Y.Z+1`

## Notes

- Only one active release branch is supported at a time
- All changes must go through PR review process
