---
id: additional-details
title: "Additional Details"
hide_title: true
hide_table_of_contents: true
sidebar_position: 2
---

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
