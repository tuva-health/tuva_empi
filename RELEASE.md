# Release Process

This document outlines the release process for the Tuva EMPI project.

## Release Types

1. **Dev Release from Main**
   - Happens automatically on push to main
   - Images tagged with commit SHA
   - No version change required

2. **Release Candidate Process**
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

3. **Bugfix Release**
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

4. **Backport from Main**
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

## Automated Actions

The following actions happen automatically:

1. On push to `main` or `build-dev/*`:
   - Build Docker images
   - Tag with commit SHA
   - Push to GitHub Container Registry

2. On merge to `release/*` branch with VERSION change:
   - Create Git tag
   - Create GitHub Release
   - Build Docker images
   - Tag with version
   - Push to GitHub Container Registry
   - If previous version was -dev:
     - Create PR to bump main to next dev version

## Version Format

- Development: `X.Y.Z-dev`
- Release: `X.Y.Z`
- Bugfix: `X.Y.Z+1`

## Notes

- Only one active release branch is supported at a time
- Backports are only supported for the current release branch
- All changes must go through PR review process
- Version can be overridden in release workflow using workflow dispatch
