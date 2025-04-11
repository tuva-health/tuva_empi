---
id: release-process
title: "Release Process Steps"
hide_title: true
hide_table_of_contents: true
sidebar_position: 3
---

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
