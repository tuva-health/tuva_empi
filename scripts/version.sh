#!/bin/bash

# Get current version from VERSION file
get_version() {
  cat VERSION
}

# Check if version is a dev version
is_dev_version() {
  local version=$1
  [[ $version == *"-dev"* ]] && echo "true" || echo "false"
}

# Bump version to next dev version
# Example: 1.2.0 -> 1.3.0-dev
bump_to_next_dev() {
  local version=$1
  local major minor patch

  # Extract version parts
  IFS='.' read -r major minor patch <<< "${version%-*}"

  # Increment minor version
  minor=$((minor + 1))

  echo "${major}.${minor}.0-dev"
}

# Main logic based on command
case "$1" in
  "get")
    get_version
    ;;
  "is-dev")
    is_dev_version "$(get_version)"
    ;;
  "bump-dev")
    bump_to_next_dev "$(get_version)"
    ;;
  *)
    echo "Usage: $0 {get|is-dev|bump-dev}"
    exit 1
    ;;
esac
