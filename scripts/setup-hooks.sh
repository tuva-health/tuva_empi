#!/bin/sh
# Script to set up Git hooks

HOOKS_DIR=".git/hooks"

echo "Setting up Git hooks..."

# Copy pre-commit hooks

PRE_COMMIT_HOOK="pre-commit-frontend"
cp ./scripts/hooks/$PRE_COMMIT_HOOK $HOOKS_DIR/
chmod +x $HOOKS_DIR/$PRE_COMMIT_HOOK

PRE_COMMIT_HOOK="pre-commit-backend"
cp ./scripts/hooks/$PRE_COMMIT_HOOK $HOOKS_DIR/
chmod +x $HOOKS_DIR/$PRE_COMMIT_HOOK

echo "Git hooks set up successfully."
