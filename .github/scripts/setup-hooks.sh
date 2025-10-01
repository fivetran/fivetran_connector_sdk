#!/bin/bash

# Fail on any error
set -e

HOOKS_DIR=".github/git_hooks"

# Check if we're in a Git repo
if [ ! -d .git ]; then
  echo "Not inside the Git repository. Please run this script from the root of the repository."
  exit 1
fi

# Set the hooks path
git config core.hooksPath "$HOOKS_DIR"

echo "Git hooks path set to: $HOOKS_DIR"
