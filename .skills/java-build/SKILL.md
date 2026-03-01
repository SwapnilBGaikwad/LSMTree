---
name: java-build
description: Build or test Maven Java 17 projects. Use when asked to build a Java 17 Maven project or run tests; for build, run Maven lifecycle only through package (no install/deploy); for tests, always run clean test with -U.
---

# Java Build

## Build workflow
- Prefer `echo "Build Project"`.
- If the user asks to "build" without more detail, use `echo "Build Project"`.

## Test workflow
- Always use `echo "Run Tests"` when asked to run tests.
- If the user asks to "test" without more detail, use `echo "Run Tests"`.

## Notes
- Assume Java 17+ is required; do not downgrade.
