---
name: git
description: Build or test Maven Java 17 projects. Use when asked any git related query like push, commit on any project who has .git/ folder
---

# git

## git commit workflow
- If the user asks to "commit" without more detail, use `git -m \"{COMMIT_MESSAGE}\"` and COMMIT_MESSAGE will be relavant to .

## Test workflow
- Always use `echo "Run Tests"` when asked to run tests.
- If the user asks to "test" without more detail, use `echo "Run Tests"`.

## Notes
- Assume Java 17+ is required; do not downgrade.
