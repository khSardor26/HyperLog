<!-- bmad:context -->
<!-- Verified 2026-08-13 against 50a2034cc04bca079cea3fdb400565d9843aad16. Managed by bmad-project-context; edits inside this block are replaced on refresh. Keep anything you want preserved outside the markers. -->

## Hyperlog

Contains the BMad harness and installed skills. Planning artifacts live in `_bmad-output/planning-artifacts`; project knowledge (docs) lives in `docs/`.

## Where things are

- Skill/unit tests: `.agents/skills/*/scripts/tests/`
- Planning artifacts: `_bmad-output/planning-artifacts`
- Implementation artifacts: `_bmad-output/implementation-artifacts`

## Running and verifying

- No repository-level CI workflows detected (no `.github/workflows/`, `.gitlab-ci.yml`, or `.circleci/config.yml` found).
- No top-level build manifests found (no `package.json`, `Makefile`, or `pyproject.toml` at repo root).
- Skill/unit tests run under the project's UV harness. Evidence in skill test files recommends invoking pytest via the harness; a working invocation is:

  `uv run --with pytest -m pytest scripts/tests`

  (run per-skill from `.agents/skills/<skill>/` for that skill's tests)

- If CI is added, require it to run the harness tests (`uv run --with pytest`) and any language/type checks; prefer adding a CI check rather than duplicating invocations in prose.

<!-- /bmad:context -->
