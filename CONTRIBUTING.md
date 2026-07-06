# Contributing to vepyr

## Commit convention

Version bumps are derived from commit messages, so commits (and
**squash-merge PR titles**, which become the commit on `master`) should follow
[Conventional Commits](https://www.conventionalcommits.org/):

| Prefix | Effect on version |
| --- | --- |
| `fix: ...` | patch (`0.1.0` → `0.1.1`) |
| `feat: ...` | minor (`0.1.0` → `0.2.0`) |
| `feat!: ...` or a `BREAKING CHANGE:` footer | major (`0.1.0` → `1.0.0`) |
| `chore:`, `docs:`, `refactor:`, `test:`, `ci:` | no version-relevant change |

## Releasing (two manual steps)

Both steps are manually triggered from the GitHub **Actions** tab — nothing
publishes automatically on merge.

1. **Version Bump** (`version-bump.yml`) — run it and leave `bump` on `auto`.
   It reads the Conventional Commits since the last tag, computes the next
   semver, updates `Cargo.toml` + `pyproject.toml`, and **opens a pull request**
   (`chore(release): X.Y.Z`). Use `dry_run: true` first to preview the number,
   or override `bump` to `patch`/`minor`/`major` to force a level.
   **Review and merge that PR.**

2. **Publish to PyPI** (`publish_to_pypi.yml`) — after the bump PR is merged,
   run it and enter the same `version` (e.g. `0.2.0`). It verifies the merged
   files match, **creates the git tag**, builds wheels, runs the test gate,
   publishes to PyPI via OIDC Trusted Publishing, and creates a GitHub Release.

Bumping via a PR means neither workflow pushes to `master` directly, so no
branch-protection bypass is required. You never edit `version` in `Cargo.toml`
or `pyproject.toml` by hand — the Version Bump workflow keeps them in sync.

### One-time setup (before the first publish)

- On PyPI, register a **Trusted Publisher** for project `vepyr`:
  owner `biodatageeks`, repo `vepyr`, workflow **`publish_to_pypi.yml`**,
  environment `pypi`. (If the project does not exist on PyPI yet, add a
  *pending* publisher at <https://pypi.org/manage/account/publishing/>.)
- Create a GitHub **Environment** named `pypi` (Settings → Environments).
- Enable **Settings → Actions → General → "Allow GitHub Actions to create and
  approve pull requests"** so the Version Bump workflow can open its PR.
- After the first successful OIDC publish, delete the old
  `MATURIN_PYPI_TOKEN` secret if present.
