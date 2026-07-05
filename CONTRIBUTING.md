# Contributing to vepyr

## Commit convention

Releases are automated with [release-please](https://github.com/googleapis/release-please).
The next version and changelog are derived from commit messages on `master`,
so commits (and **squash-merge PR titles**, which become the commit on
`master`) must follow [Conventional Commits](https://www.conventionalcommits.org/):

| Prefix | Effect on version |
| --- | --- |
| `fix: ...` | patch release (`0.1.0` → `0.1.1`) |
| `feat: ...` | minor release (`0.1.0` → `0.2.0`) |
| `feat!: ...` or a `BREAKING CHANGE:` footer | major release (`0.1.0` → `1.0.0`) |
| `chore:`, `docs:`, `refactor:`, `test:`, `ci:` | no release |

## How a release happens

1. Merge Conventional-Commit PRs to `master` as usual.
2. release-please maintains an open **"chore: release X.Y.Z"** pull request
   with the computed version bump and `CHANGELOG.md`.
3. Merging that release PR builds the wheels, publishes to PyPI via OIDC
   Trusted Publishing, and attaches the wheels to a GitHub Release.

You never edit `version` in `Cargo.toml` or `pyproject.toml` by hand;
release-please keeps them in sync.
