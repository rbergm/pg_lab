# Changelog

Version numbers are composed of three components, i.e. major.minor.patch.
As a rough guideline, patch releases are just for fixing bugs or adding minor details, minor releases change slightly
larger parts of the library or add significant new functionality.
Major releases fundamentally shift how the library is used and indicate stability.
Since we are not ready for the 1.0 release yet, this does not matter right now.

To view the changelog of older versions, please refer to the [HISTORY.md](HISTORY.md) file.

---

# Version 0.5.2

## 🏥 Fixes

- Reworked the internal path pruning logic to enforce hint compliance. Our rework switches from a pruning-based model
  to a cost-based model: instead of rejecting illegal paths altogether, we manipulate their costs in such a way that they
  can truly never get picked by the optimizer. See the documentation on commit da1e9916cf9284abe2f5fee540727596d3400934
  for details on why this is necessary and how we approach the problem now.

---

# Version 0.5.1

## 🐣 New features

- The Docker setup now supports initializing commonly-used datasets such as JOB/IMDB, Stats and Stack by passing additional
  `--env` variables to the `docker run` command, e.g. `--env SETUP_JOB=true`.

## 💀 Breaking changes

- _None_

## 📰 Updates

- _None_

## 🏥 Fixes

- _None_

## 🪲 Known bugs

- _None_

---

# Version 0.5.0

## 🐣 New features

- Added support for Postgres v18. Sadly, this required a few changes to the underlying build process. Updating from older
  versions does not work out of the box. Please do the following to migrate:
  - Use `git fetch` to load the latest changes
  - Use `git submodule sync` to update the PG source reference
  - Use `git submodule update --init` to initialize the submodule
  - Run the normal `postgres-setup.sh` script
  With that being said, just using a fresh clone is probably the easiest way to get started.

## 💀 Breaking changes

- _None_

## 📰 Updates

- The setup scripts now default to PG 18
- We now issue a warning when using an older version of Meson during the build process.

## 🏥 Fixes

- Added a bunch of missing PG operators to the internal path traversal logic. This should add support for more complex
  queries.

## 🪲 Known bugs

- _None_
