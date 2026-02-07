# Changelog

Version numbers are composed of three components, i.e. major.minor.patch.
As a rough guideline, patch releases are just for fixing bugs or adding minor details, minor releases change slightly
larger parts of the library or add significant new functionality.
Major releases fundamentally shift how the library is used and indicate stability.
Since we are not ready for the 1.0 release yet, this does not matter right now.

To view the changelog of older versions, please refer to the [HISTORY.md](HISTORY.md) file.

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
