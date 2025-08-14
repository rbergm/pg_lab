# Changelog

Version numbers are composed of three components, i.e. major.minor.patch.
As a rough guideline, patch releases are just for fixing bugs or adding minor details, minor releases change slightly
larger parts of the library or add significant new functionality.
Major releases fundamentally shift how the library is used and indicate stability.
Since we are not ready for the 1.0 release yet, this does not matter right now.

---

# ➡ Version 0.4.0 _(current)_

## 🐣 New features
- 🐳 Reworked the Dockerfile to properly support volumes

## 💀 Breaking changes
- _None_

## 📰 Updates
- _None_

## 🏥 Fixes

---


# Earlier releases

## 🕑 Version 0.3.1

### 🐣 New features
- Support for partial join orders using the new `JoinPrefix` hint. Each hint block can contain multiple prefixes, the resulting
  query plan must start with the given joins as outlined.
- Support for temporary GUC settings using the new `Set` hint. All GUC modifications are made just for the current query and
  will be rolled back once the query finishes.

### 💀 Breaking changes
- _None_

### 📰 Updates
- _None_

### 🏥 Fixes
- #1 - Fixed implementation of *pg_temperature* on PG <= 16 using features of PG 17. **🙏 Thanks @JWehrstein for reporting!**
- Fixed cleanup of temporary GUCs breaking queries that did not specify any hints.
- #1 - Documented *libzstd-dev* as a required dependency. **🙏 Thanks @JWehrstein for reporting!**
- Added support for a bunch of missing path types to the hinting logic
- 🐳 Fixed the Dockerfile using an old branch to install pg_lab

### 🪲 Known bugs
- _None_

---


## 🕑 Version 0.3.0

### 🐣 New features
- Support for partial join orders using the new `JoinPrefix` hint. Each hint block can contain multiple prefixes, the resulting
  query plan must start with the given joins as outlined.
- Support for temporary GUC settings using the new `Set` hint. All GUC modifications are made just for the current query and
  will be rolled back once the query finishes.

### 💀 Breaking changes
- _None_

### 📰 Updates
- _None_

### 🏥 Fixes
- Added support for a bunch of missing path types to the hinting logic
- 🐳 Fixed the Dockerfile using an old branch to install pg_lab

### 🪲 Known bugs
- _None_

---


## 🕑 Version 0.2.1

### 🐣 New features
- _None_

### 💀 Breaking changes
- _None_

### 📰 Updates
- _None_

### 🏥 Fixes
- Fixed being unable to hint merge joins with materialized inner input node. As it turns out, the material nodes are never
  actually inserted during path construction. Instead, the planner creates them on-the-fly when turning the merge join path
  into a plan node.
- Fixed not finding parallel plans for some hint combinations. Parallel joins take their inner access path from the (parallel
  safe) non-partial access paths, which was not correctly accounted for before.

### 🪲 Known bugs
- _None_

---


# 🕑 Version 0.2.0

### 🐣 New features
- Added support for parallel worker hints to operators and as top-level hints.

### 💀 Breaking changes
- _None_

### 📰 Updates
- _None_

### 🏥 Fixes
- _None_

### 🪲 Known bugs
- _None_
