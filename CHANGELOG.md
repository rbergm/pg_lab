# Changelog

Version numbers are composed of three components, i.e. major.minor.patch.
As a rough guideline, patch releases are just for fixing bugs or adding minor details, minor releases change slightly
larger parts of the library or add significant new functionality.
Major releases fundamentally shift how the library is used and indicate stability.
Since we are not ready for the 1.0 release yet, this does not matter right now.

# ➡ Version 0.2.0 _(current)_

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

---


# ⏳ Version 0.2.1 _(planned)_

### 🐣 New features
- _None_

### 💀 Breaking changes
- _None_

### 📰 Updates
- _None_

### 🏥 Fixes
- Fixed being unable to hint mergejoins with materialized inner input node. As it turns out, the material nodes are never
  actually inserted during path construction. Instead, the planner creates them on-the-fly when turning the merge join path
  into a plan node.

### 🪲 Known bugs
- _None_
