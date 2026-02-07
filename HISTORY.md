This file contains the changelog of older versions of pg_lab. Please refer to the [CHANGELOG.md](CHANGELOG.md) file for the latest version.

# Version 0.4.3

## 🐣 New features
- Setting *plan_mode=full* now forces sequential sequential if the other hints do not explicitly request parallel plans.
  This is in line with how the absence of memoize/materialize hints are already handled.

## 💀 Breaking changes
- _None_

## 📰 Updates
- Debug builds now automatically enable assert-checking.
- Reworked the internal hinting logic once again. This should fix some edge cases when hinting parallel plans or intermediate
  operators like materialization/memoization. The new approach seems to be much more robust and easier to maintain.
  Hopefully this pave the way for future features as well.
- Introduced basic regression tests for the hinting logic. This should help to avoid future breakages. Maybe..

## 🏥 Fixes
- Fixed segfault when using *plan_mode=full*
- Fixed the setup script failing on some systems. It seems like make produces an error when trying to clean projects that have
  not run configure before. **🙏 Thanks @JWehrstein for reporting!**

---

# Version 0.4.2

## 🐣 New features
- _None_

## 💀 Breaking changes
- _None_

## 📰 Updates
- Migrated to Meson for the Postgres build. This seems to be the more robust approach across platforms (especially
  including MacOS). At the same time, this also provides faster build times and better IntelliSense for VSCode on WSL.

## 🏥 Fixes
- _None_

---


# Version 0.4.1

## 🐣 New features
- _None_

## 💀 Breaking changes
- _None_

## 📰 Updates
- _None_

## 🏥 Fixes
- Fixed segfault after "arbitrary" query executions. It turns out that internal management of temporary GUCs used Postgres'
  memory contexts incorrectly, leading to use-after-free bugs.

---


# Version 0.4.0

## 🐣 New features
- Reworked the Dockerfile to properly support volumes

## 💀 Breaking changes
- _None_

## 📰 Updates
- _None_

## 🏥 Fixes
- _None_

---


# Version 0.3.1

## 🐣 New features
- Support for partial join orders using the new `JoinPrefix` hint. Each hint block can contain multiple prefixes, the resulting
  query plan must start with the given joins as outlined.
- Support for temporary GUC settings using the new `Set` hint. All GUC modifications are made just for the current query and
  will be rolled back once the query finishes.

## 💀 Breaking changes
- _None_

## 📰 Updates
- _None_

## 🏥 Fixes
- #1 - Fixed implementation of *pg_temperature* on PG <= 16 using features of PG 17. **🙏 Thanks @JWehrstein for reporting!**
- Fixed cleanup of temporary GUCs breaking queries that did not specify any hints.
- #1 - Documented *libzstd-dev* as a required dependency. **🙏 Thanks @JWehrstein for reporting!**
- Added support for a bunch of missing path types to the hinting logic
- Fixed the Dockerfile using an old branch to install pg_lab

## 🪲 Known bugs
- _None_

---


# Version 0.3.0

## 🐣 New features
- Support for partial join orders using the new `JoinPrefix` hint. Each hint block can contain multiple prefixes, the resulting
  query plan must start with the given joins as outlined.
- Support for temporary GUC settings using the new `Set` hint. All GUC modifications are made just for the current query and
  will be rolled back once the query finishes.

## 💀 Breaking changes
- _None_

## 📰 Updates
- _None_

## 🏥 Fixes
- Added support for a bunch of missing path types to the hinting logic
- Fixed the Dockerfile using an old branch to install pg_lab

## 🪲 Known bugs
- _None_

---


# Version 0.2.1

## 🐣 New features
- _None_

## 💀 Breaking changes
- _None_

## 📰 Updates
- _None_

## 🏥 Fixes
- Fixed being unable to hint merge joins with materialized inner input node. As it turns out, the material nodes are never
  actually inserted during path construction. Instead, the planner creates them on-the-fly when turning the merge join path
  into a plan node.
- Fixed not finding parallel plans for some hint combinations. Parallel joins take their inner access path from the (parallel
  safe) non-partial access paths, which was not correctly accounted for before.

## 🪲 Known bugs
- _None_

---


# Version 0.2.0

## 🐣 New features
- Added support for parallel worker hints to operators and as top-level hints.

## 💀 Breaking changes
- _None_

## 📰 Updates
- _None_

## 🏥 Fixes
- _None_

## 🪲 Known bugs
- _None_
 Version 0.4.2

## 🐣 New features
- _None_

## 💀 Breaking changes
- _None_

## 📰 Updates
- Migrated to Meson for the Postgres build. This seems to be the more robust approach across platforms (especially
  including MacOS). At the same time, this also provides faster build times and better IntelliSense for VSCode on WSL.

## 🏥 Fixes
- _None_

---


# Version 0.4.1

## 🐣 New features
- _None_

## 💀 Breaking changes
- _None_

## 📰 Updates
- _None_

## 🏥 Fixes
- Fixed segfault after "arbitrary" query executions. It turns out that internal management of temporary GUCs used Postgres'
  memory contexts incorrectly, leading to use-after-free bugs.

---


# Version 0.4.0

## 🐣 New features
- Reworked the Dockerfile to properly support volumes

## 💀 Breaking changes
- _None_

## 📰 Updates
- _None_

## 🏥 Fixes
- _None_

---


# Version 0.3.1

## 🐣 New features
- Support for partial join orders using the new `JoinPrefix` hint. Each hint block can contain multiple prefixes, the resulting
  query plan must start with the given joins as outlined.
- Support for temporary GUC settings using the new `Set` hint. All GUC modifications are made just for the current query and
  will be rolled back once the query finishes.

## 💀 Breaking changes
- _None_

## 📰 Updates
- _None_

## 🏥 Fixes
- #1 - Fixed implementation of *pg_temperature* on PG <= 16 using features of PG 17. **🙏 Thanks @JWehrstein for reporting!**
- Fixed cleanup of temporary GUCs breaking queries that did not specify any hints.
- #1 - Documented *libzstd-dev* as a required dependency. **🙏 Thanks @JWehrstein for reporting!**
- Added support for a bunch of missing path types to the hinting logic
- Fixed the Dockerfile using an old branch to install pg_lab

## 🪲 Known bugs
- _None_

---


# Version 0.3.0

## 🐣 New features
- Support for partial join orders using the new `JoinPrefix` hint. Each hint block can contain multiple prefixes, the resulting
  query plan must start with the given joins as outlined.
- Support for temporary GUC settings using the new `Set` hint. All GUC modifications are made just for the current query and
  will be rolled back once the query finishes.

## 💀 Breaking changes
- _None_

## 📰 Updates
- _None_

## 🏥 Fixes
- Added support for a bunch of missing path types to the hinting logic
- Fixed the Dockerfile using an old branch to install pg_lab

## 🪲 Known bugs
- _None_

---


# Version 0.2.1

## 🐣 New features
- _None_

## 💀 Breaking changes
- _None_

## 📰 Updates
- _None_

## 🏥 Fixes
- Fixed being unable to hint merge joins with materialized inner input node. As it turns out, the material nodes are never
  actually inserted during path construction. Instead, the planner creates them on-the-fly when turning the merge join path
  into a plan node.
- Fixed not finding parallel plans for some hint combinations. Parallel joins take their inner access path from the (parallel
  safe) non-partial access paths, which was not correctly accounted for before.

## 🪲 Known bugs
- _None_

---


# Version 0.2.0

## 🐣 New features
- Added support for parallel worker hints to operators and as top-level hints.

## 💀 Breaking changes
- _None_

## 📰 Updates
- _None_

## 🏥 Fixes
- _None_

## 🪲 Known bugs
- _None_
