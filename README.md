# pg_lab

![GitHub License](https://img.shields.io/github/license/rbergm/pg_lab?color=green)
![GitHub Release](https://img.shields.io/github/v/release/rbergm/pg_lab?color=blue)

<p align="center">
  <img src="assets/pg_lab_logo.png" width="256" alt="The Logo of pg_lab: a blue elephant wearing a chemist's coat is surrounded by various reagents." />
</p>

_pg\_lab_ is a research-focused fork of PostgreSQL.
It has two main goals:

1. allow Postgres extensions to modify details of the query planner, such as the cardinality estimator or cost model
2. enable the modification of planner decisions through detailed query hints

These goals are achieved by introducing new extensions points into the original Postgres code base and shipping a hinting
extension similar to [pg_hint_plan](https://github.com/ossc-db/pg_hint_plan).
Essentially, this requires to maintain a fork of the Postgres source code (available at
https://github.com/rbergm/postgres).
Our SIGMOD'2025 paper [^elephant] provides some insight into the motivation behind pg_lab.

| **💻 [Installation](docs/installation.md)** | **📝 [Hinting](docs/hinting.md)** | **🛠️ [Extension Points](docs/extension_points.md)** |


## ⚡ Quick start

You can install pg_lab either as a local Postgres server or as a Docker container.

For the **local installation** on a Ubuntu-based system, use the following:

```sh
sudo apt install -y \
   build-essential meson ninja-build sudo tzdata procps \
   bison flex curl pkg-config cmake llvm clang \
   git vim unzip zstd default-jre \
   libicu-dev libreadline-dev libssl-dev libzstd-dev liblz4-dev libossp-uuid-dev

./postgres-setup.sh --pg-ver 17 --debug --stop

. ./postgres-start.sh
```

For the **Docker installation**, use the following:

```sh
docker build -t pg_lab --build-arg TIMEZONE=$(cat /etc/timezone) .

docker run -d --name pg_lab -p 5432:5432 --volume $PWD/docker-volume:/pg_lab pg_lab

docker exec -it pg_lab /bin/bash
```

See the [Installation](docs/installation.md) documentation for more details on installation (including on other systems)
and usage of pg_lab.

> [!IMPORTANT]
> pg_lab is currently only tested Ubuntu/WSL and MacOS.
> Other platforms might (accidentally) work, but we cannot guarantee that.


## 📊 Comparison with pg_hint_plan

While a lot of the hinting behavior of pg_lab can also be found in pg_hint_plan, some features are only present in pg_lab
but not in pg_hint_plan and vice-versa.
See the table below for a comparison.
In addition to functional differences, pg_lab uses a different design philosophy:
instead of directly modifying the optimizer's logic, pg_lab utilizes extension points to implement its custom behavior.
The fundamental control-flow of the optimizer is thereby left intact.
The downside of this approach is that we essentially require a fork of the upstream Postgres source code to implement
pg_lab.

| Feature | pg_hint_plan | pg_lab |
|---------|--------------|--------|
| Forcing the join order | ✅ `Leading` hint | ✅ `JoinOrder` hint |
| Forcing initial joins | ❔ `Leading` hint (only linear join orders) | ✅ `JoinPrefix` hint (including bushy joins) |
| Forcing physical operators | ✅ Specific hints, e.g. `NestLoop(a b)` | ✅ Specific hints, e.g. `NestLoop(a b)` |
| Hints for Materialize and Memoize operators | ❔ Memoize (not enforced), ❌ Materialize | ✅ `Memo` and `Material` hints |
| Disabling specific operators | ✅ `NoNestLoop`, etc. hints | ❌ Not supported (⏳ but planned) |
| Custom cardinality estimates for joins | ✅ `Rows` hint | ✅ `Card` hint |
| Custom cardinality estimates for base tables | ❌ Not supported | ✅ `Card` hint |
| Parallel workers for joins | ❔ `Parallel` hint | ✅ e.g., as `workers` hint for operators |
| Storing and automatically re-using hint sets | ✅ Specific hint table | ❌ Not supported |
| Custom cost estimates for joins | ❌ Not supported | ✅ Specific hints, e.g. `NestLoop(a b (COST start=4.2 total=42.42))` |
| Custom cost estimates for base tables | ❌ Not supported | ✅ Specific hints, e.g. `SeqScan(a (COST start=4.2 total=42.42))` |
| Temporary GUC adjustments | ✅ `Set` hint | ✅ `Set` hint |

In the end, we took a lot of inspiration from pg_hint_plan for the design of pg_lab's hinting system:

```sql
/*=pg_lab=
 Card(t #42000)

 JoinOrder(((t mi) ci))
 IdxScan(mi)
 NestLoop(t mi (workers=4))

 HashJoin(t mi ci (COST start=50 total=500))
*/
SELECT count(*)
FROM title t
  JOIN movie_info mi ON t.id = mi.movie_id
  JOIN cast_info ci ON t.id = ci.movie_id
WHERE t.production_year > 2000;
```

See the [Hinting](docs/hinting.md) documentation for more details on the hinting system and how to use it.


## 🤬 Issues

Something feels wrong or broken, or a part of pg_lab is poorly documented or otherwise unclear?
Please don't hestitate to file an issue or open a pull request!
pg_lab is not one-off software, but an ongoing research project.
We are always happy to improve both pg_lab and its documentation and we feel that user experience (specifically,
_your_ user experience) is a very important part of this.


## 🧰 Other utilities

In addition to the core functionality, pg_lab also provides a number of utilities that are aimed at making the day-to-day
activities of optimizer research easier.

Specifically, we provide an extension called **`pg_temperature`** that allows you to easily simulate real cold-start and
hot-start scenarios for your benchmarks.
Take a look at `extension/pg_temperature` for more details.

The **[PostBOUND project](https://github.com/rbergm/PostBOUND)** is a high-level framework to rapidly implement optimizer
prototypes in a much more high-level language (i.e. Python) and to compare different optimizer prototypes in a transparent
and reproducible manner.
This framework also provides a number of utilities to easily setup common benchmarks such as JOB, Stats or Stack (even
if we might integrate them directly into pg_lab in the future).

The **`cout_star`** extension implements a simplified cost model inspired by $C_{MM}$ from Leis et al.[^how-good]
Our SIGMOD'2025 paper [^elephant] describes its design in depth.
See the `extension/cout_star` directory for the source code and installation instructions.

[^how-good]: Leis et al.: _How Good Are Query Optimizers, Really?_ (VLDB'2015) [🔗 Link](https://www.vldb.org/pvldb/vol9/p204-leis.pdf)

[^elephant]: Bergmann et al.: _An Elephant Under The Microscope_ (SIGMOD'2025) [🔗 Link](https://dl.acm.org/doi/10.1145/3709659)


## 🫶 Reference

If you find our work useful, please cite the following paper:

```bibtex
@inproceedings{bergmann2025elephant,
  author       = {Rico Bergmann and
                  Claudio Hartmann and
                  Dirk Habich and
                  Wolfgang Lehner},
  title        = {An Elephant Under the Microscope: Analyzing the Interaction of Optimizer
                  Components in PostgreSQL},
  journal      = {Proc. {ACM} Manag. Data},
  volume       = {3},
  number       = {1},
  pages        = {9:1--9:28},
  year         = {2025},
  url          = {https://doi.org/10.1145/3709659},
  doi          = {10.1145/3709659},
  timestamp    = {Tue, 01 Apr 2025 19:03:19 +0200},
  biburl       = {https://dblp.org/rec/journals/pacmmod/BergmannHHL25.bib},
  bibsource    = {dblp computer science bibliography, https://dblp.org}
}
```
