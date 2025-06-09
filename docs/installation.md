# Detailed installation

Currently, we provide two ways to setup a new pg_lab instance: as a local Postgres server, or within a Docker container.
Both are described below.

## Local installation

pg_lab can be installed as a local Postgres system, without interfering with global paths.
Generally speaking, this is the preferred installation method.

> [!IMPORTANT]
> A local installation includes compiling the Postgres source code.
> Therefore, a number of dependencies and libraries need to be available on your system.
> On Ubuntu, the following apt command should cover it all:
> ```sh
> sudo apt install -y \
>    build-essential sudo tzdata procps \
>    bison flex curl pkg-config cmake llvm clang \
>    git vim unzip zstd default-jre \
>    libicu-dev libreadline-dev libssl-dev liblz4-dev libossp-uuid-dev
> ```
> On other distributions (including MacOS), install the appropriate counterparts and make sure that they are available on
> the PATH and/or the C_INCLUDE_PATH and LD_LIBRARY_PATH.
> For example, clang and LLVM (used for the Postgres JIT compiler) might not be found by the Postgres `configure` script.

Currently, we only support UNIX-ish platforms and test the setup primarily on a Linux (WSL) system and sporadicly on MacOS.
We cannot make any guarantees whether pg_lab will work on other plaforms and as a general rule-of-thumb, the more you
deviate from a plain Linux system, the quicker you will run into trouble.
All management scripts shipped with pg_lab are tested on Bash but should also work on zsh.
We test the latter as part of the aformentioned sporadic MacOS testing.

> [!TIP]
> If you do not set paths for these scripts explicitly, the scripts assume that they are run from the main pg_lab
> directory.

The installation itself controlled by the **postgres-setup.sh** script.
It works by loading a specific Postgres major version at `pg-source` and installing the compiled binaries at `pg-build`.
You can install multiple different versions in different directories.
By default, each installation receives a different subdirectory under *pg-build*, depending on the PG major release, such
as `pg-build/pg-17`.
The *data* directory will also be located there.
Use `--help` to view all supported installation options.

To start a Postgres instance, simply use **postgres-start.sh** script.
In addition to booting up the database server, it also takes care of updating your PATH and the like to make the local
installation more readily accessible.
Therefore, this script should be sourced (i.e. as `. ./postgres-start.sh`), rather than executed directly.
Use `--help` to view all supported options.

To shut down a running pg_lab server, use **postgres-stop.sh**.
In many ways, this script acts as the counterpart to *postgres-start.sh*.
When called, it also takes care of undoing all changes to your PATH that are performed by the start script.
Therefore, this script also has to be sourced.
Use `--help` to view all supported options.

If you want to interact with your pg_lab installation from a different terminal than the one you called *postgres-start.sh*
in, **postgres-load-env.sh** takes care of all necessary modifications of your PATH, etc.
Therefore, this script also has to be sourced.
Use `--help` to view all supported options.

## Docker

If you don't want to install pg_lab locally, we also supply a Dockerfile for a containerized setup.
**Please note that this Dockerfile is currently experimental and there might still be issues in the day-to-day usage.**

> [!TIP]
> If you don't like reading a lengthy documentation, simply use the following commands to get started:
>
> ```sh
>
> $ docker build \
>       --build-arg TIMEZONE=$(cat /etc/timezone) \
>       -t pg_lab \
>       .
>
> $ docker run -dt \
>       --shm-size 4G \
>       --name pg_lab \
>       --publish 5432:5432 \
>       pg_lab
>
> $ docker exec -it pg_lab /bin/bash
>
> ```

When building the pg_lab image, you can pass most of the settings that the *postgres-setup.sh* script would expect:

| Argument | Description | Default |
|----------|-------------|---------|
| `USERNAME` | The username in the container. This is also used as the Postgres user. The server password is the same as the username. | *lab* |
| `TIMEZONE` | The timezone of the server. Defaults to *UTC*. | *UTC* |
| `PGVER` | The Postgres server version to use. Currently, pg_lab supports PG 16 and 17. | 17 |
| `DEBUG` | Whether a debug build should be used. Allowed values are *true* and *false*. | *false* |

The image exposes the default Postgres port `5432`.
Don't forget to bind to it when creating the container if you want to directly connect to the server.

Once you have your container up and running, the shell will have all paths already set up properly.
