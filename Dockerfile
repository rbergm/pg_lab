FROM ubuntu:noble

EXPOSE 5432

# Install dependencies
RUN apt update && apt install -y \
        build-essential sudo locales tzdata procps lsof \
        bison flex curl pkg-config cmake llvm clang \
        libicu-dev libreadline-dev libssl-dev liblz4-dev libossp-uuid-dev libzstd-dev zlib1g-dev \
        python3 python3-venv python3-pip \
        git vim unzip zstd default-jre tmux ; \
    locale-gen en_US.UTF-8 && \
    update-locale LANG=en_US.UTF-8


# Configure some general settings
ARG USERNAME=lab
ENV USERNAME=$USERNAME
ENV USER=$USERNAME

ENV LANG=en_US.UTF-8
ENV LC_ALL=C.UTF-8
ARG TIMEZONE=UTC
ENV TZ=$TIMEZONE
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

ARG PGVER=17
ENV PGVER=$PGVER
ARG DEBUG=false
ENV DEBUG=$DEBUG

WORKDIR /pg_lab
RUN useradd -ms /bin/bash $USERNAME ; \
    chown -R $USERNAME:$USERNAME /pg_lab ; \
    chmod 755 /pg_lab ; \
    echo "$USERNAME:$USERNAME" | chpasswd ; \
    usermod -aG sudo $USERNAME
USER $USERNAME

RUN git clone --depth 1 --branch=feature/parallel-hints https://github.com/rbergm/pg_lab /pg_lab ; \
    if [ "$DEBUG" = "true" ]; then \
        ./postgres-setup.sh --stop --pg-ver ${PGVER} --debug ; \
    else \
        ./postgres-setup.sh --stop --pg-ver ${PGVER} ; \
    fi

CMD ["/pg_lab/tools/docker-entrypoint.sh"]
