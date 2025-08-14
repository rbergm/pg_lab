FROM ubuntu:noble

EXPOSE 5432
STOPSIGNAL SIGINT

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

WORKDIR /pg_lab
RUN useradd -ms /bin/bash $USERNAME ; \
    echo "$USERNAME:$USERNAME" | chpasswd ; \
    usermod -aG sudo $USERNAME ; \
    echo "$USER ALL=(ALL:ALL) NOPASSWD: ALL" | tee /etc/sudoers.d/$USER
USER $USERNAME

VOLUME /pg_lab
COPY tools/docker-entrypoint.sh /docker-entrypoint.sh
CMD ["/docker-entrypoint.sh"]
