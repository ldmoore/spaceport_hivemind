FROM mcr.microsoft.com/devcontainers/base:bookworm

# The bundled Bazaar practice-server binaries require Linux glibc. Bookworm
# supplies glibc 2.36; protobuf-compiler is also useful when experimenting
# with the schema outside Python's generated bindings.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-venv \
        make \
        protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
