FROM openjdk:11-jre-slim

LABEL mantainer="Marcello Percoco <marcello.percoco@diennea.com>"

ENV HERD_HOME_DIR=/opt/herddb 
ENV HERD_CONF_DIR=${HERD_HOME_DIR}/conf \
    HERD_DATA_DIR=${HERD_HOME_DIR}/data \
    HERD_LOG_DIR=${HERD_HOME_DIR}/logs
    ENV PATH=${PATH}:/${HERD_HOME_DIR}/bin

# Add a user with an explicit UID/GID and create necessary directories
RUN set -eux; \
    groupadd -r herddb --gid=1000; \
    useradd -r -g herddb --uid=1000 herddb; \
    mkdir -p "${HERD_DATA_DIR}" "${HERD_CONF_DIR}" "${HERD_LOG_DIR}"; \
    chown herddb:herddb "${HERD_DATA_DIR}" "${HERD_CONF_DIR}" "${HERD_LOG_DIR}"

# Install required packges
RUN set -eux; \
    apt-get update; \
    DEBIAN_FRONTEND=noninteractive \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        dirmngr \
        gosu \
        netcat \
        wget \
        unzip; \
    rm -rf /var/lib/apt/lists/*; \
# Verify that gosu binary works
    gosu nobody true

ARG DISTRO_VERSION=0.12.0
ARG DISTRO_NAME=herddb-services-0.12.0

# Download HerdDb, verify its PGP signature, untar and clean up
RUN set -eux; \
    wget -q "https://github.com/diennea/herddb/releases/download/v${DISTRO_VERSION}/herddb-services-${DISTRO_VERSION}.zip"; \
    unzip "${DISTRO_NAME}".zip; \
    rm -rf "${DISTRO_NAME}"/conf/*; \
    mv "${DISTRO_NAME}"/* "${HERD_HOME_DIR}"; \
    rm -rf "${DISTRO_NAME}".zip

COPY confs/* ${HERD_HOME_DIR}/conf/ 

RUN chown -R herddb:herddb "${HERD_HOME_DIR}"

USER herddb

WORKDIR ${HERD_HOME_DIR}
VOLUME ["${HERD_DATA_DIR}", "${HERD_LOG_DIR}", "${HERD_CONF_DIR}"]

EXPOSE 7000

COPY docker-entrypoint.sh /

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["service","server","console","--use-env"]
