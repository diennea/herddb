FROM java

ARG HERDDB_DIR=/herddb

ENV JAVA_HOME /usr

ADD /herddb-${project.version}.zip //

RUN unzip -q herddb-${project.version}.zip && \
    rm herddb-${project.version}.zip && \    
    mv herddb-* ${HERDDB_DIR}

COPY * /local/
RUN [ "/bin/sh", "/local/build.sh" ]

WORKDIR ${HERDDB_DIR}

ENTRYPOINT [ "/herddb/bin/service", "server", "console","--use-env" ]