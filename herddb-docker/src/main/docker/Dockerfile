FROM java

ENV JAVA_HOME /usr

ADD /herddb-${project.version}.zip //

COPY * /local/
RUN [ "/bin/sh", "/local/build.sh" ]

VOLUME ["/herddb/data","/herddb/conf"]

WORKDIR /herddb

ENTRYPOINT [ "/herddb/bin/service", "server", "console","--use-env" ]