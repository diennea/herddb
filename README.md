[![Build Status](https://travis-ci.org/diennea/herddb.svg?branch=master)](https://travis-ci.org/diennea/herddb) [![Coverage Status](https://coveralls.io/repos/github/diennea/herddb/badge.svg?branch=master)](https://coveralls.io/github/diennea/herddb?branch=master)


# What is HerdDB ?

HerdDB is a **distributed Database**, data is distributed among a cluster of server **without the need of a shared storage**.

HerdDB primary language is **SQL** and clients are encouraged to use both the JDBC Driver API and the low level API.

HerdDB is **embeddable** in any Java Virtual Machine, each node will access directly to local data without the use of the network.

HerdDB replication functions are built upon **Apache ZooKeeper** and **Apache BookKeeper**.

HerdDB is internally very similar to a NoSQL database and, basically, it is a **key-value DB** with an SQL abstraction layer which enables every user to leverage existing known-how and to port existing applications.

*HerdDB has been designed for fast "writes" and for primary key read/update data access patterns.*

HerdDB supports **transactions** and "committed read" isolation level

HerdDB uses **Apache Calcite** as SQL parser and SQL Planner

## Basic concepts

Data, as in any **SQL database**, is organized in tables and tables are grouped inside **Tablespaces**.

A Tablespace is the fundamental architectural brick upon which the replication is built and some DB features are available only among tables of the same tablespace:
- transactions may span only tables of the same tablespace
- subqueries may span only tables of the same tablespace

Replication is configured at tablespace level, so for each tablespace only one server is designed to be the 'leader' (manager) at a specific point in time, then you may configure a set of 'replicas'.
The system automatically replicates data between replicas and handles server failures transparently.

## Overview

[Intoducing HerdDB - Pulsar Summit 2020](https://www.youtube.com/watch?v=K7xQZ9V9Ml0) - Enrico Olivelli

[![Intoducing HerdDB - Youtube link](https://img.youtube.com/vi/K7xQZ9V9Ml0/0.jpg)](https://www.youtube.com/watch?v=K7xQZ9V9Ml0)

[Other talks and deep dives](https://github.com/diennea/herddb/wiki/Talks-&-Publications)

## Getting Involved

Join the [mailing list](http://lists.herddb.org/mailman/listinfo)

## License

HerdDB is under [Apache 2 license](http://www.apache.org/licenses/LICENSE-2.0.html).

