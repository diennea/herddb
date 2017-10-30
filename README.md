# What is HerdDB ?

HerdDB is a **distributed Database**, data is distributed among a cluster of server **without the need of a shared storage**.

HerdDB primary language is **SQL** and clients are encouraged to use both the JDBC Driver API and the low level API.

HerdDB is **embeddable** in any Java Virtual Machine, each node will access without network to local data.

HerdDB replication functions are built upon **Apache ZooKeeper** and **Apache BookKeeper** projects.

HerdDB is very similar to NoSQL databases, in fact at the Low level is it basically a **key-value DB** with an SQL abstraction layer which enables every user to leverage existing known-how and to port existing applications to HerdDB.

*HerdDB has been designed for fast "writes" and for primary key read/update data access patterns.*

HerdDB supports **transactions** and "committed read" isolation level

## Basic concepts

Data, as in any **SQL database**, is organized in tables, so there is nothing new to tell, but in order to leverage HerdDB replication function tables are grouped inside **Tablespaces**.

A Tablespace is a logical set of tables and it is the fundamental architectural brick upon which is built the replication.

There are some DB features which are available only among tables of the same tablespace:
- transactions may span only tables of the same tablespace
- subqueries may span only tables of the same tablespace

Replication is configured at tablespace level, for each tablespace only one server is designed to be the 'leader' (manager) and then you can configure a set of 'replicas'.
The system automatically replicates data and handles transparently server failures.

## License

Majordodo is under [Apache 2 license](http://www.apache.org/licenses/LICENSE-2.0.html).

