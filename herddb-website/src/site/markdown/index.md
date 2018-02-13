<p class="intro">
HerdDB is a SQL distributed database implemented in Java. It has been designed to be embeddable in any Java Virtual Machine. It is optimized for fast "writes" and primary key read/update access patterns. 
</p>

<h2>Scalability</h2>
HerdDB is designed to manage hundreds of tables. It is simple to add and remove hosts and to reconfigure tablespaces to easly distribute the load on multiple systems.

<h2>Resiliency</h2>
HerdDB leverages <a href="http://zookeeper.apache.org/" >Apache Zookeeper </a> and <a href="http://bookkeeper.apache.org/" >Apache Bookkeeper </a>to build a fully replicated, shared-nothing architecture without any single point of failure.

<h2>Ease of use</h2>
At the low level HerdDB is very similar to a key-value NoSQL database. On top of that an SQL abstraction layer and JDBC Driver support enables every user to leverage existing known-how and port existing applications to HerdDB.


<h2>Why we designed a new distributed database ?</h2>

At <a href="http://www.diennea.com">Diennea</a> we developed <a href="http://www.emailsuccess.com">EmailSuccess</a>, a powerfull MTA (Mail Transfer Agent), designed to deliver millions of email messages per hour to inboxes all around the world.

We have very particular requirements for the database layer of EmailSucces, we want a system which:

* is ultra fast for writes

* can be replicated among tens of machines. without any shared medium or SAN

* can handle thousands of tables

* can handle multi-table transactions

* can handle multi-table queries

* scale horizontally on the number of tables, simply adding macchines

* can optionally run inside the same process of the JVM which runs the main service

* can be controlled by using SQL language and the JDBC API

* can support indexing of data


We have been using <a href="http://hbase.apache.org">Apache HBase</a> for long time for our internal business but HBase (and other Big-Data engines) do not satisfy our requirements.

So we designed a new Key-Value database which will be fast enough to handle the write load of the system and then we added an SQL Planner and a JDBC Driver.

We already have great experience of <a href="http://bookkeeper.apache.org">Apache BookKeeper</a> and <a href="http://zookeeper.apache.org">Apache ZooKeeper</a> projects, as we use them to build sophisticated distributed services,
for instance <a href="http://majordodo.org">Majordodo</a> (which is open source and you can find it on GitHub and Maven Central),
and we decided to use BookKeeper as  write-ahead transaction log and ZookKeeper for group membership and coordination..

Herd DB leverages <a href='http://calcite.apache.org'>Apache Calcite</a> as SQL Planner, beeing able to support complex query plans, accessing data using optimized access plans.

<h2>HerdDB overview</h2>

From the API point of view you can see HerdDB as a traditional SQL-based database, so your are going to issue CREATE TABLE, SELECT, INSERT, JOIN...statements and the system will do what you expect.

But internally it works as a Key-Value engine, accessing to data by the Primary Key is as fast a possible, both for reads and for writes.
In fact the primary user of HerdDB, EmailSucess, uses it to store the state of every single email message.

On the Key-Value core we added the ability to run scans and multi-row updates, aggregate functions and so on, this way you can use it like any other SQL database.

The main unit of work for an HerdDB cluster is the **tablespace**, a tablespace is a group of tables.
In the context of a tablespace you can run transactions, joins and subqueries which span multiple tables.

In a cluster for each tablespace a leader node is designated by the administrator (with some kind of auto-healing and auto leader reassignment in case of failure) and all the transactions on its tables are run on that node.
This system scales well by having many tablespaces and so the load can be spread among all the machines in the cluster.

Indexes are supported by using an implementation of the Block Range Index pattern (BRIN indexes), adapted to the way the HerdDB uses to store data.

The database can be accessed from outside the process by using TLS and authentication is performed using SASL with Kerberos.

![Architecture](images/herddb.png)

<h2>The write path</h2>

The most critical path for data access in HerdDB is the *write path*, in particular the INSERT and the UPDATE-by-PK data manipulation statements are the most important for us, together with the GET-by-PK.

The leader of the tablespace keeps in memory a data structure which holds all the PKs for a table in an hash table, and an in-memory buffer which contains all the dirty and/or recently accessed records.

When an INSERT reachs the the server the write is first logged to the log, then the map of valid PKs gets updated and the new record is stored in the memory buffer.
If an UPDATE is issued on the same PK (and this is our primary use case) the update is directly performed in memory, without hitting "data pages" disks, we only write to the log in order to achieve durability.
If a GET comes for the same PK we can read directly the "dirty" record from the buffer.
After a configurable timeout or when the system is running out of memory a checkpoint is performed and buffers are flushed to disk, creating immutable data pages, so usually all the work is in memory, writes are performed serially on the transaction log and when flushing to disk complete data pages are written, without ever modifiing existing files.
This kind of write pattern is very suitable of our use case: data files are always written or read entirely, leveraging the most of OS caches.

<h2>Replication and Apache BookKeeper</h2>
HerdDB leverages Apache BookKeeper ability to provide a distributed write ahead log, when a node is running as leader it writes each state change to BookKeeper, working as a replicated state machine.
Some features:

 * each write is guaranteed to be "durable" after the ack from BookKeeeper

 * each replica is guaranteed to read only entries for which the ack has been received from the writer (the Last-Add-Confirmed protocol)

 * each ledger (the basic storage unit of BookKeeper) can be written only once


For each tablespace you can add a virtually unlimited number of replicas, each 'replica' node will 'tail' the transaction log and replay each data manipulation activity to its local copy of the data.
If a "new leader" comes in, BookKeeper will fence out the "old leader", preventing any further write to the ledger, this way the old leader will not be able to carry on its activity and change its local state: this will guarantee that every node will converge to the same consistent view of the system.

Apache BookKeeper servers, called Bookies, can be run standalone but the preferred way is to run them inside the same JVM of the database, leveraging the ability to talk to the Bookie without passing from the network stack. 
