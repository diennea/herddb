<p class="intro">
HerdDB is a SQL distributed database implemented in Java. It has been designed to be embeddable in any Java Virtual Machine. It is optimized for fast "writes" and primary key read/update access patterns. 
</p>

<h2>Scalability</h2>
HerdDB is designed to manage hundreds of tables. It is simple to add and remove hosts and to reconfigure tablespaces to easly distribute the load on multiple systems.

<h2>Resiliency</h2>
HerdDB leverages <a href="http://zookeeper.apache.org/" >Apache Zookeeper </a> and <a href="http://bookkeeper.apache.org/" >Apache Bookkeeper </a>to build a fully replicated, shared-nothing architecture without any single point of failure.

<h2>Ease of use</h2>
At the low level HerdDB is very similar to a key-value NoSQL database. On top of that an SQL abstraction layer and JDBC Driver support enables every user to leverage existing known-how and port existing applications to HerdDB.
 