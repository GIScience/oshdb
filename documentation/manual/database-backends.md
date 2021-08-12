OSHDB Database Backends
=======================

There OSHDB currently implements two different database backends. One is executing queries locally on a single machine and expects the OSHDB data to be stored in any JDBC compatible database, for example in a [H2 database](https://www.h2database.com) which can be used with the OSHDB data extracts available from [downloads.ohsome.org](https://downloads.ohsome.org/OSHDB/v0.7/). The second database backend executes OSHDB queries on a distributed cluster environment running the [Apache Ignite](https://ignite.apache.org/) big data platform.

Database backends can implement different algorithms that control how a query is actually executed and can be optimized for different scenarios. These are explained below. 

OSHDBJdbc / OSHDBH2
-------------------

The [`ODHSBJDBC`](https://docs.ohsome.org/java/oshdb/0.7.1/aggregated/org/heigit/ohsome/oshdb/api/db/OSHDBJdbc.html) backend is often used in the `OSHDBH2` variant, which expects data to be stored in a single H2 database file. A few example OSHDB extracts in the H2 format are available as download from [downloads.ohsome.org](https://downloads.ohsome.org/OSHDB/v0.7/). It is also possible to generate an extract using an OSM history dump file using the [`oshdb-etl`](https://github.com/GIScience/oshdb/tree/0.7.1/oshdb-etl) module.

Alternatively, the OSHDB data can also be stored in any JDBC compatible database (e.g. a [PostgreSQL](https://www.postgresql.org/) database). The OSHDB data is however always processed and analyzed locally on the machine from which the OSHDB query is started. It is therefore advisable to keep the OSHDB data as local as possible in order to minimize network traffic when using the OSHDBJdbc backend. 

OSHDBIgnite
-----------

The [`OSHDBIgnite`](https://docs.ohsome.org/java/oshdb/0.7.1/aggregated/org/heigit/ohsome/oshdb/api/db/OSHDBIgnite.html) backend executes computations on a distributed cluster of computers running the [Apache Ignite](https://ignite.apache.org/) big data platform. Each of the computers of the cluster only holds a subset of the global OSHDB data set and can therefore execute its part of an OSHDB query more quickly than a single computer having to process the whole data set.

There are currently three different [compute modes](https://docs.ohsome.org/java/oshdb/0.7.1/aggregated/org/heigit/ohsome/oshdb/api/db/OSHDBIgnite.html#computeMode()) available in the OSHDBIgnite backend:

* *LOCAL_PEEK* - (default) is optimized for small to mid scale queries.
* *SCAN_QUERY* - works better for large scale (e.g. global) analysis queries.
* *AFFINITY_CALL* - is generally slower than the other two compute modes, but supports [streaming](https://docs.ohsome.org/java/oshdb/0.7.1/aggregated/org/heigit/ohsome/oshdb/api/mapreducer/MapReducer.html#stream()) of results.

In order to use the OSHDB Ignite backend, it is necessary to add the maven module `oshdb-api-ignite` to your project's maven dependencies:

```xml
<dependency>
  <groupId>org.heigit.ohsome</groupId>
  <artifactId>oshdb-api-ignite</artifactId>
  <version>0.7.1</version>
</dependency>
```
