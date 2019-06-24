Data Update
===========
The following documentation explains how to set up an updating mechanism for a running OSHDB. For problems you may encounter when updating your client OSHDB-Version, please see [CHANGELOG](../../CHANGELOG.md).

The provided machanism uses replication files provided by OSM at [https://planet.openstreetmap.org/replication/](https://planet.openstreetmap.org/replication/). Minutely, hourly and dayly updates are available, depending on your needs.

## Mechanism
Updates are implemented via a two step process.

### Storing OSHEntities
New changes are first downloaded from the server and transformed into OSHEntites. Locally provied so called "ETL-files" are used to combine new changes with earlyer versions of an entity. ETL-files are in the form of a long table with index providing fast acces to OSH-Entities by id. They are a byproduct of the original data import (ETL) and can be requested form the OSHDB-Team.

These Entities are stored in a SQL-Database together with their Boundingbox to enable spatial queries. In addition Bitmaps are provided storing IDs of changed entities. This enables updating of OSHDB queries on the fly by excluding updated entities form the OSHDB and including these form the update database. This process is automatically implemented in the OSHDB-API when setting an update database.

### Updating the OSHDB

In a second step updated entities are written to the original OSHDB by replacing the outdated entities (here called flushing aka. merging). This should be done on a regular basis but the schedule depends on the user. The size of the update database should be supervised and flushing initialised on signs of overflow.

## Preparation
- To implement updates an update database has to be prepared. Provided with the correct JDBC-Connection the [Updater-Class](provideFinalLink) will take this off your hands by creating necessary tables. Ignite-SQL whit ignite-geospatial, H2 with h2gis and PostgreSQL whith postgis enabled are supported backends.
- As stated above one needs ETL-Files to parse for older versions of OSHEntities

## Updating
- the Updater-Class can be called via another java class or the command line. Javadoc and commandline helper provide detailed information on its usage.

## Flushing
- when flushing the update database, OSHDB must be disabled for query as concurrency is not supported.
- All other options an restrictions are described in the javadoc and command line helpers
