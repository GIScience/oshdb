Changelog
=========

## 0.6.0 SNAPSHOT (current master)

* bigspatialdata-parent version bump to 1.2, rename bigspatialdata-core-parent → oshdb-parent
* improved performance of data [stream](https://docs.ohsome.org/java/oshdb/0.6.0-SNAPSHOT/oshdb-api/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#stream--)ing queries on ignite (using AffinityCall backend)
* make monthly time intervals more intuitive #201 

## 0.5.3

* update ignite version to 2.7.5

## 0.5.2

* fix calculation of insertIds / entities stored in too high zoom levels, which resulted in partially missing data in some queries #183
* prevent crashes while building certain invalid multipolygon relation geometries #179

## 0.5.1

* oshdb-util: Fix a bug in `Geo.areaOf` when applied to polygons with holes. Before this fix, the method errorneously skipped the first inner ring when calculating the total area of a polygon. This affected geometries constructed from OSM multipolygon relations.
* oshdb-util: Implemented `QUARTERLY`, `WEEKLY`, `DAILY`, and `HOURLY` as additional time intervals.

## 0.5.0

### breaking changes

* JTS library was updated to version 1.16. Because this library is now maintained by a different company, import statements need to be adjusted as explained in their [JTS migration guide](https://github.com/locationtech/jts/blob/master/MIGRATION.md#jts-115). #75

### bugfixes

* Fix incorrect detection of deletions in queries using the ContributionView.
* Return the correct changeset id in case of concurrent updates on entities by different changesets.
* Fix crash while checking empty geometries resulting from erroneous OSM data. #57
* Fix a crash when trying to build polygons on partially incomplete OSM ways. #31
* Make importer work with "factory-settings" ignite system. #49

### new features

#### oshdb-api:

* Refactored how result aggregation by custom groupings works. It is now possible to [combine multiple](documentation/manual/aggregation.md#combining-multiple-aggregateby) aggregation groupings.
* Add methods to aggregate results by [sub-regions](documentation/manual/aggregation.md#aggregateByGeometry).
* Results of data extraction queries can now also be streamed and immediately post-processed. #19
* Include of [t-digest](https://github.com/tdunning/t-digest) algorithm to calculate estimated quantiles of results. #34
* All backends now support query timeouts. #47 #68

#### oshdb core

* Tweaked data format slightly to avoid overly full grid cells at low zoom levels. #130

### performance

#### oshdb-api

* Make the `getModificationTimestamps` method of OSHEntites faster, resulting in general performance improvement of every query, but especially when analyzing complex relations. #10
* Improve performance of bbox-in-polygon checking routines. #33
* Avoid unnecessary clipping of geometries. #66
* Improve building of complex multipolygon geometries. #111
* Many small performance improvements.

#### oshdb-tool

* Improve speed and functionality of the ETL module.

### other changes

* Source code is now released as open-source under _GNU Lesser General Public License version 3_.
* Dependencies are updated and reduced to the minimum. Also they are now declared in the modules where needed instead of the top level. You might therefore have to declare dependencies of your code explicitly when upgrading. #79 #5
* Drop most deprecated methods from OSHDB version 0.4.0
* More [examples and documentation](https://github.com/GIScience/oshdb/tree/master/documentation) are available.
* Many small bugfixes and improvements, especially for the Ignite backend. Ignite can now be considered stable and used to analyze a global data set.
* oshdb-api: renamed some methods (`where` filter → `osmTag` and `osmEntityFilter`, `osmTypes` filter → `osmType`) and refactored some methods to accept a wider range of input objects.
* `GeometryCollection` geometries are no longer ignored when calculating lengths or areas of features. #51
* Restructured core OSHDB data structures to be more flexible in upcoming version changes. #138
* Rename `getChangeset` method to `getChangesetId. #35

## 0.4.0

### breaking changes

* renamed bounding box class to `OSHDBBoundingBox` and change order of constructor parameters to `minLon, minLat, maxLon, maxLat` (was `minLon, maxLon, minLat, maxLat`), for example:
  * ~~`new BoundingBox(9.4,17.5,46.4,49.1)`~~ (0.3 code)
  * `new OSHDBBoundingBox(9.4,46.4,17.5,49.1)` (0.4)
* fixed naming scheme of oshdb related classes: `OSHDB` (as well as `OSM`, `OSH`) are written in upper case:
  * ~~`OSHDB_Ignite`~~ is now `OSHDBIgnite`
  * ~~`OSHDbGeometryBuilder`~~ is now `OSHDBGeometryBuilder`
* re-introduced `oshdb-util` module
  * some classes/packages have been moved to this module (e.g. the `geometry` and `time` helpers, as well as `tagInterpreter`, `tagTranslator`, `cellIterator`, `export`, …)
  * moved some functionality from `OSM`, `OSH` and `Grid` classes into the oshdb-util package (these methods are now found in more specific classes like the `OSHDBGeometryBuilder`)
* all timestamps related to oshdb objects (osm entities, etc.) return `OSHDBTimestamp` objects
  * if you need to access the raw unix timestamp (`long`) value, use `….getTimestamp().getRawUnixTimestamp()`
* introduce specific classes for osm tags (`OSMTag`), tag-keys (`OSMTagKey`), roles (`OSMRole`) and their oshdb counterparts (`OSHDBTag`, …)
  * these will be returned e.g. by `OSMEntity.getTags()` instead of the raw tag (`int`) ids – if you need these, use `OSMEntity.getRawTags()` or the appropriate getter function of the new objects (e.g. `OSHDBTag.getKey()`)
* the celliterator is now a (reusable) object instead of a set of static functions
* drop ~~`MEMBERLIST_CHANGE`~~ from analyzed contribution types of `OSMContribution`s

### new features

#### oshdb-core

* slightly tweaked database cell structure:
  * higher max-zoom level (up to level 15 instead of 12)
  * move content from almost empty cells to higher zoom levels
  * store objects in cells where they fit fully
  * (note: oshdb database files created with this format are backwards compatible with oshdb version 0.3.1 as long as the max-zoom parameter is kept at `12`)

#### oshdb-api

* add possibility to aggregate results by custom timestamp values
* add possibility to zero-fill custom aggregation indices
* add method to get unclipped geometries
* add method to get changeset id of OSMContribution objects
* add option to cache all data in memory when using the H2 backend for faster queries (at the cost of slower startup times and higher memory usage)

#### oshdb-tool

* rewritten importer (.osh.pbf → .oshdb) etl toolchain
* generated oshdb files include metadata about the included (osm) data

### performance

* oshdb-api: much faster processing of queries with polygonal areas of interest
* oshdb-api: implement lazy evaluation of geometries (large speed up for queries like `count()` that don't require entity geometries)
* oshdb-api: slightly more performant querying of data cells (both in H2 as well as Ignite backends)
* oshdb-util: significant performance improvements of internal `getModificationTimestamps` method (which is called once for every matching entity when using the oshdb-api)

#### usability improvements

* oshdb-api: includes `slf4j-simple` logging framework by default
* oshdb-util: more robust geometry building, e.g. incomplete relations or broken multipolygon relations are now returned as GeometryCollections

### other changes

* the git repository now includes the documentation and basic usage tutorial
* moved "parent" maven module outside of this repository
* improve code quality all over the place (reduced duplicate code, reduced or annotated type casting warnings, reduced usage of raw types)
* various bugfixes

## 0.3.1

* make java API methods work with updated "0.4" oshdb schema
* mark some methods as deprecated that are removed in 0.4

## 0.3.0

* added a new easy to use _"functional programming style"_ API abstraction level that works on local oshdb files as well as on an Ignite cluster
	* OSMEntitySnapshotMapper – iterates over entity "snapshots" at given timestamps
	* OSMContributionMapper – iterates over all OSM contributions for each entity (i.e. creation, modifications, deletion)
* (breaking) renamed properties of `CellIterator.iterateAll`'s results
* (breaking) renamed `Geo.distanceOf` to `Geo.lengthOf`
* moved osmatrix processing code into its own repository
* `CellIterator.iterateAll` now groups consecutive changes by changeset id
* added `TagTranslator` helper class
* switched logging system to slf4j
* improved javaDoc in a lot of places
* extended unit test coverage
* various bugfixes
* …

## 0.2.0

Starting point of changelogs.
First stable DB schema with data cells in multiple zoom levels.
Raw access to data is possible.
