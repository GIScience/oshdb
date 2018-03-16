## 0.5.0 SNAPSHOT (current master)

* oshdb-api: implemented aggregation by polygonal geometries
* …

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
