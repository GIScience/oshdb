## 0.5.0 SNAPSHOT (current master)

### breaking changes
* general
  - JTS was updated to 1.16.1. Although this might seem like a minor change it breakes all imports because the suite is now [hosted by LocationTach](https://github.com/locationtech/jts/blob/master/MIGRATION.md#jts-115)
* oshdb-api
  - Rename methods
    - `where` filter on MapReducer-Objects was renamed to `osmTag` and `osmEntityFilter`
    - `osmTypes` filter to `osmType`
    - move `OSHEntites` to `osh`-Package

### bugfixes
* oshdb-api
  - Fix [incorrect recognition](https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/oshdb/commit/2cce34c97e564f1035374f7ddb43d22b7d259f84?view=parallel#note_3806) of deletions
  - Fix [return of wrong changeset](https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/oshdb/commit/40c837cec4d590fe73693b092e1976b3c74d515e) for concurrent updates on entities by different changesets
  - Fix crash if [OSM-Geometry](https://github.com/GIScience/oshdb/pull/57) could not be created
* oshdb
  - Fix [false `isArea`-check](https://github.com/GIScience/oshdb/pull/31) on incomplete OSM-Data
* oshdb-tool etl
  - Make [importer](https://github.com/GIScience/oshdb/issues/49) usable for "factory-settings"-ignite

### new features
* oshdb-api:
  - [Aggregation by polygonal geometries](https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/oshdb/issues/49) is now possible and can be combined with e.g. timestamp aggregations. Using the `.nest()` method on the output will deflade the result.
  - Refactored some methods to accept a wider range of collection objects
  - Instead of collecting MapReducer-results into a collection they can now be [streamed](https://github.com/GIScience/oshdb/pull/19) and immediately post-processed
  - Implmentation of t-digest to calculate [quantiles](https://github.com/GIScience/oshdb/pull/34) for results in a sparse and therefore performant manner while still being very accurate
  - All backends now support timeouts to [cancel calculations](https://github.com/GIScience/oshdb/pull/47) that exeed a certain limit
  - GeometryCollections are no [longer ignored](https://github.com/GIScience/oshdb/pull/51) when calculating length or area of features

### performance
* oshdb-api
  - Many small and medium improvements
  - Filtering relevant entities with a [spatial index](https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/oshdb/commit/6ed0164c489c58470847f82af879ad806351863e) before processing them
  - Getting the ModificationTimestamps of OSHEntites is now [faster](https://github.com/GIScience/oshdb/pull/10), especially on relations
  - Polygon-BoundingBoxes are now [A LOT faster](https://github.com/GIScience/oshdb/pull/33)
  - Creating geometries from OSMEntites is now [A LOT faster](https://github.com/GIScience/oshdb/pull/111)
* oshdb-tool etl
  - Improve speed and functionality of ETL

### other changes
* general:
  - The code is now released as open-source under [GPLv3](https://github.com/GIScience/oshdb/blob/master/LICENSE)
  - Dependencies are updated and reduced to the minimum. Also they are now declared where needed instead of the toplevel pom. You might therefore have to declare dependencies of your code explicitely when upgrading.
  - Most deprecated methods form 0.4.0 are now gone
  - More [examples and documentation](https://github.com/GIScience/oshdb/tree/master/documentation) are available
  - Many other bugfixes and improvements, especially for Ignite backends. Ignite can now be considered stable on a global cluster.
  - The data format was [changed](https://github.com/GIScience/oshdb/pull/130). Gridcells are now overlapping allowing for Entities to be stored in lower zoomlevels when overlapping with a border.

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
