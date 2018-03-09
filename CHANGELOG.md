### 0.4.0 SNAPSHOT (current master)

* tweak database cell structure: higher max-zoom level (15 instead of 12), move content from almost empty cells to higher zoom levels, store objects in cells where they fit fully
* (breaking) renamed bounding box class to `OSHDBBoundingBox` and change order of constructor parameters to `minLon, minLat, maxLon, maxLat` (was `minLon, maxLon, minLat, maxLat`)
* (breaking) all timestamps related to oshdb objects (osm entities, etc.) return `OSHDBTimestamp` objects
* api: added possibility to aggregate results by custom timestamp values
* much faster processing of queries with polygonal areas of interest
* (breaking) celliterator is now an object instead of a set of static functions
* api: include `slf4j-simple` logging framework by default
* (breaking) the OSM, OSH and Grid-Classes held more methods than information. These methods where therefore outsourced to the oshdb-util package. Also some oshdb-api util-classes where moved.
* more robust geometry building (e.g. incomplete or broken relations are now returned as GeometryCollections)
* more performant querying of data cells (both in H2 as well as Ignite backends)
* api: add possibility to zero-fill custom aggregation indices
* api: implement lazy evaluation of geometries (speeding up queries like `count()` that don't require entity geometries)
* naming scheme of oshdb related classes: `OSHDB` (as well as `OSM`, `OSH`) are written in upper case
* moved "parent" maven module outside of this repository
* (breaking) introduce specific classes for osm tags, tag-keys, roles and their oshdb counterparts
* significant performance improvements of internal `getModificationTimestamps` method (which is called once for every matching entity)
* (breaking) drop MEMBERLIST_CHANGE from available contribution types
* (api) add method to get unclipped geometries
* add method to get changeset id of OSMContribution objects
* (api) add option to cache all data in memory when using the H2 backend
* improve code quality all over the place (reduced duplicate code, reduced or annotated type casting warnings, reduced usage of raw types)
* various bugfixes

### 0.3.1

* make java API methods work with updated "0.4" oshdb schema
* mark some methods as deprecated that are removed in 0.4

### 0.3.0

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

### 0.2.0

Starting point of changelogs.
First stable DB schema with data cells in multiple zoom levels.
Raw access to data is possible.
