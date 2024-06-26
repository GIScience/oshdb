Changelog
=========

## 1.2.3

### bugfixes

* Fix crash while building geometry of multipolygon with zero-node way members under certain circumstances ([#529])

### other changes

* skip/abort some query filter optimization if normalization exceeds reasonable limits ([#530])

[#529]: https://github.com/GIScience/oshdb/pull/529
[#530]: https://github.com/GIScience/oshdb/issues/530


## 1.2.2

### other changes

* upgrade dependencies: ignite to v2.16, guava to v33, caffeine to v3.1.8 ([#521]), postgresql to v42.6 ([#523])
* rename development branch to `main` ([#522])

[#521]: https://github.com/GIScience/oshdb/pull/521
[#522]: https://github.com/GIScience/oshdb/pull/522
[#523]: https://github.com/GIScience/oshdb/pull/523


## 1.2.1

* Fix performance degradation in the streaming endpoints when running on Ignite using the `AFFINITY_CALL` backend ([#516])

[#516]: https://github.com/GIScience/oshdb/pull/516


## 1.2.0

### new features

* `OSMEntitySnapshot` now also returns the `lastContributionTimestamp` for each snapshot ([#495])

### performance improvements

* Significantly improve performance of queries which use filters and don't use `flatMap` ([#511])

### bugfixes

* `areaOfInterest(bbox)` should not override previously set bbox ([#512])
* Fix a (rare) bug which can cause relation-versions with no members to be misinterpreted ([#510])

### other changes

* `CellIterator` is now decoupled from implementation of the "Grid" ([#495])
* Enhance geometry builder with methods to generate geometries from entities when their members are already pre-resolved for a specific timestamp ([#501])
* Upgrade "ohsome parent" repository to version 2.14.0 ([#513])

[#495]: https://github.com/GIScience/oshdb/pull/495
[#501]: https://github.com/GIScience/oshdb/pull/501
[#510]: https://github.com/GIScience/oshdb/pull/510
[#511]: https://github.com/GIScience/oshdb/pull/511
[#512]: https://github.com/GIScience/oshdb/pull/512
[#513]: https://github.com/GIScience/oshdb/pull/513


## 1.1.2

* update ignite dependency to 2.15.0 ([#506])

[#506]: https://github.com/GIScience/oshdb/pull/506


## 1.1.1

* update ignite dependency to [2.14.0-heigit1] ([#491])

[#491]: https://github.com/GIScience/oshdb/pull/491
[2.14.0-heigit1]: https://github.com/GIScience/ignite/compare/2.14.0...GIScience:ignite:2.14.0-heigit1

## 1.1.0

### breaking changes

* java 17 is now a requirement ([#488])

### other changes

* dependency version updates ([#488])

[#488]: https://github.com/GIScience/oshdb/pull/488


## 1.0.0

### breaking changes

* remove class `oshdb-util:util.time.TimestampFormatter` ([#419])
* drop "old" etl module ([#447])
* remove Comparable interface from OSMEntity ([#449])
* remove deprecated filter methods `osmType`, `osmTag` and `osmEntityFlter` (which were replaced by [OSHDB `filter`s](https://github.com/GIScience/oshdb/blob/0.7/documentation/manual/filters.md#oshdb-filter)) ([#451])
* `OSMType.toString` returns lower-case name. ([#459])
* rework of handling separate keytables, see [#470] ([#470])
* `prefix` is now a mandatory parameter for `OSHDBJdbc` and `OSHDBIgnite`. ([#478])
* `OSHDBH2` no longer supports `DataSource`s as argument. ([#478])

### new features

* allow to flexibly combine (automatic) aggregation methods (like `aggregateByGeometry(…)` or `aggregateByTimestamp()`) with each other and with `filter` or `map`/`flatMap`, regardless of the order of the applied operations ([#451])
* add new OSHDB filters: `perimeter`, `geometry.vertices`, `geometry.outers`, `geometry.inners`, `geometry.roundness` and `geometry.squareness` ([#436])
* add OSHDB-helpers module providing two helpers (`OSHDBDriver` and `OSHDBApplication`) to simplify database setup ([#474])

### bugfixes

* fix building valid geometries for multipolygons with shells which share a single point ([#424])
* change geometry filters to be based on full (unclipped) geometries ([#433])
* make sure area computation never returns negative results (instead zero is returned for the invalid geometries which previously resulted in negative values) ([#438])

### other changes

* remove deprecated method `OSHEntity.getRawTagKeys` ([#441])
* remove deprecated method `OSMEntity.getRawTags` ([#443])
* remove `OSMMember.getRawRoleId` ([#453])
* refactor `OSHDBRole` and move to oshdb-core ([#453])
* update jts dependency to version 1.18.2
* update ignite dependency to version 2.14.0 ([#459], [#467])
* add natural order to `OSHDBTag` ([#454])
* throw exception when invalid timestamp strings are supplied to the MapReducer ([#260])

[#260]: https://github.com/GIScience/oshdb/issues/260
[#419]: https://github.com/GIScience/oshdb/pull/419
[#424]: https://github.com/GIScience/oshdb/pull/424
[#433]: https://github.com/GIScience/oshdb/issues/433
[#436]: https://github.com/GIScience/oshdb/pull/436
[#438]: https://github.com/GIScience/oshdb/pull/438
[#441]: https://github.com/GIScience/oshdb/pull/441
[#443]: https://github.com/GIScience/oshdb/pull/443
[#447]: https://github.com/GIScience/oshdb/pull/447
[#449]: https://github.com/GIScience/oshdb/pull/449
[#451]: https://github.com/GIScience/oshdb/pull/451
[#453]: https://github.com/GIScience/oshdb/pull/453
[#454]: https://github.com/GIScience/oshdb/pull/454
[#459]: https://github.com/GIScience/oshdb/pull/459
[#467]: https://github.com/GIScience/oshdb/pull/467
[#470]: https://github.com/GIScience/oshdb/pull/470
[#474]: https://github.com/GIScience/oshdb/pull/474
[#478]: https://github.com/GIScience/oshdb/pull/478


## 0.7.2

### bugfixes

* fix a bug which causes queries using the geometry filters `length:` and `area:` to fail when executed on an ignite cluster ([#426])
* fix a bug which cause ComputeJobs to keep processing for a while despite they are already canceled ([#428])

[#426]: https://github.com/GIScience/oshdb/pull/426
[#428]: https://github.com/GIScience/oshdb/pull/428

## 0.7.1

* fix a bug where contribution-based filters are not applied when used in an and/or operation. ([#409])
* fix a regression in `0.7.0` which prevents queries from being executed on an ignite cluster backend ([#417])

[#409]: https://github.com/GIScience/oshdb/issues/409
[#417]: https://github.com/GIScience/oshdb/pull/417


## 0.7.0

### breaking changes

* reorganize java packages, moving them from `org/heigit/bigspatialdata` to `org/heigit/ohsome`
* integrate [ohsome-filter](https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/libs/ohsome-filter) module fully into this repository, renaming it to `oshdb-filter` ([#306])
* rename and move submodules of `oshdb-tool` ([#384])
* rename some classes, methods and enum constants; move some classes/interfaces ([#369], [#374])
* move the oshdb-api's ignite backend implementation into its own submodule `oshdb-api-ignite` ([#387])
* refactoring of internal coordinate representation form long to int, mainly `OSHDBBoundingBox`, `OSMNode`, `OSHEntity` ([#395])

> See the _upgrading from 0.6_ section below for instructions how to update your code according to these breaking changes.

### new features

* enhance functionality of oshdb-filter: add new `changeset: <id|(ids)>` and (optional) `contributor: <id|(ids)>` filters ([#380])

### performance improvements

* replace an unnecessarily used Map with a more lightweight implementation using a List. ([#352])

### other changes

* make sure predicate-filters are always serializable ([#353])
* improve maintainability of parts of important central processing algorithms for determining entity modification history: refactoring improves code structure, adds inline documentation and enhances test coverage ([#327])
* reorganize and update ohsome parent module, requires maven version 3.6 or higher ([#360], [#375])
* add new interfaces `OSHDBTemporal` and `OSHDBBoundable` ([#369])
* major improvements to code style guide adherence; fix some potential bugs found in static code analyis ([#374])
* upgrade apache ignite to version 2.10.0 ([#386])
* drop deprecated oshdb-api methods `where` and `osmTypes` ([#308])
* clarify documentation of OSMContributionView results ([#292])
* if cluster state is not active an exception will thrown ([#337])

### bugfixes

* when filtering for `geometry:other`: also consider GeometryCollections occurring as a side effect of clipping ([#338])

### upgrading from 0.6

* Since the java package namespace was updated, you need to change your imports to the new path, e.g.:
  ```java
  import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
  import org.heigit.ohsome.oshdb.api.db.OSHDBH2;
  ```
* If you used the “ohsome filter” functionality from OSHDB version 0.6 and imported one or more classes from the ohsome filter module, you would need to adjust the package names from `org.heigit.ohsome.filter` to `org.heigit.ohsome.oshdb.filter`.
* the following enum constants, methods, classes and packages were renamed or moved:
  |     | old | new |
  | --- | --- | --- |
  | moved and renamed module | `oshdb-tool/etl` | `oshdb-etl` |
  | moved and renamed module | `oshdb-tool/oshpbf-parser` | `oshdb-oshpbf-parser` |
  | renamed method | `OSMWay.getRef()` | `OSMWay.getMember()` |
  | renamed method | `OSMWay.getRefEntities(long)` | `OSMWay.getMemberEntities(long)` |
  | renamed method | `OSHDBTimestamp.getRawUnixTimestamp()` | `OSHDBTimestamp.getEpochSecond()` |
  | moved class | `oshdb.util.OSHDBTimestamp` | `oshdb.OSHDBTimestamp` |
  | moved class | `oshdb.util.OSHDBBoundingBox` | `oshdb.OSHDBBoundingBox` |
  | deprecated method | `new OSHDBBoundingBox(double, double, double, double)` | replaced by `OSHDBBoundingBox.bboxWgs84Coordinates(...)`|
  | renamed method | `OSHDBBoundingBox.get(Max/Min)(Lon/Lat)Long()` | `OSHDBBoundingBox.get(Max/Min)(Longitude/Latitude)()` |
  | moved class | `oshdb.util.OSHDBTag` | `oshdb.OSHDBTag` |
  | moved class | `CellIterator.OSHEntityFilter` | `oshdb-util/oshdb.osh.OSHEntityFilter` |
  | moved class | `CellIterator.OSMEntityFilter` | `oshdb-util/oshdb.osm.OSMEntityFilter` |
  | moved class | `oshdb-api.generic.function` | `oshdb-util/oshdb.util.function` |
  | renamed enum constants | `ComputeMode.LocalPeek`, `….ScanQuery`, `….AffinityCall` | `ComputeMode.LOCAL_PEEK`, `….SCAN_QUERY`, `….AFFINITY_CALL` |
  | moved class | `org.heigit.ohsome.oshdb.api.object.OSMContribution` | `org.heigit.ohsome.oshdb.util.mappable.OSMContribution` |
  | moved class | `org.heigit.ohsome.oshdb.api.object.OSMEntitySnapshot` | `org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot` |
* if you are using the ignite backend, you need to add the following dependency to your project:
  ```xml
    <dependency>
      <groupId>org.heigit.ohsome</groupId>
      <artifactId>oshdb-api-ignite</artifactId>
      <version>0.7.0</version>
    </dependency>
  ```


[#292]: https://github.com/GIScience/oshdb/issues/292
[#306]: https://github.com/GIScience/oshdb/pull/306
[#308]: https://github.com/GIScience/oshdb/issues/308
[#327]: https://github.com/GIScience/oshdb/issues/327
[#337]: https://github.com/GIScience/oshdb/issues/337
[#338]: https://github.com/GIScience/oshdb/issues/338
[#352]: https://github.com/GIScience/oshdb/pull/352
[#353]: https://github.com/GIScience/oshdb/pull/353
[#360]: https://github.com/GIScience/oshdb/pull/360
[#369]: https://github.com/GIScience/oshdb/pull/369
[#374]: https://github.com/GIScience/oshdb/pull/374
[#375]: https://github.com/GIScience/oshdb/pull/375
[#380]: https://github.com/GIScience/oshdb/pull/380
[#384]: https://github.com/GIScience/oshdb/pull/384
[#386]: https://github.com/GIScience/oshdb/issues/386
[#387]: https://github.com/GIScience/oshdb/pull/387
[#395]: https://github.com/GIScience/oshdb/pull/395


## 0.6.4

* make `aggregateByGeometry` robust against broken geometries causing topology exceptions (regression in version 0.6) ([#362])
* fix a crash caused by the use of non-existent tags in OSHDB filters ([#363])

[#362]: https://github.com/GIScience/oshdb/pull/362
[#363]: https://github.com/GIScience/oshdb/pull/363


## 0.6.3

* fix an infinite loop caused by certain invalid multipolygons with touching inner rings which are partially incomplete. ([#343])

[#343]: https://github.com/GIScience/oshdb/pull/343


## 0.6.2

* don't change cluster state when executing queries on an Ignite cluster. ([#335])
* fix bug when building multipolygon geometries with certain invalid inner ring configurations (e.g. duplicate inner rings). ([#334])

[#334]: https://github.com/GIScience/oshdb/issues/334
[#335]: https://github.com/GIScience/oshdb/pull/335


## 0.6.1

* fix a crash caused when _oshdb-filters_ are used in `groupByEntity` queries. ([#321])
* fix a crash when relations reference redacted ways. ([#325])

[#321]: https://github.com/GIScience/oshdb/issues/321
[#325]: https://github.com/GIScience/oshdb/issues/325


## 0.6.0

### breaking changes

* reorganize maven packages: rename group parent to ohsome-parent, rename local parent to oshdb-parent, and change groupId to `org.heigit.ohsome`. ([#234], [#257])

When switching to the OSHDB version 0.6 you need to adapt your `pom.xml` to the new groupId, e.g.:
```xml
<dependency>
  <groupId>org.heigit.ohsome</groupId>
  <artifactId>oshdb-api</artifactId>
  <version>0.6.0</version>
</dependency>
```

* Timestamp parser class renamed to `IsoDateTimeParser` from `ISODateTimeParser` and adjust how input timestamps (e.g. in `MapReducer.timestamps()`) are handled: only the UTC time zone identifier `Z` is supported. ([#265])
* Removed `UNKOWN` from the `OSMType` enumeration class. ([#239])

### new features

* improve accuracy of built-in geometry helper functions which calculate the geodesic lengths and areas of OSM geometries. ([#193])
* integrate [ohsome filter](https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/libs/ohsome-filter) functionality. ([#253])

### performance improvements

* better handling of OSM multipolygons with touching inner rings. This improves performance considerably in some cases (especially large complex multipolygons). ([#249])
* improve performance of `aggregateByGeometry` queries. ([#272])
* improve performance of geometry-building of relations with a huge number members. ([#287])

### bugfixes and other changes

* compatibility fix to allow building of javadoc under Java 11
* fix bug where in some cases, instead of an OSHDBTimeoutException an IgniteException was thrown. ([#258])
* various code style and code quality improvements
* the OSHDB is now published on Zenodo for easier citation using the DOI `10.5281/zenodo.4146991`

[#193]: https://github.com/GIScience/oshdb/issues/193
[#234]: https://github.com/GIScience/oshdb/issues/234
[#239]: https://github.com/GIScience/oshdb/issues/239
[#249]: https://github.com/GIScience/oshdb/issues/249
[#253]: https://github.com/GIScience/oshdb/issues/253
[#257]: https://github.com/GIScience/oshdb/issues/257
[#258]: https://github.com/GIScience/oshdb/issues/258
[#265]: https://github.com/GIScience/oshdb/issues/265
[#272]: https://github.com/GIScience/oshdb/issues/272
[#287]: https://github.com/GIScience/oshdb/issues/287


## 0.5.11

* fix a crash when relations reference redacted ways (backported from version 0.6.1). ([#325])


## 0.5.10

* update Ignite to version 2.9.0


## 0.5.9

* update Ignite to version 2.8.0


## 0.5.8

* fix a regression in 0.5.7 when using OSHDB on Ignite, restoring binary compatibility when running clients with different oshdb 0.5 versions in parallel. ([#235])
* fix a bug in the geometry builder utility causing exceptions to be thrown for certain invalid OSM multipolygons. ([#231])

[#231]: https://github.com/GIScience/oshdb/issues/231
[#235]: https://github.com/GIScience/oshdb/issues/235


## 0.5.7

* fix regression in version 0.5.6 which made queries run slowly when executed on ignite using the (default) “LocalPeek” backend. ([#229])
* throw an exception if the `aggregateByTimestamps(callback)` is fed with timestamps outside the query's time range. Before this change, this used to cause unspecific exceptions or undefined behaviour. ([#158])
* improve querying of tag from keytables. ([#224])
* minor bug fixes and coding clean up. ([#216], [#198], [#206])

[#158]: https://github.com/GIScience/oshdb/issues/158
[#198]: https://github.com/GIScience/oshdb/issues/198
[#206]: https://github.com/GIScience/oshdb/issues/206
[#216]: https://github.com/GIScience/oshdb/issues/216
[#224]: https://github.com/GIScience/oshdb/issues/224
[#229]: https://github.com/GIScience/oshdb/issues/229


## 0.5.6

* fix how osm-type filters work when called multiple times: now, like with other filters, osm entity must match all supplied type filters. ([#157])
* osmTag filters are more flexible: when used with a list of tags, it now accepts also `tagKey=*` statements (which can be mixed with `key=value` statements as before). ([#209])
* fix a bug where polygonal areas of interest would throw an exception in some (rare) edge cases. ([#204])

[#157]: https://github.com/GIScience/oshdb/issues/157
[#204]: https://github.com/GIScience/oshdb/issues/204
[#209]: https://github.com/GIScience/oshdb/issues/209


## 0.5.5

* improved performance of data streaming queries on ignite (using AffinityCall backend).
* make monthly time intervals more intuitive to use. ([#201])

[#201]: https://github.com/GIScience/oshdb/issues/201


## 0.5.4

* fix a regression where broken referential integrity in OSM data causes a crash during geometry building


## 0.5.3

* update Ignite to version 2.7.5


## 0.5.2

* fix calculation of insertIds / entities stored in too high zoom levels, which resulted in partially missing data in some queries ([#183])
* prevent crashes while building certain invalid multipolygon relation geometries ([#179])

[#179]: https://github.com/GIScience/oshdb/issues/179
[#183]: https://github.com/GIScience/oshdb/issues/183


## 0.5.1

* oshdb-util: Fix a bug in `Geo.areaOf` when applied to polygons with holes. Before this fix, the method erroneously skipped the first inner ring when calculating the total area of a polygon. This affected geometries constructed from OSM multipolygon relations.
* oshdb-util: Implemented `QUARTERLY`, `WEEKLY`, `DAILY`, and `HOURLY` as additional time intervals.


## 0.5.0

### breaking changes

* JTS library was updated to version 1.16. Because this library is now maintained by a different company, import statements need to be adjusted as explained in their [JTS migration guide](https://github.com/locationtech/jts/blob/master/MIGRATION.md#jts-115). ([#75])

[#75]: https://github.com/GIScience/oshdb/issues/75

### bugfixes

* Fix incorrect detection of deletions in queries using the ContributionView.
* Return the correct changeset id in case of concurrent updates on entities by different changesets.
* Fix crash while checking empty geometries resulting from erroneous OSM data. ([#57])
* Fix a crash when trying to build polygons on partially incomplete OSM ways. ([#31])
* Make importer work with “factory-settings” ignite system. ([#49])

[#31]: https://github.com/GIScience/oshdb/issues/31
[#49]: https://github.com/GIScience/oshdb/issues/49
[#57]: https://github.com/GIScience/oshdb/issues/57

### new features

#### oshdb-api:

* Refactored how result aggregation by custom groupings works. It is now possible to [combine multiple](documentation/manual/aggregation.md#combining-multiple-aggregateby) aggregation groupings.
* Add methods to aggregate results by [sub-regions](documentation/manual/aggregation.md#aggregateByGeometry).
* Results of data extraction queries can now also be streamed and immediately post-processed. ([#19])
* Include of [t-digest](https://github.com/tdunning/t-digest) algorithm to calculate estimated quantiles of results. ([#34])
* All backends now support query timeouts. ([#47], [#68])

#### oshdb core

* Tweaked data format slightly to avoid overly full grid cells at low zoom levels. ([#130])

### performance

#### oshdb-api

* Make the `getModificationTimestamps` method of OSHEntities faster, resulting in general performance improvement of every query, but especially when analyzing complex relations. ([#10])
* Improve performance of bbox-in-polygon checking routines. ([#33])
* Avoid unnecessary clipping of geometries. ([#66])
* Improve building of complex multipolygon geometries. ([#111])
* Many small performance improvements.

#### oshdb-tool

* Improve speed and functionality of the ETL module.

### other changes

* Source code is now released as open-source under _GNU Lesser General Public License version 3_.
* Dependencies are updated and reduced to the minimum. Also, they are now declared in the modules where needed instead of the top level. You might therefore have to declare dependencies of your code explicitly when upgrading. ([#79], [#5])
* Drop most deprecated methods from OSHDB version 0.4.0
* More [examples and documentation](https://github.com/GIScience/oshdb/tree/main/documentation) are available.
* Many small bugfixes and improvements, especially for the Ignite-backend. Ignite can now be considered stable and used to analyze a global data set.
* oshdb-api: renamed some methods (`where` filter → `osmTag` and `osmEntityFilter`, `osmTypes` filter → `osmType`) and refactored some methods to accept a wider range of input objects.
* `GeometryCollection` geometries are no longer ignored when calculating lengths or areas of features. ([#51])
* Restructured core OSHDB data structures to be more flexible in upcoming version changes. ([#138])
* Rename `getChangeset` method to `getChangesetId. ([#35])

[#5]: https://github.com/GIScience/oshdb/issues/5
[#10]: https://github.com/GIScience/oshdb/issues/10
[#19]: https://github.com/GIScience/oshdb/issues/19
[#33]: https://github.com/GIScience/oshdb/issues/33
[#34]: https://github.com/GIScience/oshdb/issues/34
[#35]: https://github.com/GIScience/oshdb/issues/35
[#47]: https://github.com/GIScience/oshdb/issues/47
[#51]: https://github.com/GIScience/oshdb/issues/51
[#66]: https://github.com/GIScience/oshdb/issues/66
[#68]: https://github.com/GIScience/oshdb/issues/68
[#79]: https://github.com/GIScience/oshdb/issues/79
[#111]: https://github.com/GIScience/oshdb/issues/111
[#130]: https://github.com/GIScience/oshdb/issues/130
[#138]: https://github.com/GIScience/oshdb/issues/138


## 0.4.0

### breaking changes

* renamed bounding box class to `OSHDBBoundingBox` and change order of constructor parameters to `minLon, minLat, maxLon, maxLat` (was `minLon, maxLon, minLat, maxLat`), for example:
  * ~~`new BoundingBox(9.4,17.5,46.4,49.1)`~~ (0.3 code)
  * `new OSHDBBoundingBox(9.4,46.4,17.5,49.1)` (0.4)
* fixed naming scheme of OSHDB related classes: `OSHDB` (as well as `OSM`, `OSH`) are written in upper case:
  * ~~`OSHDB_Ignite`~~ is now `OSHDBIgnite`
  * ~~`OSHDbGeometryBuilder`~~ is now `OSHDBGeometryBuilder`
* re-introduced `oshdb-util` module
  * some classes/packages have been moved to this module (e.g. the `geometry` and `time` helpers, as well as `tagInterpreter`, `tagTranslator`, `cellIterator`, `export`, …)
  * moved some functionality from `OSM`, `OSH` and `Grid` classes into the oshdb-util package (these methods are now found in more specific classes like the `OSHDBGeometryBuilder`)
* all timestamps related to OSHDB objects (osm entities, etc.) return `OSHDBTimestamp` objects
  * if you need to access the raw unix timestamp (`long`) value, use `….getTimestamp().getRawUnixTimestamp()`
* introduce specific classes for osm tags (`OSMTag`), tag-keys (`OSMTagKey`), roles (`OSMRole`) and their OSHDB counterparts (`OSHDBTag`, …)
  * these will be returned e.g. by `OSMEntity.getTags()` instead of the raw tag (`int`) ids – if you need these, use `OSMEntity.getRawTags()` or the appropriate getter function of the new objects (e.g. `OSHDBTag.getKey()`)
* the celliterator is now a (reusable) object instead of a set of static functions
* drop ~~`MEMBERLIST_CHANGE`~~ from analyzed contribution types of `OSMContribution`s

### new features

#### oshdb-core

* slightly tweaked database cell structure:
  * higher max-zoom level (up to level 15 instead of 12)
  * move content from almost empty cells to higher zoom levels
  * store objects in cells where they fit fully
  * (note: OSHDB database files created with this format are backwards compatible with oshdb version 0.3.1 as long as the max-zoom parameter is kept at `12`)

#### oshdb-api

* add possibility to aggregate results by custom timestamp values
* add possibility to zero-fill custom aggregation indices
* add method to get unclipped geometries
* add method to get changeset id of OSMContribution objects
* add option to cache all data in memory when using the H2 backend for faster queries (at the cost of slower startup times and higher memory usage)

#### oshdb-tool

* rewritten importer (.osh.pbf → .oshdb) etl toolchain
* generated OSHDB files include metadata about the included (osm) data

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
* moved “parent” maven module outside this repository
* improve code quality all over the place (reduced duplicate code, reduced or annotated type casting warnings, reduced usage of raw types)
* various bugfixes


## 0.3.1

* make java API methods work with updated “0.4” OSHDB schema
* mark some methods as deprecated that are removed in 0.4


## 0.3.0

* added a new easy to use _“functional programming style”_ API abstraction level that works on local OSHDB files as well as on an Ignite cluster
	* OSMEntitySnapshotMapper – iterates over entity “snapshots” at given timestamps
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
