Filtering OSM Data
==================

Often one doesn't want to investigate the whole OSM data set at once, but only a specific part of it. For example, all the OSM data in a given region, or all OSM objects that have a given [tag](https://wiki.openstreetmap.org/wiki/Tags), [type](https://wiki.openstreetmap.org/wiki/Elements), or other property of the respective OSM entity.

For this, the [`MapReducer`](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html) provides a variety of filtering methods which allow one to select any subset of the OSM data. Multiple filters can be applied after each other. The result will then contain any OSM elements that match **all** of the specified filters.


areaOfInterest
--------------

This defines the region where the query should be restricted on. It can be either a [bounding box](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#areaOfInterest(org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox)) ([`OSHDBBoundingBox`](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/util/OSHDBBoundingBox.html)) or any [(polygonal) JTS geomety](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#areaOfInterest(P)) such as a Polygon or MultiPolygon.

The ouput of this filter will keep only OSM entities whose geometry lie within or which intersect the given areaOfInterest. This inclused also OSM entities for which that none of their child elements lie within the given area of interest.

> For example, a large forest polygon in OSM that completely encompasses a small area of interest _is_ returned by the OSHDB API.

The resulting geometries produced by the different OSHDB [views](views.md) are by default clipped to the specified area of interest. This makes it possible to directly calculate the length or area of linear or polygonal OSM features within the given query region, without having to consider the fact that some of the features might only partially lie within the region. It is, at the same time, still possible to access full extent of the respective OSM features' [unclipped](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/object/OSMEntitySnapshot.html#getGeometryUnclipped()) [geometries](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/object/OSMContribution.html#getGeometryUnclippedBefore()).

The OSHDB is able to cope well even with complex polygons that have many vertices, but keep in mind that using simpler geometries will generally result in higher query performance: For example a bounding-box query is executed slightly faster than a polygon-areaOfInterest query with a rectangular polygon. 

<!-- todo: link to blog post with spatial filtering performance benchmarks -->

timestamps
----------

This specifies the time range and time subdivisions for the OSHDB query. Accepts [one](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#timestamps(java.lang.String)) [or](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#timestamps(java.lang.String,java.lang.String)) [more](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#timestamps(java.lang.String,java.lang.String,java.lang.String...)) [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) formatted dates (given in the [UTC](https://en.wikipedia.org/wiki/Coordinated_Universal_Time) timezone). Depending on the used OSHDB [view](views.md), these timestamps are interpreted slightly differently: When using the **snapshot** view, the given timestamps define the dates at which the snapshots of the OSM entities are taken. When using the **contribution** view, all modifications to the OSM entities are returned that lie within the time range defined by the given first and last timestamp, while any further timestamps can be used later to [aggregate](aggregation.md) results into finer time intervals.

There exists also a [method](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#timestamps(java.lang.String,java.lang.String,org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps.Interval)) to define common regularly spaced time intervals within a time range, e.g. a monthly time interval between two dates.

osmType
-------

Filters OSM entities by their OSM data type. These types are: node, way and relation, represented in the OSHDB by the enumeration [OSMType](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/osm/OSMType.html). In a MapReducer, the [method](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#osmType(java.util.EnumSet)) [`osmType`](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#osmType(org.heigit.bigspatialdata.oshdb.osm.OSMType,org.heigit.bigspatialdata.oshdb.osm.OSMType...)) allows to filter objects that are of a specific OSM data type.

osmTag
------

This is the most commonly used filter to select a certain subset of OSM data. It returns only OSM entities that match a given OSM tag. Elements can be filtered by either the presence of a tag [key](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#osmTag(java.lang.String)), the presence of an [exact tag](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#osmTag(java.lang.String,java.lang.String)), or other matching condition that tests the presence of specific tags or tag combinations.
<!-- list and document all versions: collection(tag), key+collection(values), etc. -->

osmEntityFilter
---------------

It is possible to [define custom filtering functions](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#osmEntityFilter(org.heigit.bigspatialdata.oshdb.api.generic.function.SerializablePredicate)), that take an OSM entity object as input and decide wether each individual entity should be included in the result or not by returning a boolean value.

_ohsome_ filter
---------------

An easy way to provide complex [`filter`s](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#filter(java.lang.String)) is through the functionality of [ohsome filters](https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/libs/ohsome-filter#readme), which allow one to define osm data filter in a human readable syntax. With these one can combine several tag, type and geometry filters with arbitrary boolean operators.

_lambda_ filter
---------------

It is possible to define [`filter` functions](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#filter(org.heigit.bigspatialdata.oshdb.api.generic.function.SerializablePredicate)) that can sort out values after they already have been transformed in a [map](map-reduce.md#map) step.

Note that it is usually best to use the less flexible, but more performant OSM data filters described above wherever possible, as they can reduce the amount of data to be iterated over right from the start of the query. This is because while the basic filters are applied at the beginning of each query on the full OSM history data directly, the filter lambda functions are only executed after the data has already been computed and transformed.
