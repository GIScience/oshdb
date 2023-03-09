Filtering OSM Data
==================

Often one doesn't want to investigate the whole OSM data set at once, but only a specific part of it. For example, all the OSM data in a given region, or all OSM objects that have a given [tag](https://wiki.openstreetmap.org/wiki/Tags), [type](https://wiki.openstreetmap.org/wiki/Elements), or other property of the respective OSM entity.

For this, the [`MapReducer`](https://docs.ohsome.org/java/oshdb/1.1.0/aggregated/org/heigit/ohsome/oshdb/api/mapreducer/MapReducer.html) provides a variety of filtering methods which allow one to select any subset of the OSM data. Multiple filters can be applied after each other. The result will then contain any OSM elements that match **all** of the specified filters.


areaOfInterest
--------------

This defines the region where the query should be restricted on. It can be either a [bounding box](https://docs.ohsome.org/java/oshdb/1.1.0/aggregated/org/heigit/ohsome/oshdb/api/mapreducer/MapReducer.html#areaOfInterest(org.heigit.ohsome.oshdb.OSHDBBoundingBox)) ([`OSHDBBoundingBox`](https://docs.ohsome.org/java/oshdb/1.1.0/aggregated/org/heigit/ohsome/oshdb/OSHDBBoundingBox.html)) or any [(polygonal) JTS geometry](https://docs.ohsome.org/java/oshdb/1.1.0/aggregated/org/heigit/ohsome/oshdb/api/mapreducer/MapReducer.html#areaOfInterest(P)) such as a Polygon or MultiPolygon.

The output of this filter will keep only OSM entities whose geometry lie within or which intersect the given areaOfInterest. This included also OSM entities for which that none of their child elements lie within the given area of interest.

> For example, a large forest polygon in OSM that completely encompasses a small area of interest _is_ returned by the OSHDB API.

The resulting geometries produced by the different OSHDB [views](views.md) are by default clipped to the specified area of interest. This makes it possible to directly calculate the length or area of linear or polygonal OSM features within the given query region, without having to consider the fact that some features might only partially lie within the region. It is, at the same time, still possible to access full extent of the respective OSM features' [unclipped](https://docs.ohsome.org/java/oshdb/1.1.0/aggregated/org/heigit/ohsome/oshdb/util/mappable/OSMEntitySnapshot.html#getGeometryUnclipped()) [geometries](https://docs.ohsome.org/java/oshdb/1.1.0/aggregated/org/heigit/ohsome/oshdb/util/mappable/OSMContribution.html#getGeometryUnclippedBefore()). You can find further information in the section about how the OSHDB [builds geometries](geometries.md) from OSM data.

The OSHDB is able to cope well even with complex polygons that have many vertices as areas of interest, but keep in mind that using simpler geometries will generally result in higher query performance: For example a bounding-box query is executed slightly faster than a polygon-areaOfInterest query with a rectangular polygon.

<!-- todo: link to blog post with spatial filtering performance benchmarks -->

timestamps
----------

This specifies the time range and time subdivisions for the OSHDB query. Accepts [one](https://docs.ohsome.org/java/oshdb/1.1.0/aggregated/org/heigit/ohsome/oshdb/api/mapreducer/MapReducer.html#timestamps(java.lang.String)) [or](https://docs.ohsome.org/java/oshdb/1.1.0/aggregated/org/heigit/ohsome/oshdb/api/mapreducer/MapReducer.html#timestamps(java.lang.String,java.lang.String)) [more](https://docs.ohsome.org/java/oshdb/1.1.0/aggregated/org/heigit/ohsome/oshdb/api/mapreducer/MapReducer.html#timestamps(java.lang.String,java.lang.String,java.lang.String...)) [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) formatted dates (given in the [UTC](https://en.wikipedia.org/wiki/Coordinated_Universal_Time) timezone). Depending on the used OSHDB [view](views.md), these timestamps are interpreted slightly differently: When using the **snapshot** view, the given timestamps define the dates at which the snapshots of the OSM entities are taken. When using the **contribution** view, all modifications to the OSM entities are returned that lie within the time range defined by the given first and last timestamp, while any further timestamps can be used later to [aggregate](aggregation.md) results into finer time intervals.

There exists also a [method](https://docs.ohsome.org/java/oshdb/1.1.0/aggregated/org/heigit/ohsome/oshdb/api/mapreducer/MapReducer.html#timestamps(java.lang.String,java.lang.String,org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps.Interval)) to define common regularly spaced time intervals within a time range, e.g. a monthly time interval between two dates.

_OSHDB_ filter
---------------

An easy way to provide [`filter`s](https://docs.ohsome.org/java/oshdb/1.1.0/aggregated/org/heigit/ohsome/oshdb/api/mapreducer/MapReducer.html#filter(java.lang.String)) is through the functionality of [OSHDB filters](https://github.com/GIScience/oshdb/blob/1.1.0/oshdb-filter/README.md), which allow one to define osm data filters in a human-readable syntax. With these one can combine several tag-, type- and geometry-filters with arbitrary boolean operators.

Simple examples of filters are `type:node and natural=tree` to select trees, or `geometry:polygon and building=*` to filter for buildings. More examples and can be found on the [dedicated filter documentation page](https://github.com/GIScience/oshdb/blob/1.1.0/oshdb-filter/README.md#examples).

By using the methods [`Filter.byOSMEntity`](https://docs.ohsome.org/java/oshdb/1.1.0/aggregated/org/heigit/ohsome/oshdb/filter/Filter.html#byOSMEntity(org.heigit.ohsome.oshdb.util.function.OSMEntityFilter)) and [`Filter.byOSHEntity`](https://docs.ohsome.org/java/oshdb/1.1.0/aggregated/org/heigit/ohsome/oshdb/filter/Filter.html#byOSHEntity(org.heigit.ohsome.oshdb.util.function.OSHEntityFilter)) one can define arbitrary callback functions to filter OSM or OSH entities, respectively.

_lambda_ filter
---------------

It is possible to define [`filter` functions](https://docs.ohsome.org/java/oshdb/1.1.0/aggregated/org/heigit/ohsome/oshdb/api/mapreducer/MapReducer.html#filter(org.heigit.ohsome.oshdb.util.function.SerializablePredicate)) that can sort out values after they already have been transformed in a [map](map-reduce.md#map) step.

Note that it is usually best to use the _OSHDB_ filters described above wherever possible, as they can reduce the amount of data to be iterated over right from the start of the query. Lambda filter functions are only executed after the OSM data has already been computed and transformed.
