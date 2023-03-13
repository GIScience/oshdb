OSM Feature Geometries
======================

The [_views_](views.md) provided by the OSHDB API provide direct access to the OSM entities, but also allow to get the [JTS](https://en.wikipedia.org/wiki/JTS_Topology_Suite#Geometry_model) [geometries](https://locationtech.github.io/jts/javadoc/org/locationtech/jts/geom/Geometry.html) of the OSM features corresponding to their state at the requested _snapshot_ or _contribution_ time(s). With this geometry, further operations such as [_filtering_](filters.md#areaOfInterest) or [_aggregation_](aggregation.md#aggregateByGeometry) can be performed. It is also irreplaceable during the [_map-reduce_](map-reduce.md#geometry-helpers) step to generate useful results, such as the length of a road network for example.

For some OSM elements, such as nodes, generating geometries is straight forward, for others the conversion requires further knowledge of the [data model](https://wiki.openstreetmap.org/wiki/Elements) and [tagging schema](https://wiki.openstreetmap.org/wiki/Tags) used by OSM. The following document gives an overview of how the OSHDB handles the building of geometries of different OSM object types.

Nodes
-----

Nodes are always presented as [`Point`](https://locationtech.github.io/jts/javadoc/org/locationtech/jts/geom/Point.html)s. Note that nodes which have never had a tag and are a part of a way or relation are considered to be structural-only points (sometimes called _vertices_), and thus not returned in an OSHDB query when querying all nodes. This is because the OSHDB does not consider them to not be _map features_ by their own. They can however be fetched as members of their parent way or relation objects, if needed.

Ways
----

Ways are converted to either [`LineString`](https://locationtech.github.io/jts/javadoc/org/locationtech/jts/geom/LineString.html) or [`Polygon`](https://locationtech.github.io/jts/javadoc/org/locationtech/jts/geom/Polygon.html) geometries depending on their composition and their tags: A not closed way is always represented as a line, while it depends for a closed one. The [`TagInterpreter`](https://docs.ohsome.org/java/oshdb/1.1.1/aggregated/org/heigit/ohsome/oshdb/util/taginterpreter/TagInterpreter.html) component of the OSHDB is responsible for deciding whether a closed way results in a line or a polygon: A (closed) OSM way with the tag `building=yes` will be converted to a polygon geometry, while a `junction=roundabout` one will not.

Relations
---------

Relations are handled by the OSHDB in two different ways: [Multipolygons](https://wiki.openstreetmap.org/wiki/Multipolygon) are converted to either [`Polygon`](https://locationtech.github.io/jts/javadoc/org/locationtech/jts/geom/Polygon.html) or [`MultiPolygon`](https://locationtech.github.io/jts/javadoc/org/locationtech/jts/geom/MultiPolygon.html) geometries (depending on the number of outer rings), while all [other relation types](https://wiki.openstreetmap.org/wiki/Types_of_relation) result in a [`GeometryCollection`](https://locationtech.github.io/jts/javadoc/org/locationtech/jts/geom/GeometryCollection.html).

Invalid OSM Data
----------------

There are situations where a part of OSM's entities have either incomplete or invalid data, for example a broken multipolygon resulting from a mapping error. The OSHDB makes the best effort to return a (potentially [invalid](https://locationtech.github.io/jts/javadoc/org/locationtech/jts/geom/Geometry.html#isValid--)) geometry also for these objects. For performance reasons the OSHDB cannot check for every possible error in the input (OSM) data, and for similar reasons it also cannot correct all errors it does find. This means that the OSHDB does not provide any guaranteed outcome for all possible errors and might return an invalid or valid geometry as a result in such cases or even no result at all. 
