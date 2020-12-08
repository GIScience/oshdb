Map and Reduce
==============

The [`MapReducer`](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html) is the central object of every OSHDB query. It is returned by the initial OSHDB [view](views.md) and allows to [filter](filters.md) out defined subsets of the OSM history dataset. At that point one can transform (**map**) and aggregate (**reduce**) the respective OSM data into a final result.

> For example, a map function can calculate the length of every OSM highway, and a reduce function can sum up all of these length values.

For many of the most frequently used reduce operations, such as the summing up of many values or the counting of elements, there exist [specialized reducers](#specialized-reducers).

map
---

A transformation function can be set by calling the [`map`](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#map-org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunction-) method of any MapReducer. It is allowed to have an OSHDB query without a map step or one with multiple map steps, which are executed one after each other. Such a map function can also transform the data type of the MapReducer it operates on.

> For example, when calculating the length (which is a floating point number) of an entity snapshot, the underlying MapReducer changes from type `MapReducer<OSMEntitySnapshot>` to being a `MapReducer<Double>`.

flatMap
-------

A [`flatMap`](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#flatMap-org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunction-) operation allows one to map any input value to an arbitrary amount of output values. Each of the output values can be transformed in further map steps individually.

filter
------

Filters can even be applied in the map phase. Read more about this feature in the [filters](filters.md#lambda-filters) section of this manual.

reduce
------

The [`reduce`](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#reduce-org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableSupplier-org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBiFunction-org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBinaryOperator-) operation produces the final result of an OSHDB query. It takes the result of the previous map steps and combines (reduces) these values into a final result. This can be something as simple as summing up all of the values, but also something more complicated, for example estimating statistical properties such as the median of the calculated values. Many query use common reduce operations, for which the OSHDB provides shorthand methods (see [below](#specialized-reducers)).

Every OSHDB query must have exactly one terminal reduce operation (or use the `stream` method explained [below](#stream)).

> Remark: If you are already familiar with [Hadoop](https://en.wikipedia.org/wiki/Apache_Hadoop), note that for defining a reduce operation we use the terminology of the [Java stream API](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/stream/package-summary.html) which is slightly different from the terminology used in Hadoop. In particular, the Java stream API and Hadoop use the same term 'combiner' for different things.

specialized reducers
--------------------

The OSHDB provides the following list of default reduce operations, that are often used for querying OSM history data. Their names and usage are mostly self explanatory. 

* [`count`](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#count--)
* [`sum`](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#sum--)
* [`average`](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#average--)
* [`weightedAverage`](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#weightedAverage-org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunction-)
* [`uniq`](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#uniq--)
* [`countUniq`](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#countUniq--)
* [`estimatedMedian`](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#estimatedMedian--)
* [`estimatedQuantile(s)`](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#estimatedQuantiles--)
* [`collect`](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#collect--)

Some of the listed specialized reducers also have overloaded versions that accept a mapping function directly. This allows some queries to be written more consicely, but also allows for improved type inference: For example when summing integer values, using the overloaded [`sum`](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#sum-org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunction-) reducer knows that the result must also be of type `Integer`, and doesn't have to resort on returning the more generic `Number` type.

stream
------

Instead of using a regular reduce operation at the end of an OSHDB query, one can also call [`stream`](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#stream--), which doesn't aggregate the values into a final result, but rather returns a (potentially long) stream of values. If possible, using a reduce operation instead of streaming all values and using post-processing results in better performance of a query, because there is less data to be transferred. The stream operation is however beneficiall over using `collect` if the result set is expected to be large, because it doesn't require all the data to be buffered into a result collection.

geometry helpers
----------------

Often, one might be interested in analyzing properties of the geometries of the analyzed OSM features. For some often used metrics, the OSHDB comes with a few built in helper functions in its [`Geo`](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/util/geometry/Geo.html) class:

* [`areaOf`](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/util/geometry/Geo.html#areaOf(org.locationtech.jts.geom.Geometry)) returns the area (in `m²`) of polygonal geometries.
* [`lengthOf`](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/util/geometry/Geo.html#lengthOf(org.locationtech.jts.geom.Geometry)) returns the length (in `m`) of linear geometries.

> Note that both of these methods use approximation formulas to calculate the length or area of OSM geometries. For typical features present in OpenStreetMap data, however, the relative error introduced by these approximations are quite small (below 0.1% for lengths and < 0.001% for areas).
