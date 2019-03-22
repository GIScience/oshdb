Data Aggregation
================

Often, when querying OSM history data one isn't just interested in getting only single results, but is rather interested in multiple results at once that each refer to a certain subset of the queried data. For example, when querying for multiple [timestamps](filters.md#timestamps), typically the result should be in the form of one [result](map-reduce.md#specialized-reducers) per timestamp.  

The OSHDB API provides a flexible and powerful way to produce aggregated results that are calculated for arbitrary subsets of the data, which can also be combined with each other.

aggregateByTimestamp
--------------------

[`aggregateByTimestamp`](https://docs.ohsome.org/java/oshdb/0.5.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#aggregateByTimestamp--)

[`aggregateByTimestamp`](https://docs.ohsome.org/java/oshdb/0.5.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#aggregateByTimestamp-org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunction-) (with callback)

aggregateByGeometry
-------------------

[`aggregateByGeometry`](https://docs.ohsome.org/java/oshdb/0.5.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#aggregateByGeometry-java.util.Map-)

custom aggregateBy
------------------

[`aggregateBy`](https://docs.ohsome.org/java/oshdb/0.5.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#aggregateBy-org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunction-)

[`aggregateBy`](https://docs.ohsome.org/java/oshdb/0.5.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#aggregateBy-org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunction-java.util.Collection-) (with zerofill)

combining multiple aggregateBy
------------------------------



