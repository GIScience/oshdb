Data Aggregation
================

Often, when querying OSM history data one is interested in getting multiple results at once that each refer to a certain subset of the queried data. For example, when querying for multiple [timestamps](filters.md#timestamps), typically the result should be in the form of one [result](map-reduce.md#specialized-reducers) per timestamp.

The OSHDB API provides a flexible and powerful way to produce aggregated results that are calculated for arbitrary subsets of the data. This `aggregateBy` functionality also supports the combination of multiple such grouping functions chained after each other.

When executing any of the below listed aggregateBy methods, the query's MapReducer is transformed into a [`MapAggregator`](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapAggregator.html) object which is (mostly) functionally equivalent to a MapReducer, with the difference that instead of returning single result values when calling any [reduce](map-reduce.md#reduce) method, an associative list of multiple values is returned instead: The result contains one entry for each requested grouping.

aggregateBy
-----------

This is the most generic grouping method, that allows to produce aggregated results that refer to arbitrary subsets of the input data. The [`aggregateBy`](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#aggregateBy-org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunction-) method accepts a function that must return an "index" value by which the respective result should be grouped by. For example, when one wants to group results by OSM type, the aggregateBy method should simply return the OSM type value, as in the following example using the OSHDB snapshot view:

```java
Map<OSMType, Integer> countBuildingsByType = OSMEntitySnapshotView.on(…)
    .areaOfInterest(…)
    .timestamps(…)
    .osmTag("building")
    .aggregateBy(snapshot -> snapshot.getEntity().getType())
    .count();
``` 

Optionally, the [`aggregateBy`](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#aggregateBy-org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunction-java.util.Collection-) method allows to specify a collection of groups  which are expected to be present in the result. If for a particular group, no matching OSM entities are found in the query, the result will then still contain this key, filled with a "zero" value. The actual value used to fill these no-data entries of the result are the indentity values of the query's reduce method.

 > For example, if the count reducer is used in a query, the result contains `0` integer values in entries for witch no results were found. If instead the collect reduce method is used, empty lists are used to fill no-data entries.

```java
    .aggregateBy(
        snapshot -> snapshot.getEntity().getType(),
        EnumSet.allOf(OSMType.class)
    )
```

aggregateByTimestamp
--------------------

This is a specialized method for grouping results by timestamps. Depending on the used [view](views.md), aggregating by timestamp has slightly different meanings: In the OSMEntitySnapshotView, the snapshots' timestamp will be used directly to group results. In the OSMContributionView however, the timestamps of the respective modifications will be matched to the corresponding time intervals defined in the OSHDB query.

> For example, when in a query the following three timestamps are set: `2014-01-01`, `2015-01-01` and `2016-01-01`, then a contribution happening at `2015-03-14` will be associated to the time interval between `2015-01-01` and `2016-01-01` (which is represented in the output as the starting time of the interval: `2015-01-01`).


There are two variants that allow this grouping by timestamp: [`aggregateByTimestamp`](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#aggregateByTimestamp--) tries to automatically fetch the timestamps from the queried data (i.e. the snapshot or the contribution objects), while the second variant of [`aggregateByTimestamp`](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#aggregateByTimestamp-org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunction-) takes a callback function that returns an arbitrary timestamp value. The second variant has to be used in some cases where the automatic matching of objects to its timestamps isn't possible, for example when using the [groupByEntity](views.md#groupbyentity) option in a query, or when using multiple [aggregateBy](#combining-multiple-aggregateby)s in a query.

aggregateByGeometry
-------------------

Calculating results for multiple sub-regions of an area of interest at once is possible through [`aggregateByGeometry`](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#aggregateByGeometry-java.util.Map-). It accepts an associative list of polygonal geometries with corresponding index values. The result will then use these index values to represent the individual sub-region results.

When using the aggregateByGeometry functionality, any OSM entity geometry that is contained in multiple sub-regions will be split and clipped to the respective geometries.

The given grouping geometries are allowed to overlap each other, but they should exactly match (i.e. fully cover and not protrude out of) the [areaOfInterest](filters.md#areaofinterest) of the query.

combining multiple aggregateBy
------------------------------

When writing an OSHDB query it is possible to perform multiple of the above mentioned aggregateBy operations. For example, it is possible to write a query that returns results that are aggregated by timestamps and by OSM type. In this case, the final result will contain one entry for each possible combination of the specified groupings. These combined indices are encoded as [`OSHDBCombinedIndex`](https://docs.ohsome.org/java/oshdb/0.6.0/aggregated/org/heigit/bigspatialdata/oshdb/api/generic/OSHDBCombinedIndex.html) objects in the final result map.

```java
Map<OSHDBCombinedIndex<OSHDBTimestamp, OSMType>, Integer> countBuildingsByTimeAndType = OSMEntitySnapshotView.on(…)
    .areaOfInterest(…)
    .timestamps(…)
    .osmTag("building")
    .aggregateByTimestamp()
    .aggregateBy(snapshot -> snapshot.getEntity().getType())
    .count();
```

This produces result data as a long list of entries with a complex key. Sometimes it is however easier to work with data in a more strucutured, nested form. The OSHDB API provides a helper method which can convert result data from the long format into the nested format:

```java
SortedMap<OSHDBCombinedIndex<OSHDBTimestamp, OSMType>, Integer> flatCountBuildingsByTimeAndType = …;
SortedMap<OSHDBTimestamp, SortedMap<OSMType, Integer>> nestedCountBuildingsByTimeAndType = OSHDBCombinedIndex.nest(flatCountBuildingsByTimeAndType);
System.out.println(
    "building count at timestamp1 for ways: "
    + nestedCountBuildingsByTimeAndType.get(timestamp1).get(OSMType.WAY)
);
```

Chaining together more than two aggregateBy methods is also possible, which results in nested combined indices:

```java
OSHDBCombinedIndex<OSHDBCombinedIndex<IndexType1, IndexType2>, IndexType3>
```