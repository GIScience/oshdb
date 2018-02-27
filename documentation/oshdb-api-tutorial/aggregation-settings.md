# Aggregation Settings

When analysing data one is often not interested in the raw data objects
but in aggregated statistics of these objects (e.g., counting buildings,
summing up lengths of streets). By default, the oshdb-API aggregates
your data into a single result. However, different aggregation methods
may be specified. The most common one is to aggregate by timestamp
(for custom aggregations see [advanced options](advanced-options.md)).
Timestamp aggregation puts your data into buckets of the same period and
returns a
[SortedMap](https://docs.oracle.com/javase/8/docs/api/java/util/SortedMap.html)
where each timestamp holds your specified result.

```
// -- AGGREGATION --
// aggreate by timestamp into periods
MapAggregatorByTimestamps<OSMEntitySnapshot> mapAggregatorByTimestamp = mapReducer.aggregateByTimestamp();
```


The next step is to [define mappings](map.md) on the selected data.

