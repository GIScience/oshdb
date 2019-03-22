Views
=====

Two different _views_ are available, which determine how the OSM history data is actually analyzed in a given OSHDB query. 

* The *snapshot view* ([`OSMEntitySnapshotView`](https://docs.ohsome.org/java/oshdb/0.5.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/OSMEntitySnapshotView.html)) returns the state of the OSM history data at specific given points in time.
* The *contribution view* ([`OSMContributionView`](https://docs.ohsome.org/java/oshdb/0.5.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/OSMContributionView.html)) returns all modifications (e.g., creations, modifications or deletions) to the OSM elements within a given time period.

The snapshot view is particularly useful for analysing how the amount of OSM data changed over time. The contribution view can be used to determine the number of OSM contributors editing the OSM data.

<!-- todo: figure: time-slices compared to "events" -->

Using OSHDB Views
-----------------

Both views can be used in the OSHDB API in very similar ways and only differ in the type of data that is returned by the [`MapReducer`](https://docs.ohsome.org/java/oshdb/0.5.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html) object that is returned when calling the [`on`](https://docs.ohsome.org/java/oshdb/0.5.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/OSMContributionView.html#on-org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase-) method of the respective view: The `OSMEntitySnapshotView` returns a MapReducer of [`OSMEntitySnapshot`](https://docs.ohsome.org/java/oshdb/0.5.0/aggregated/org/heigit/bigspatialdata/oshdb/api/object/OSMEntitySnapshot.html) objects, while the `OSMContributionView` returns a MapReducer of [`OSMContribution`](https://docs.ohsome.org/java/oshdb/0.5.0/aggregated/org/heigit/bigspatialdata/oshdb/api/object/OSMContribution.html) objects.

```java
OSHDBDatabase oshdb = …;
MapReducer<OSMEntitySnapshot> snapshotsMapReducer = OSMEntitySnapshotView.on(oshdb);
// or
MapReducer<OSMContribution> contributionsMapReducer = OSMContributionView.on(oshdb);
```

A MapReducer is conceptually very similar to a [Stream](https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html) object in Java 8: It stores all the information about what kind of filters, transformation functions and aggregation methods should be applied to the data and is executed exactly once by calling a terminal operation, such as the _reduce_ method, or one of the supplied _special reducers_ (e.g., `count`, `sum`, etc.). The chapter [MapReduce](map-reduce.md) of this manual describes the ideas of the `MapReducer` object in more detail.

<!--
todo: explain views: what they do, what data they return, how they work, etc

Snapshot View
-------------


Contribution View
-----------------

-->

GroupByEntity
-------------

The [`groupByEntity()`](https://docs.ohsome.org/java/oshdb/0.5.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#groupByEntity--) method of a MapReducer slightly changes the way the MapReducers recieves and transforms values: Instead of iterating over each snapshot or contribution individually, in this mode all snapshots or all contributinos of an individual OSM entity are collected into a list of values first. This makes it possible to investigate the full edit history of individual OSM objects at once.

It is recommended to call this method immediately after creating the MapReducer from a view:

```java
OSMEntitySnapshotView.on(oshdb).groupByEntity()
```