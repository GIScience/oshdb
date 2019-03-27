Views
=====

Two different ways of querying OSM data are available, which determine how the OSM history data is actually analyzed in a given OSHDB query: 

* The **snapshot view** ([`OSMEntitySnapshotView`](https://docs.ohsome.org/java/oshdb/0.5.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/OSMEntitySnapshotView.html)) returns the state of the OSM history data at specific given points in time.
* The **contribution view** ([`OSMContributionView`](https://docs.ohsome.org/java/oshdb/0.5.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/OSMContributionView.html)) returns all modifications (e.g., creations, modifications or deletions) to the OSM elements within a given time period.

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

A MapReducer is conceptually very similar to a [Stream](https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html) object in Java 8: It stores all the information about what kind of filters, transformation functions and aggregation methods should be applied to the data and is executed exactly once by calling a terminal operation, such as the reduce method, or one of the supplied specialized reducers (e.g., `count`, `sum`, etc.). The chapter “[Map and Reduce](map-reduce.md)” of this manual describes the ideas of the `MapReducer` object in more detail.

### Snapshot View

The [`OSMEntitySnapshot`](https://docs.ohsome.org/java/oshdb/0.5.0/aggregated/org/heigit/bigspatialdata/oshdb/api/object/OSMEntitySnapshot.html) is quite simple: it returns the state of the OSM data at a given point in time, or at multiple given points in time. In the OSHDB API, these are called snapshots and are represented by [`OSMEntitySnapshot`](https://docs.ohsome.org/java/oshdb/0.5.0/aggregated/org/heigit/bigspatialdata/oshdb/api/object/OSMEntitySnapshot.html) objects. They allow access to the following properties:

* the [timestamp](https://docs.ohsome.org/java/oshdb/0.5.0/aggregated/org/heigit/bigspatialdata/oshdb/api/object/OSMEntitySnapshot.html#getTimestamp--) of the snapshot
* the [geometry](https://docs.ohsome.org/java/oshdb/0.5.0/aggregated/org/heigit/bigspatialdata/oshdb/api/object/OSMEntitySnapshot.html#getGeometry--) of the queried OSM feature
* the [OSM entity](https://docs.ohsome.org/java/oshdb/0.5.0/aggregated/org/heigit/bigspatialdata/oshdb/api/object/OSMEntitySnapshot.html#getEntity--) of this snapshot

### Contribution View

The [`OSMContributionView`](https://docs.ohsome.org/java/oshdb/0.5.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/OSMContributionView.html) returns all modifications to matching OSM entities. This is in general more more computationally intensive than using the snapshot view, but allows to inspect the OSM data in more detail, especially if one is interested in how the OSM data is modified by the contributors to the OSM project.

Through the returned [`OSMContribution`](https://docs.ohsome.org/java/oshdb/0.5.0/aggregated/org/heigit/bigspatialdata/oshdb/api/object/OSMContribution.html) objects, one has access to the following properties:

* the [timestamp](https://docs.ohsome.org/java/oshdb/0.5.0/aggregated/org/heigit/bigspatialdata/oshdb/api/object/OSMContribution.html#getTimestamp--) of the contribution
* the geometries [before](https://docs.ohsome.org/java/oshdb/0.5.0/aggregated/org/heigit/bigspatialdata/oshdb/api/object/OSMContribution.html#getGeometryBefore--) and [after](https://docs.ohsome.org/java/oshdb/0.5.0/aggregated/org/heigit/bigspatialdata/oshdb/api/object/OSMContribution.html#getGeometryAfter--) the modification. If the contribution object represents a creation of an entity, the before geometry doesn't exist and returns `null` if it is accessed. Similarly, this is also true for the geometry after a deletion of an OSM object.
* the OSM entity [before](https://docs.ohsome.org/java/oshdb/0.5.0/aggregated/org/heigit/bigspatialdata/oshdb/api/object/OSMContribution.html#getEntityBefore--) and [after](https://docs.ohsome.org/java/oshdb/0.5.0/aggregated/org/heigit/bigspatialdata/oshdb/api/object/OSMContribution.html#getEntityBefore--) the modification
* the [id of the OSM user](https://docs.ohsome.org/java/oshdb/0.5.0/aggregated/org/heigit/bigspatialdata/oshdb/api/object/OSMContribution.html#getContributorUserId--) who performed this contribution
* the [id of the OSM changeset](https://docs.ohsome.org/java/oshdb/0.5.0/aggregated/org/heigit/bigspatialdata/oshdb/api/object/OSMContribution.html#getChangesetId--) in which this contribution was performed
* the [type](https://docs.ohsome.org/java/oshdb/0.5.0/aggregated/org/heigit/bigspatialdata/oshdb/api/object/OSMContribution.html#getContributionTypes--) of the contribution.

The [contribution type](https://docs.ohsome.org/java/oshdb/0.5.0/aggregated/org/heigit/bigspatialdata/oshdb/util/celliterator/ContributionType.html) can be either a **creation**, a **deletion**, a **tag change** or a **geometry change** of an OSM entity.

All of these contribution types refer to the filtered set of OSM data of the current MapReducer. This means that an OSM feature that has gained a specific tag in one of versions greater than one, will be reported as a "creation" by the contribution view of the OSHDB API if the query was programmed to filter for that particular tag. Analogously this is also the case if an object was moved from outside an area of interest into the query region, and also for the inverse cases which are returned as deletions. This makes sure that summing up all creations and subtracting all deletions matches the results one can obtain from a query using the snapshot view.

Note that there exist [cases](https://github.com/GIScience/oshdb/issues/87) where a contribution object doesn't belong to any of the mentioned contribution type (i.e. when a modification of an object doesn't result in a change in geometry or tags).

GroupByEntity
-------------

The [`groupByEntity()`](https://docs.ohsome.org/java/oshdb/0.5.0/aggregated/org/heigit/bigspatialdata/oshdb/api/mapreducer/MapReducer.html#groupByEntity--) method of a MapReducer slightly changes the way the MapReducers recieves and transforms values: Instead of iterating over each snapshot or contribution individually, in this mode all snapshots or all contributinos of an individual OSM entity are collected into a list of values first. This makes it possible to investigate the full edit history of individual OSM objects at once.

It is recommended to call this method immediately after creating the MapReducer from a view:

```java
OSMEntitySnapshotView.on(oshdb).groupByEntity()
```