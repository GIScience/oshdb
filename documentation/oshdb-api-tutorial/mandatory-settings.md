# Mandatory Settings

## Keytables

For efficiency, the OSM-tags (keys and values) are not directly stored
as strings in the oshdb, but encoded as numbers. The mapping between
the strings and the numerical encodings is stored in [keytables].
The keytables may be delivered either directly included in the oshdb
or as an external file. When using external keytables, the API
requires a link to the keytables file.

```
// -- MANDATORY SETTINGS --
// declare and open link to keytables
OSHDBDatabase tagsDB = new OSHDBH2("path/to/keytables");
// set keytables to MapReducer
mapReducer = mapReducer.keytables(tagsDB);
```

## Area of Interest

You are not restricted to any region but you still have to actively set
your area of interest. The API accepts bounding boxes, polygons and
multi-polygons are also accepted.

```
// create bounding box from country
BoundingBox boundingBox = Country.getBoundingBox("Maldives");
// OR create bounding box from coordinates
BoundingBox boundingBox = new BoundingBox(72.684825,73.753184,-0.688572,7.107245);

// add bounding box to MapReducer
mapReducer = mapReducer.areaOfInterest(boundingBox);
```

## Time of Interest

Remember that we decided to use the snapshot view for analysing OSM history
data. In this view you have to specify at which points in time you wish to
take a data snapshot. For example, taking monthly snapshots in the time bounds
from 2014-01-01 till 2015-01-01 is done by adding the following code:

```
// add timestamps to MapReducer
mapReducer = mapReducer.timestamps("2014-01-01", "2015-01-01", OSHDBTimestamps.Interval.MONTHLY);
```

## Summary

TODO: [link-to-full-code]

The next step is to setup [entity filters](entity-filters.md) that select
the OSM entities you wish to analyse.
