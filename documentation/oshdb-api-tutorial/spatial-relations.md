# Querying by Spatial Relation

OSM snapshots and contributions can be queried based on spatial relations to other surrounding features. The methods are implemented for both the `MapReducer` and `MapAggregator` classes.  

## General structure   

The following Egenhofer relations are supported: 

* `contains()` / `containedFeatures()`
* `covers()` / `coveredFeatures()`
* `coveredBy()` / `coveringFeatures()`
* `equals()` / `equalFeatures()`
* `overlaps()` / `overlappingFeatures()`
* `touches()` / `touchingFeatures()`
* `inside()` / `enclosingFeatures()`

In addition, features can be queried based on neighbouring features using the methods

* `neighbouring()` / `neighbourhood()`

The former method filters the features of the MapReducer by comparing their geometry to the other features. The latter method returns a Pair whose first element is the feature and the second element is a list of features in the surrounding area that match the respective spatial relation.

The surrounding features can be specified using a callback function which returns a list of OSMEntitySnapshots:

```
...
  .osmTag("leisure", "park")
  .containedFeatures(mapReduce -> mapReduce.osmTag("natural", "tree").collect())
...
```

OSM key (and value) tags:

```
...
  .osmTag("leisure", "park")
  .containedFeatures("natural", "tree")
...
```
```
...
  .osmTag("leisure", "park")
  .containedFeatures("natural")
...
```

or none at all: 

```
...
  .osmTag("leisure", "park")
  .containedFeatures()
...
```

In the case of querying by neighbouring features, a distance value needs to be provided which quantifies the neighbourhood range in meters e.g.

```
...
  .osmTag("amenity", "bench")
  .neighbourhood(5, mapReduce -> mapReduce.osmTag("natural", "tree").collect())
...
```

## Important notes

### Covers
Every point of the other geometry is a point of this geometry. `coveredBy()` is the converse to covers. 

### Contains
Every point of the other geometry is a point of this geometry, and the interiors of the two geometries have at least one point in common. `inside()` is the converse to contains.

### Overlaps
Geometry.overlaps is only true if the two geometries have the same dimension. For Egenhofer relations this is however not the case. Therefore, overlaps() is not used here
                
## Examples 


### Finding duplicate nodes using `equals()`

```
Integer result = OSMEntitySnapshotView.on(oshdb)
    .keytables(oshdb)
    .timestamps(timestamps2016)
    .areaOfInterest(bbox)
    .osmType(OSMType.NODE)
    .equals()
    .count();
```

### Finding benches nearby trees using `neighbouring()`

The following query will return a list of tuples whose first element is a bench and the second element is a list of trees that are located within a 5 meter distance of the respective bench and that have been edited between the timestamp of this snapshot and the following snapshot. 

```
...
List<Pair<OSMEntitySnapshot, List<OSMEntitySnapshot>>> result = MapReducer
  .on(oshdb)
  .keytables(oshdb)
  .timestamps(timestamps2017)
  .areaOfInterest(bbox)
  .osmTag("amenity", "bench")
  .neighbouring(5, "natural", "tree")
  .collect()
```

### Count the nodes inside a line using `contains()` or `inside()`

According to the Egenhofer relations, nodes that are not located at the ends of a line geometry are classified as being contained in the line. For an `OSMEntitySnapshotView()` there are two ways to find these nodes.

If there are less ways than nodes, the following query is faster:

```
Integer result_contains = OSMEntitySnapshotView.on(oshdb)
    .keytables(oshdb)
    .timestamps(timestamps2017)
    .areaOfInterest(bbox)
    .osmType(OSMType.WAY)
    .filter(x -> x.getEntity().getId() == 36493984)
    .containedFeatures(
        mapReduce -> mapReduce.osmType(OSMType.NODE).collect())
    .flatMap(x -> x.getRight())
    .count();
```
If there are more nodes than ways, the following query is faster:

```
Integer result_inside = OSMEntitySnapshotView.on(oshdb)
    .keytables(oshdb)
    .timestamps(timestamps2017)
    .areaOfInterest(bbox)
    .osmType(OSMType.NODE)
    .inside(
        mapReduce -> mapReduce
            .filter(x -> ((OSMEntitySnapshot) x).getEntity().getId() == 36493984).collect())
    .count();
  }
```

### Count contributions inside a park using `inside()`
In case of a OSMContributionView there is only one way to count, since the callback function of `inside()` must be of type `SerializableFunctionWithException<MapReducer<OSMEntitySnapshot>, List<OSMEntitySnapshot>>`.

__Important Note:__ In an `OSMContributionView()` each contribution is compared to the last snapshot of the time period, not to snapshots at the time of the contribution. 

```
Integer result = OSMContributionView.on(oshdb)
    .keytables(oshdb)
    .timestamps(timestamps10)
    .areaOfInterest(bbox)
    .osmType(OSMType.NODE)
    .inside("leisure", "park")
    .count();
```

### Find adjacent buildings using `touchingFeatures()`
The following query returns a list of adjacent buildings for each building. 

```
List<Pair<OSMEntitySnapshot, List<Object>>> result2 = OSMEntitySnapshotView.on(oshdb)
    .keytables(oshdb)
    .timestamps(timestamps2017)
    .areaOfInterest(bbox)
    .osmTag("building")
    .filter(x -> x.getEntity().getId() == 172510837)
    .touchingElements(
        mapReduce -> mapReduce.osmTag("building").collect())
    .collect();
```






