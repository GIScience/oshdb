# Query by Neighbourhood


In addition, to [map()](map.md) and [filter()](filter.md), OSM features can also be queried based on spatial relations. 


## Query by Neighbourhood 

Using `neighbourhood()` and `neighbouring()` objects can be queried based on their neighborhood. Both require a distance parameter given in meters and a second parameter specifying the nearby objects. 

### Filter OSMEntitySnapshots by neighbouring OSMEntitySnapshots

Using `neighbouring()` the OSM features are filtered depending on the objects that are located nearby. 

__Example__: A query for all benches that are located within a 5 meter distance to a objects with key "natural" might look like this:

```
...
  .areaOfInterest(...)
  .timestamps(...)
  .osmTag("amenity", "bench")
  .neighbourhoodFilter(5, "natural")
  .collect()
```

Neighbouring objects can be filtered either by key or by key and value e.g. 

```
...
  .osmTag("amenity", "bench")
  .neighbourhoodFilter(5, "natural", "tree")
  .collect()
```

The same result can be achieved by passing a call back function to `neighbouring()`. This facilitates more complex queries. 

```
...
  .osmTag("amenity", "bench")
  .neighbourhoodFilter(5, mapReduce -> mapReduce.osmTag("natural", "tree").count() > 0)
  .collect()
```


### Query OSMEntitySnapshots in the neighbourhood of an OSMEntitySnapshot

Using the `neighbouring()` function features are only filtered based on the presence of other objects in the neighbourhood, but no information about these objects is returned. This can be achieved using the `neighbouring()`function, which returns all OSM features and their respective neighbouring objects as a list.  

__Example__: The following query will return a list of tuples whose first element is a bench and the second element is a list of trees located within a 5 meter distance of the respective bench. If there are no neighbouring trees, the list will be empty. 

```
...
List<Pair<OSHDBSnapshot, List<OSHDBSnapshot>>> result = MapReducer
  .osmTag("amenity", "bench")
  .neighbourhoodMap(5, mapReduce -> mapReduce.osmTag("natural", "tree").collect())
  .collect()
```

### Query OSMContributions in the neighbourhood of an OSMEntitySnapshot

It is also possible to find OSMContributions in the neighbourhood of an OSMEntitySnapshot. This option can be enabled by passing a third argument `queryContributions = true` to the neighbourhood function.  

__Example__: The following query will return a list of tuples whose first element is a bench and the second element is a list of trees that are located within a 5 meter distance of the respective bench and that have been edited between the timestamp of this snapshot and the following snapshot. 

```
...
List<Pair<OSHDBSnapshot, List<OSHDBSnapshot>>> result = MapReducer
  .osmTag("amenity", "bench")
  .neighbourhoodMap(5, mapReduce -> mapReduce.osmTag("natural", "tree").collect(), queryContributions = true)
  .collect()
```





