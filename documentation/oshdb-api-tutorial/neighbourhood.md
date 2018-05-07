# Query by neighbouring objects 


In addition, to [map()](map.md) and [filter()](filter.md), OSM features can also be queried by neighbouring objects using `neighbouring()` and `neighbourhood()`. Both require a distance parameter given in meters and a second parameter specifying the nearby objects. 

### Filter by neighbouring obejcts

Using `neighbouring()` the OSM features are filtered depending on the objects that are located nearby. 

__Example__: A query for all benches that are located within a 5 meter distance to a objects with key "natural" might look like this:

```
...
  .areaOfInterest(...)
  .timestamps(...)
  .where("amenity", "bench")
  .neighbouring(5, "natural")
  .collect()
```

Neighbouring objects can be filtered either by key or by key and value e.g. 

```
...
  .where("amenity", "bench")
  .neighbouring(5, "natural", "tree")
  .collect()
```

The same result can be achieved by passing a call back function to `neighbouring()`. This facilitates more complex queries. 

```
...
  .where("amenity", "bench")
  .neighbouring(5, mapReduce -> mapReduce.where("natural", "tree").count() > 0)
  .collect()
```

### Query neighbouring objects 

Using the `neighbouring()` function features are only filtered based on the presence of other objects in the neighbourhood, but no information about these objects is returned. This can be achieved using the `neighourhood()`function, which returns all OSM features and their respective neighbouring objects as a list.  

__Example__: The following query will return a list of tuples whose first element is a bench and the second element is a list of trees located within a 5 meter distance of the respective bench. If there are no neighbouring trees, the list will be empty. 

```
...
  .areaOfInterest(...)
  .timestamps(...)
  .where("amenity", "bench")
  .neighbourhood(5, mapReduce -> mapReduce.where("natural", "tree").collect())
  .collect()
```

