# Map

The map phase is one of the two main phases of the MapReduce programming
model. It applies a map (function) to each of your selected data elements,
and the MapReduce framework takes care of doing this in parallel. Two types
of mapping are available:

* map is a 1-to-1 mapping that generates a single result for each data object,
* flatMap is a 1-to-n mapping that generates a (possibly empty) list of
  results for each data object and automatically flattens the results of
  several data objects into a single long list
  (see [advanced options](advanced-options.md)).

We will again implement the mapping in a custom class for clarity. This simple
mapping function calculates the length of the element's geometry.

```
public static void main(...) throws [...] {
  [...]

// -- MAPPING --
// normal mapAggregatorTwoResultFilter 1->1
MapAggregator<OSHDBTimestamp, Double> mapAggregatorWithMap = mapAggregatorByTimestamp.map(new Mapper());

}

private static class Mapper implements SerializableFunction<OSMEntitySnapshot, Double> {
  public Double apply(OSMEntitySnapshot t) {
    return Geo.lengthOf(t.getGeometry());
  }
}
```

The next step is to [filter results](result-filters.md) for those that are in
the scope of your interest.