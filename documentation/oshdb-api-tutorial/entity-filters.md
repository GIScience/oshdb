# Entity Filters

Entity filters permit one to restrict the scope of an analysis
to certain OSM entities (e.g., buildings). That is, you may
select the entities that are interesting for your analysis
before the actual processing is done.


## OSM Type

If you wish to analyse one or two out of the three OSM types,
use a type filter. For example, selecting all OSM ways is done
as follows:

```
// -- ENTITY FILTER --
// by type
mapReducer = mapReducer.osmTypes(OSMType.WAY);
```


## OSM Tags

Filtering OSM entities by their tags may be done
either by key or by key and value:

```
// by tag (key only)
mapReducer = mapReducer.osmTag("highway");

// by tag (key and value)
mapReducer = mapReducer.osmTag("highway", "motorway");
```


## Complex Properties

If you wish to select entities by more complex properties, you may
implement a SerializablePredicate on OSM entities. In this tutorial
we select entities with a version number >2.
(TODO: find better example, as versions are a questionable feature)

We use a nested class for clarity, although you could also use
a lambda expression or implement the class in a different file.

```
public static void main(...) throws [...] {

  [...]

  // by entity-definition
  mapReducer = mapReducer.osmEntityFilter(new EntityFilter());
}

private static class EntityFilter implements SerializablePredicate<OSMEntity> {

  public boolean test(OSMEntity t) {
    return t.getVersion() > 2;
  }
}

```

## Summary

In this step we selected the entities in which we are interested.

The next step is to define how the selected entities shall be 
[aggregated](aggregation-settings.md).