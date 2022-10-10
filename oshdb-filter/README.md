OSHDB filter
============

A parser and interpreter for filters which can be applied on OSM entities. It allows, for example, to filter by various combinations of OSM tags.

Before this was integrated in the main OSHDB repository, this was developed as an independent library called [“_ohsome filter_”](https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/libs/ohsome-filter).

Usage
-----

Filters can be used directly in the OSHDB:

```java
OSMEntitySnapshotView.on(…)
    .areaOfInterest(…)
    .timestamps(…)
    .filter("geometry:polygon and building=*")
    .aggregateByTimestamp()
    .count()
    .forEach((t, val) -> System.out.println(t + "\t" + val)); 
```

Alternatively one can parse filter strings like this:

```java
String filterText = …;
TagTranslator tagTranslator = …;
FilterParser parser = new FilterParser(tagTranslator);
FilterExpression filter = parser.parse(filterText);
```

Such filters can be applied to OSM entity snapshots, for example:

```java
String featureRequirementsFilterText = "geometry:polygon and addr:housenumber=*";
TagTranslator tagTranslator = …;
FilterParser filterParser = new FilterParser(tagTranslator);
FilterExpression featureRequirementsFilter = filterParser.parse(featureRequirementsFilterText);

OSMEntitySnapshotView.on(…)
    .areaOfInterest(…)
    .timestamps("…")
    .filter("building=*")
    .aggregateBy(snapshot ->
        featureRequirementsFilter.applyOSMGeometry(snapshot.getEntity(), snapshot::getGeometry)
            ? "feature matches requirements"
            : "feature does not match requirements"
    )
    .count()
    .forEach((t, val) -> System.out.println(t + "\t" + val));
```

Syntax
------

Filters are defined in textual form. A filter expression can be composed out of several actual filters, which are combined with boolean operators and parentheses. OSM entities can be filters by their tags and/or their type, for example.

### Selectors

|   | description | example |
|---|-------------|---------|
| `key=value` | matches all entities which have this exact tag | `natural=tree` |
| `key=*` | matches all entities which have any tag with the given key | `addr:housenumber=*` |
| `key!=value` | matches all entities which do not have this exact tag – the same as `not key=value` | `oneway!=yes` |
| `key!=*` | matches all entities which do not have any tag with the given key – the same as `not key=*`  | `name!=*` |
| `key in (list,of,values)` | matches all entities with have a tag with the given key and one of the given comma separated values | `highway in (residential, living_street)` |
| `type:osm-type` | matches all entities of the given OSM type | `type:node` |
| `id:osm-id` | matches all entities with the given OSM id | `id:1` |
| `id:osm-type/osm-id` | matches all entities with the given OSM type and id | `id:node/1` |
| `id:(list,of,ids)` | matches all entities whose OSM id is in the given comma separated list of ids | `id:(1,2,3)` |
| `id:(list,of,type/ids)` | matches all entities whose OSM id is in the given comma separated list of ids | `id:(node/1,way/2)` |
| `id:(from..to-range)` | matches all entities whose OSM id is in the given range of ids. Ranges use `..` to define the start and end of an interval. The interval bounds are included in the result. Either the start or the end of a range can be omitted, to select all features up to or starting from the given id. | `id:(1..3)`, `id:(..3)`, `id:(1..)` |
| `geometry:geom-type` | matches anything which has a geometry of the given type (_point_, _line_, _polygon_, or _other_) | `geometry:polygon` |
| `area:(from..to-range)` | matches all features with an area that falls into the given range/interval given as two numbers in decimal or scientific notation separated by `..`. The values are  interpreted as square meters (`m²`). The lower or upper limit of the interval may be omitted to select features having an area up to or starting from the given value, respectively. | `area:(123.4..1E6)` |
| `length:(from..to-range)` | matches all features with a length that falls into the given range/interval given as two numbers in decimal or scientific notation separated by `..`. The values are  interpreted as meters (`m`). The lower or upper limit of the interval may be omitted to select features having an area up to or starting from the given value, respectively. | `length:(100..)` |
| `changeset:id` | matches OSM contributions performed in the given OSM changeset. Can only be used in queries using the `OSMContributionView`. | `changeset:42` |
| `changeset:(list,of,ids)` | matches OSM contributions performed in one of the given OSM changeset ids. Can only be used in queries using the `OSMContributionView`. | `changeset:(1,42,100)` |
| `changeset:(from..to-range)` | matches OSM contributions performed in an OSM changeset falling into the given range of changeset ids. Can only be used in queries using the `OSMContributionView`. | `changeset:(100..200)` |
| `contributor:uid` | matches OSM contributions performed by a given OSM user (specified by their `uid`). Can only be used in queries using the `OSMContributionView`. | `contributor:1` |
| `contributor:(list,of,uids)` | matches OSM contributions performed by a given OSM user (specified as a list of `uid`s). Can only be used in queries using the `OSMContributionView`. | `contributor:(1,107037)` |
| `contributor:(from..to-range)` | matches OSM contributions performed by a given OSM user (specified as a range of `uid`s). Can only be used in queries using the `OSMContributionView`. | `contributor:(32..63)` |

### Operators

|   | description | example |
|---|-------------|---------|
| `(…)` | can be used to change precedence of operators | `highway=primary and (name=* or ref=*)` |
| `not X` | negates the following filter expression | `not type:node` |
| `X and Y` | returns entities which match both filter expressions X and Y | `highway=service and service=driveway` |
| `X or Y` | returns entities which match at least one of the filter expressions X or Y | `natural=wood or landuse=forest` |

Operators follow the following order of precedence: parentheses before `not`, before `and`, before `or`.

### Special Characters & Whitespace

When writing filters, tags without special characters can be supplied directly, without needing to quote them. Example: `amenity=drinking_water` or `name:it=*`. Allowed characters are: the letters `a-z` and `A-Z`, digits, underscore, dashes and colons.
When filtering by tags with any other characters in their key or value, these strings need to be supplied as double-quoted strings, e.g. `name="Heidelberger Brückenaffe"` or `opening_hours="24/7"`. Escape sequences can be used to represent a literal double-quote character `\"`, while a literal backslash is written as `\\`.

Whitespace such as spaces, tabs or newlines can be put freely between operators or parts of selectors (`name = *` is equivalent to `name=*`) to make a filter more readable.

Examples
--------

Here's some useful examples for filtering some OSM features:

| OSM Feature | filter | comment |
|-------------|--------|---------|
| forests/woods | `(landuse=forest or natural=wood) and geometry:polygon` | Using `geometry:polygon` will select closed ways as well as multipolygons (e.g. a forest with clearings). |
| parks and park benches | `leisure=park and geometry:polygon or amenity=bench and (geometry:point or geometry:line)` | A filter can also fetch features of different geometry types: this returns parks (polygons) as well as park benches (points or lines). |
| buildings | `building=* and building!=no and geometry:polygon` | This filter excludes the (rare) objects marked with `building=no`, which is a tag used to indicate that a feature might be expected to be a building (e.g. from an outdated aerial imagery source), but is in reality not one. |
| highways | `type:way and highway in (motorway, motorway_link, trunk, trunk_link, primary, primary_link, secondary, secondary_link, tertiary, tertiary_link, unclassified, residential, living_street, pedestrian) or (highway=service and service=alley))` | The list of used tags depends on the exact definition of a "highway". In a different context, it may also include less or even more highway tags (like `footway`, `cycleway`, `track`, `path`, all `highway=service`, etc.). |
| residential roads missing a name (for quality assurance) | `type:way and highway=residential and name!=* and noname!=yes` | Note that some roads might be actually unnamed in reality. Such features can be marked as unnamed with the [`noname`](https://wiki.openstreetmap.org/wiki/Key:noname) tag in OSM. |
| buildings with implausibly big footprints | `geometry:polygon and building=* and building!=no and area:(1E6..)` | The currently largest building by footprint area is a [car factory measuring about 887,800 m²](https://en.wikipedia.org/wiki/List_of_largest_buildings#Largest_footprint). |

