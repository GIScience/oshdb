ohsome filter
=============

A parser and interpreter for filters which can be applied on OSM entities. It allows, for example, to filter by various combinations of OSM tags.

[![build status](https://jenkins.ohsome.org/buildStatus/icon?job=ohsome-filter/master)](https://jenkins.ohsome.org/blue/organizations/jenkins/ohsome-filter/activity/?branch=master)
[![project status: active](https://github.com/GIScience/badges/raw/master/status/active.png)](https://github.com/GIScience/badges#active)

Usage
-----

Add the module to your maven dependencies (`pom.xml`):

```xml
<dependency>
  <groupId>org.heigit.ohsome</groupId>
  <artifactId>ohsome-filter</artifactId>
  <version>1.1-SNAPSHOT</version>
</dependency>
…
<repository>
  <snapshots>
    <enabled>true</enabled>
  </snapshots>
  <id>heigit-snapshots</id>
  <name>Heigit/GIScience repository (snapshots)</name>
  <url>http://repo.heigit.org/artifactory/libs-snapshot-local</url>
</repository>
```

Then parse filters like this:

```java
String filterText = …;
TagTranslator tagTranslator = …;
FilterParser parser = new FilterParser(tagTranslator);
FilterExpression filter = parser.parse(filterText);
```

Filters can be applied to OSM entities, for example in a OSHDB query:

```java
OSMEntitySnapshotView.on(…)
    .areaOfInterest(…)
    .timestamps(…)
    .osmEntityFilter(filter::applyOSM)
    .aggregateByTimestamp()
    .count()
    .forEach((t, val) -> System.out.println(t + "\t" + val)); 
```

If a filter should also test the geometry type of the results (by using the `geometry:…` selector, see below), one needs to apply the filter also to the respective geometries built for the OSM entities, e.g. by adding `.filter(snapshot -> filter.applyOSMGeometry(snapshot.getEntity(), snapshot.getGeometry()))` to the query above after the `aggregateByTimestamp` line.

Syntax
------

Filters are defined in textual form. A filter expression can be composed out of several actual filters, which are combined with boolean operators and parentheses. OSM entities can be filters by their tags and/or their type, for example.

### Selectors

|   | description | example |
|---|-------------|---------|
| `key=value` | matches all entites which have this exact tag | `natural=tree` |
| `key=*` | matches all entites which have any tag with the given key | `addr:housenumber=*` |
| `key!=value` | matches all entites which do not have this exact tag | `oneway!=yes` |
| `key!=*` | matches all entites which no not have any tag with the given key | `name!=*` |
| `type:osm-type` | matches all entites of the given osm type | `type:node` |
| `geometry:geom-type` | matches anything which has a geometry of the given type (_point_, _line_, _polygon_, or _other_) | `geometry:polygon` |

### Operators

|   | description | example |
|---|-------------|---------|
| `(…)` | can be used to change precedence of operators | `highway=primary and (name=* or ref=*)` |
| `not X` | negates the following filter expression | `not type:node` |
| `X and Y` | returns entities which match both filter expressions X and Y | `highway=service and service=driveway` |
| `X or Y` | returns entities which match at least one of the filter expressions X or Y | `natural=woor or landuse=forest` |

Operators follow the following order of precedence: parentheses before `not`, before `and`, before `or`.

### Special Characters & Whitespace

When writing filters, tags without special characters can be supplied directly, without needing to quote them. Example: `amenity=drinking_water` or `name:it=*`. Allowed characters are: the letters `a-z` and `A-Z`, digits, underscore, dashes and colons.
When filtering by tags with any other characters in their key or value, these strings need to be supplied as double-quoted strings, e.g. `name="Heidelberger Brückenaffe"` or `opening_hours="24/7"`. Escape sequences can be used to represent a literal double-quote character `\"`, while a literal backslash is written as `\\`.

Whitespace such as spaces, tabs or newlines can be put freely between operators or parts of selectors (`name = *` is equivalent to `name=*`) to make a filter more readable.

Documentation
-------------

* Javadoc documentation: https://docs.ohsome.org/java/ohsome-filter/ 

