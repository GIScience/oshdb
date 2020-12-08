1.5-SNAPSHOT
------------

* mark as compatible with OSHDB version 0.6

1.4.0
-----

* allow using applyOSMGeometry with a supplier method
* added convenience methods to create simple custom filters from predicates/lambdas: `Filter.by`, `Filter.byOSMEntity`, `Filter.byOSHEntity`

1.3.0
-----

* new filter: geometry filters return only features with a certain range of length (in meters) or area (in m²)
* new filter: id filter allows to select by type and id in a single expression (syntax: `id:node/42`)
* be a bit more forgiving with extra or omitted whitespace around parentheses

1.2.0
-----

* new filter: "tag key in value-list" operator (syntax: `tag in (list, of, values)`)
* new filter: select features by their OSM id (syntax: `id: 1`, `id: (1,2,3)`, `id: (1..3)`)
* allow parsing of empty filters which end up not filtering anything
* update ohsome parent to version 2.4 (upgrades build environment to JDK 11)
* improve test coverage

1.1.4
-----

* fix a bug triggered when serializing a geometry-type filter
* add license files (LGPL)

1.1.3
-----

* switch to ohsome-parent module
* replace local checkstyle config with ohsome group definitions
* prepare for deployment to central

1.1.2
-----

* Fix more spelling of keywords in error messages.

1.1.1
-----

* Improve documentation by adding some examples to readme.

1.1.0
-----

Adds support for geometry type filters (`geometry:…`).

1.0.0
-----

First release.
Supports filters for tags (`key=value`, `key!=value`, `key=*`, `key!=*`), OSM type (`type:…`), boolean operators (`and`, `or`, `not`) and parentheses.
