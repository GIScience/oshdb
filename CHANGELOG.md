1.2.0-SNAPSHOT
--------------

* new filter: "tag key in value-list" operator (syntax: `tag in (list, of, values)`)
* new filter: select features by their OSM id (syntax: `id: 1`, `id: (1,2,3)`, `id: (1..3)`)
* improve test coverage
* update ohsome parent to version 2.3 (for building javadoc with JDK 11)

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
