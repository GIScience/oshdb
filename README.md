HeiGIT OpenStreetMap History DB
===============================

High-performance data analysis platform for OpenStreetMap full-history data.

[![Build Status](http://jenkins.ohsome.org/buildStatus/icon?job=oshdb/master)](http://jenkins.ohsome.org/job/oshdb/job/master/)

Oshdb offers a distributed data base that splits storage and computation load. It is then possible to use the [map-reduce](https://en.wikipedia.org/wiki/MapReduce) programming model to analyse the data in parallel. A central idea behind oshdb is to bring the code to the data.

```java
    OSHDBDatabase oshdb = /*…*/;

    Integer numberOfUsersEditingHighways = OSMContributionView.on(oshdb)
        .timestamps("2007-10-07", "2009-04-09")
        .where("highway")
        .map(contribution -> contribution.getContributorUserId())
        .countUniq();
        
    System.out.println(numberOfUsersEditingHighways);
```

OpenStreetMap History Data
--------------------------

OpenStreetMap contains a large variety of geographic data, for example differing in scale (ranging from single points of interests to whole country borders) or feature type (from concrete things like buildings up to more abstract concepts such as turn restrictions) and offers a lot of meta data (about individual contributors, modifications, etc.) that can be analyzed in a multitude of ways. At the same time, possibilities of working with this data is limited because of the large amount of resources needed and the lack of easy to use analysis software. The huge information treasure in OSM for researchers, journalists, community members and other interested people is thereby kept hidden. A central goal of the OSHDb is to make this treasure available for a larger public.

Central Concepts
----------------

### Data Partitioning

* *(close to osm data structures)*
* *(no lossy transformations/compressions)*
* *(partially resolved references, to speed up typical operations like "geometry building")*
* *(split into cells)*

### Data Processing/Access

* *([rest api](https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/oshdb/rest-api) for common queries)*
* *(oshdb-api: flexible, simple, map-reudce pattern, works on actual geometries)*
* *(raw-data api: direct access to underlying raw osm data)*

Installation
------------

*(install instructions: maven, link to separate developer instructions)*

API
---

* [oshdb-api tutorial](documentation/oshdb-api-tutorial/README.md)

Examples
--------

*(few examples with screenshots and links code)*

See Also
--------

* https://github.com/MaZderMind/osm-history-renderer
* https://github.com/zehpunktbarron/iOSMAnalyzer
* https://github.com/mojodna/osm2orc
* https://github.com/mapbox/osm-wayback, https://mapbox.github.io/osm-analysis-dashboard/
* https://osmstats.stevecoast.com
* *…*


