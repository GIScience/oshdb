# Tutorial: oshdb-api

The _oshdb-api_ is a Java-API that makes it easy to run analyses
massively in parallel on a compute cluster. This is achieved by
providing an interface to the
[MapReduce](https://en.wikipedia.org/wiki/MapReduce)
programming model.

**Remark:** If you are already familiar with
[Hadoop](https://en.wikipedia.org/wiki/Apache_Hadoop),
note that we use the terminology of the
[Java stream API](https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html)
which is slightly different. In particular, the Java stream API and
Hadoop use the same term 'combiner' for different things.

This tutorial will get you to the point of implementing your custom
analyses. It consists of the following steps:

1. [Preparation](preparation.md)
1. [Mandatory Settings](mandatory-settings.md)
1. [Entity Filters](entity-filters.md)
1. [Aggregation Settings](aggregation-settings.md)
1. [Map](map.md)
1. [Result Filters](result-filters.md)
1. [Multiple Maps and Result Filters](multiple-maps-and-filters.md)
1. [Reduce](reduce.md)
1. [Result Handling](result-handling.md)
1. [Query by neighbouring objects](neighbourhood.md)

You may also download the code examples from our
[examples repository](https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/oshdb-examples).
