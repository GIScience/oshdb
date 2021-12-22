OSHDB User Manual
=================

Background
----------

The OSHDB was initiated as a project to make it easier for researchers to investigate data quality aspects of the OpenStreetMap (OSM) data though analysing its history.

OSM is a rich resource of freely available geographic information. Analysing historical OSM data provides great insight into the evolution of the map, and supports assessing OSM data quality intrinsically, i.e., without the need for external reference data sets. However, the possibilities for analysing the OSM history data on a global scale are limited because of the large amount of resources needed and the lack of easy to use analytics software. That is, OSM’s huge information treasure for researchers, journalists, community members and other interested people is kept hidden.

The central idea of the OSHDB is to make this data treasure available for a larger public and to develop further analysis functionality, e.g. for intrinsic data quality analytics.

Contents
--------

* [OSHDB API Manual](api.md) <br>
  Shows the different features of the OSHDB API and how they can be used to efficiently query the OSM history data.
  * [Data Views](views.md)
  * [OSM Feature Geometries](geometries.md)
  * [Filtering of OSM data](filters.md)
  * [Map and Reduce](map-reduce.md)
  * [Data Aggregation](aggregation.md)
* [Database Backends](database-backends.md) <br>
  Lists the available database backends and how they are used.
* [Data Model Manual](data-model.md) <br>
  Explains the design of the OSHDB data model.
* [Installation](installation.md) <br>
  A guide for how to install the OSHDB from sources.

See also
--------

* [first steps tutorial](../first-steps)
* [examples repository](https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/oshdb-examples)