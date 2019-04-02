OSHDB data model
================

The OSHDB features a specifically tailored data model, which is optimized to allow parallel access to the OSM data, is compact in size to optimally utilize available memory and allows to access all properties of the original OSM data, including erroneous or incomplete OSM data.

![schematic overview of the OSHDB data model](data-model.svg)

OSM History Data
----------------

The data model used by the OSHDB naturally closely reflects the [data structures](https://wiki.openstreetmap.org/wiki/Elements) of the OSM data.

The OpenStreetMap project regularly publishes the [**full history** dump](https://wiki.openstreetmap.org/wiki/Planet.osm/full), a data archive that not only contains the latest state of the OSM database but also includes the history of the data after the introduction of the [OSM-API version 0.5](https://wiki.openstreetmap.org/wiki/API_0.5) in October 2007. Modificatons to the OSM data are reflected by the presence of multiple versions of the same OSM element in the history dump. Another crucial part of meta-information in these dumps are the timestamps associated with the different versions of an OSM element. This makes it possible to correctly resolve references betweeen different OSM objects, wich is needed for example to get the coordinates of the nodes references by a way at a specific point in time.

Sometimes (historic) OSM data also contains erroneous data, such as ways with too few references nodes, missing referenced objects, or nodes with coordinates outside of the allowed value range (e.g., a longitude greater than 180 degrees). Tools working with OSM history data need to be able to handle these data errors.

OSH Entities
------------

The OSHDB stores all versions of a single OSM elements collectively in a so called "OSH Entity". This makes it possible to apply a delta encoding to store individual properties of the related OSM entities.

Additionally, an OSH entity also contains the data of its references members (i.e., the nodes of a way and the nodes and way members of a relation). This makes it possible to generate geometries of the OSM elements on the fly in a flexible way and for arbitrary timestamps.

OSHDB Grid Cells
----------------

The global OSM history data set is divided into a set of partitions (grid cells) that can be worked on in parallel. They also form a rough spatial index to speed up queries with a non-global area of interest. Because OSM contains features of largely varying extent (from objects as small as a single postbox up to large country borders), the used grid cell schema includes multiple zoom level layers. Large scale objects and objects that have been moved over large distances over time are stored in the lower zoom levels, while small objects are stored in the higher zoom levels. Any query performed on the OSHDB data needs to iterate through all zoom levels and all grid cells that intersect the respective area of interest.

Keytables
---------

In order to minimize memory needed to store the key and value strings of OSM tags, the OSHDB uses so called "keytables" that assign every string (e.g. the tag key `builing`) to a number. More often used strings are assigned to lower numbers compared to rarely used strings which are assigned to higher numers.

This allows the data stored in the OSH entities to be more compact compared to storing each complete string with each entity.