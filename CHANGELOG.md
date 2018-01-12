### 0.3.0

* added a new easy to use _"functional programming style"_ API abstraction level that works on local oshdb files as well as on an Ignite cluster
	* OSMEntitySnapshotMapper – iterates over entity "snapshots" at given timestamps
	* OSMContributionMapper – iterates over all OSM contributions for each entity (i.e. creation, modifications, deletion)
* (breaking) renamed properties of `CellIterator.iterateAll`'s results
* (breaking) renamed `Geo.distanceOf` to `Geo.lengthOf`
* moved osmatrix processing code into its own repository
* `CellIterator.iterateAll` now groups consecutive changes by changeset id
* added `TagTranslator` helper class
* switched logging system to slf4j
* improved javaDoc in a lot of places
* extended unit test coverage
* various bugfixes
* …

### 0.2.0

Starting point of changelogs.
First stable DB schema with data cells in multiple zoom levels.
Raw access to data is possible.