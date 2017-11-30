### 0.3.0 SNAPSHOT (current master)

* added a new easy to use API abstraction level
	* OSMEntitySnapshotMapper – iterates over entity "snapshots" at given timestamps
	* OSMContributionMapper – iterates over all OSM contributions for each entity (i.e. creation, modifications, deletion)
* (breaking) renamed properties of `iterateAll`'s results
* (breaking) renamed `Geo.distanceOf` to `Geo.lengthOf`
* move osmatrix processing code into its own repository
* group consecutive changes by changeset in CellIterator.iterateAll
* add TagTranslator helper class
* improved javaDoc in a lot of places
* extend unit test coverage
* switch logging to slf4j
* bugfixes
* …




### 0.2.0

Approx. starting point of changelogs. DB schema is stable since a while. Raw access to data is possible.