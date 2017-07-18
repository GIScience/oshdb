### current master

* added a new easy to use API abstraction level
	* OSMEntitySnapshotMapper – iterates over entity "snapshots" at given timestamps
	* OSMContributionMapper – iterates over all OSM contributions for each entity (i.e. creation, modifications, deletion)
* (breaking) renamed properties of `iterateAll`'s results
* (breaking) renamed `Geo.distanceOf` to `Geo.lengthOf`
* move osmatrix processing code into its own repository
* group consecutive changes by changeset in CellIterator.iterateAll
* …


### [workshop-2](https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/oshdb/core/tags/workshop-2)

Starting point of changelogs. DB schema is stable since a while, 