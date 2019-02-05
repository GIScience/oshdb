package org.heigit.bigspatialdata.oshdb-tutorial;

import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBH2;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;

public class OSHDBApiTutorial {
  public static void main(String[] args) {
    OSHDBDatabase oshdb = new OSHDBH2("path/to/extract.oshdb");
    // calculates the total area of all osm ways tagged as "building" that are not larger than
    // 1000 m² for the timestamp 2019-01-01
    Number result = OSMEntitySnapshotView.on(oshdb)
        .areaOfInterest(new OSHDBBoundingBox(8.6634,49.3965,8.7245,49.4268))
        .timestamps("2019-01-01")
        .osmType(OSMType.WAY)
        .osmTag("building")
        .map(snapshot -> Geo.areaOf(snapshot.getGeometry()))
        .filter(area -> area < 1000.0)
        .sum();
    System.out.println(result);

    // calculates the total area of all osm ways tagged as "building" that are not larger than
    // 1000 m² for yearly timestamps between 2012-01-01 and 2019-01-01
    SortedMap<OSHDBTimestamp, Number> result = OSMEntitySnapshotView.on(oshdb)
        .areaOfInterest(new OSHDBBoundingBox(8.6634,49.3965,8.7245,49.4268))
        .timestamps("2019-01-01")
        .osmType(OSMType.WAY)
        .osmTag("building")
        .map(snapshot -> Geo.areaOf(snapshot.getGeometry()))
        .filter(area -> area < 1000.0)
        .aggregateByTimestamp()
        .sum();
    System.out.println(result);
  }
}
