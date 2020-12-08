package org.example.oshdb.api.tutorial;

import java.util.SortedMap;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBH2;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.geometry.Geo;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps.Interval;

public class OSHDBApiTutorial {
  public static void main(String[] args) throws Exception {
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
    SortedMap<OSHDBTimestamp, Number> result2 = OSMEntitySnapshotView.on(oshdb)
        .areaOfInterest(new OSHDBBoundingBox(8.6634,49.3965,8.7245,49.4268))
        .timestamps("2012-01-01", "2019-01-01", Interval.YEARLY)
        .osmType(OSMType.WAY)
        .osmTag("building")
        .map(snapshot -> Geo.areaOf(snapshot.getGeometry()))
        .filter(area -> area < 1000.0)
        .aggregateByTimestamp()
        .sum();
    System.out.println(result2);
  }
}
