package org.example.oshdb.api.tutorial;

import java.util.SortedMap;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBH2;
import org.heigit.ohsome.oshdb.util.geometry.Geo;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshotView;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps.Interval;

public class OSHDBApiTutorial {
  public static void main(String[] args) throws Exception {
    OSHDBDatabase oshdb = new OSHDBH2("path/to/extract.oshdb");
    // calculates the total area of all osm ways tagged as "building" that are not larger than
    // 1000 m² for the timestamp 2019-01-01
    Number result = OSMEntitySnapshotView.on(oshdb)
        .areaOfInterest(OSHDBBoundingBox.bboxWgs84Coordinates(8.6634,49.3965,8.7245,49.4268))
        .timestamps("2021-01-01")
        .filter("type:way and building=*")
        .map(snapshot -> Geo.areaOf(snapshot.getGeometry()))
        .filter(area -> area < 1000.0)
        .sum();
    System.out.println(result);

    // calculates the total area of all osm ways tagged as "building" that are not larger than
    // 1000 m² for yearly timestamps between 2012-01-01 and 2019-01-01
    SortedMap<OSHDBTimestamp, Number> result2 = OSMEntitySnapshotView.on(oshdb)
        .areaOfInterest(OSHDBBoundingBox.bboxWgs84Coordinates(8.6634,49.3965,8.7245,49.4268))
        .timestamps("2012-01-01", "2021-01-01", Interval.YEARLY)
        .filter("type:way and building=*")
        .map(snapshot -> Geo.areaOf(snapshot.getGeometry()))
        .filter(area -> area < 1000.0)
        .aggregateByTimestamp()
        .sum();
    System.out.println(result2);
  }
}
