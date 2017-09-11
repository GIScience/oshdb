package org.heigit.bigspatialdata.oshdb.examples.workshop.workshop3;

import java.util.Map;
import java.util.SortedMap;

import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_H2;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.api.objects.OSHDBTimestamps;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.api.objects.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.Geo;

public class ResidentialRoadLengthAnalysisApi {
  public static void main(String[] args) throws Exception {
    // database
    OSHDB_H2 oshdb = (new OSHDB_H2("./baden-wuerttemberg.oshdb")).multithreading(true);
    OSHDB_H2 oshdbKeytables = new OSHDB_H2("./keytables");

    // query
    SortedMap<OSHDBTimestamp, Number> result = OSMEntitySnapshotView.on(oshdb).keytables(oshdbKeytables)
        .areaOfInterest(new BoundingBox(8.6528,8.7294, 49.3683,49.4376))
        .timestamps(new OSHDBTimestamps(2008, 2017, 1, 12))
        .osmTypes(OSMType.WAY)
        .filterByTagValue("highway", "residential")
        .filterByTagKey("maxspeed")
        .sumAggregateByTimestamp(snapshot -> Geo.lengthOf(snapshot.getGeometry()));

    // output
    for (Map.Entry<OSHDBTimestamp, Number> entry : result.entrySet())
      System.out.format("%s\t%.2f\n", entry.getKey().formatDate(), entry.getValue().doubleValue());
  }
}
