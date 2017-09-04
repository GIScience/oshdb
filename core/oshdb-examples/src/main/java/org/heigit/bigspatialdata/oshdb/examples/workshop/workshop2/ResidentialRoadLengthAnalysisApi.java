package org.heigit.bigspatialdata.oshdb.examples.workshop.workshop2;

import java.util.EnumSet;
import java.util.Map;
import java.util.SortedMap;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_H2;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.api.objects.OSHDBTimestamps;
import org.heigit.bigspatialdata.oshdb.api.mapper.OSMEntitySnapshotMapper;
import org.heigit.bigspatialdata.oshdb.api.objects.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.Geo;
import org.heigit.bigspatialdata.oshdb.util.OSMType;

public class ResidentialRoadLengthAnalysisApi {
  public static void main(String[] args) throws Exception {
    // database
    OSHDB oshdb = new OSHDB_H2("./karlsruhe-regbez")
        .multithreading(true);

    // query
    SortedMap<OSHDBTimestamp, Number> result = OSMEntitySnapshotMapper.using(oshdb)
        .boundingBox(new BoundingBox(8.6528,8.7294, 49.3683,49.4376))
        .timestamps(new OSHDBTimestamps(2008, 2017, 1, 12))
        .osmTypes(EnumSet.of(OSMType.WAY))
        .filterByTagValue("highway", "residential")
        .filterByTagKey("maxspeed")
        .sumAggregateByTimestamp(snapshot -> Geo.lengthOf(snapshot.getGeometry()));

    // output
    for (Map.Entry<OSHDBTimestamp, Number> entry : result.entrySet())
      System.out.format("%s\t%.2f\n", entry.getKey().formatDate(), entry.getValue().doubleValue());
  }
}
