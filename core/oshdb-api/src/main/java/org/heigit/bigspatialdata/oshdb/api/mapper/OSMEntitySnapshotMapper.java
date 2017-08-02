package org.heigit.bigspatialdata.oshdb.api.mapper;

import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMEntitySnapshot;

public class OSMEntitySnapshotMapper {
  public static Mapper<OSMEntitySnapshot> using(OSHDB oshdb) {
    return Mapper.<OSMEntitySnapshot>using(oshdb, OSMEntitySnapshot.class);
  }
}
