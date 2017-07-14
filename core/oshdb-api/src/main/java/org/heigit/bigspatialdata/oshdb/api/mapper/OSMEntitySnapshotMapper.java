package org.heigit.bigspatialdata.oshdb.api.mapper;

import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMEntitySnapshot;

public abstract class OSMEntitySnapshotMapper {
  public static Mapper<OSMEntitySnapshot> using(OSHDB oshdb) {
    Mapper<OSMEntitySnapshot> m = Mapper.<OSMEntitySnapshot>using(oshdb);
    m._forClass =  OSMEntitySnapshot.class;
    return m;
  }
}
