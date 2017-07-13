package org.heigit.bigspatialdata.oshdb.api.mapper;

import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;

public class OSMEntityMapper {
  public static Mapper<OSMEntity> using(OSHDB oshdb) {
    Mapper<OSMEntity> m = Mapper.<OSMEntity>using(oshdb);
    m._forClass = OSMEntity.class;
    return m;
  }
}
