package org.heigit.bigspatialdata.oshdb.api.mapper;

import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;

public class OSMEntityMapper implements MapperFactory {
  public static Mapper<OSMEntity> using(OSHDB oshdb) {
    return Mapper.<OSMEntity>using(oshdb, OSMEntity.class);
  }
}
