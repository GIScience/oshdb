package org.heigit.bigspatialdata.oshdb.osm2;

import java.util.Arrays;

public interface OSMRelation extends OSMEntity{

  OSMMember[] getMembers();

  @Override
  default String asString() {
     return String.format("RELATION(%s) members:%s", OSMEntity.super.asString(), Arrays.toString(getMembers()));
  }
}
