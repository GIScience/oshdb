package org.heigit.bigspatialdata.oshdb.osm2;

import java.util.Arrays;

public interface OSMWay extends OSMEntity{

  public OSMMember[] getMembers();
  
  @Override
  default String asString() {
    return String.format("WAY(%s) members:%s",  OSMEntity.super.asString(), Arrays.toString(getMembers()));
  }

}
