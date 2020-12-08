package org.heigit.bigspatialdata.oshdb.tool.importer.transform.oshdb;

import java.util.Arrays;

public interface OSMWay extends OSMEntity{

  public OSMMember[] getMembers();
  
  @Override
  default String asString() {
    return String.format("WAY(%s) members:%s",  OSMEntity.super.asString(), Arrays.toString(getMembers()));
  }

}
