package org.heigit.ohsome.oshdb.tool.importer.transform.oshdb;

import java.util.Arrays;

public interface OSMWay extends OSMEntity {

  OSMMember[] getMembers();

  @Override
  default String asString() {
    return String.format("WAY(%s) members:%s", OSMEntity.super.asString(),
        Arrays.toString(getMembers()));
  }

}
