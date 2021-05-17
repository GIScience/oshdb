package org.heigit.ohsome.oshdb.tool.importer.transform.oshdb;

import java.util.Arrays;

public interface OSMRelation extends OSMEntity {

  OSMMember[] getMembers();

  @Override
  default String asString() {
    return String.format("RELATION(%s) members:%s", OSMEntity.super.asString(),
        Arrays.toString(getMembers()));
  }
}
