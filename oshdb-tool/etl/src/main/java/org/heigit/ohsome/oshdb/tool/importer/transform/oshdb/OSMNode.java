package org.heigit.ohsome.oshdb.tool.importer.transform.oshdb;

public interface OSMNode extends OSMEntity{

  long getLon();
  long getLat();
  
  @Override
  default String asString() {
     return String.format("NODE(%s) %d:%d", OSMEntity.super.asString(), getLon(), getLat());
  }


}
