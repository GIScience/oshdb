package org.heigit.ohsome.oshdb.tool.importer.util;

public interface TagToIdMapper {

  int getKey(String key);

  int getValue(int key, String value);

  TagId getTag(String key, String value);

  long estimatedSize();

}
