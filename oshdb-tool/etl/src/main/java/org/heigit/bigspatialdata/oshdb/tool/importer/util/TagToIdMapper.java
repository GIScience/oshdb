package org.heigit.bigspatialdata.oshdb.tool.importer.util;

public interface TagToIdMapper {
  
  public int getKey(String key);
  public int getValue(int key, String value);
  public TagId getTag(String key, String value);
  public long estimatedSize();
  
}
