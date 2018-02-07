package org.heigit.bigspatialdata.oshdb.tool.importer.util;

public class TagId {
  public final int key;
  public final int value;
  
  public static TagId of(int key, int value){
    return new TagId(key,value);
  }
  
  private TagId(int key, int value) {
    this.key = key;
    this.value = value;
  }
  
  @Override
  public String toString() {
    return String.format("%s:%s", key,value);
  }

}
