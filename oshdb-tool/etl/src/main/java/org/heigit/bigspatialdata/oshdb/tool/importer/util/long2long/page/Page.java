package org.heigit.bigspatialdata.oshdb.tool.importer.util.long2long.page;

public interface Page {
  public long get(int offset);
  public int weigh();
}
