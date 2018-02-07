package org.heigit.bigspatialdata.oshdb.tool.importer.util.long2long.page;

public class PageLocation {
  public final long offset;
  public final int size;
  public final int rawSize;

  public PageLocation(long offset, int size, int rawSize) {
    this.offset = offset;
    this.size = size;
    this.rawSize = rawSize;
  }
}
