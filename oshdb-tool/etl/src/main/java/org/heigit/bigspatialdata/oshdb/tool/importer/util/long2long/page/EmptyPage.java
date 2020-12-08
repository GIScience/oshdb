package org.heigit.bigspatialdata.oshdb.tool.importer.util.long2long.page;

public class EmptyPage implements Page {

  @Override
  public long get(int offset) {
    return -1;
  }

  @Override
  public int weigh() {
    return 0;
  }

}
