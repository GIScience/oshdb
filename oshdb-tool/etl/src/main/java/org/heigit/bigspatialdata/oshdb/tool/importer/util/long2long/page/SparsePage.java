package org.heigit.bigspatialdata.oshdb.tool.importer.util.long2long.page;

import it.unimi.dsi.fastutil.ints.Int2LongMap;

public class SparsePage implements Page {

  private final Int2LongMap cellIds;

  public SparsePage(Int2LongMap cellIds) {
    this.cellIds = cellIds;
  }

  @Override
  public long get(int offset) {
    return cellIds.get(offset);
    
  }

  @Override
  public int weigh() {
    return cellIds.size() * 4 * 8;
  }

}
