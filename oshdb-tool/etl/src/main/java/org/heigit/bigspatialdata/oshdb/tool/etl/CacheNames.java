package org.heigit.bigspatialdata.oshdb.tool.etl;

/**
 * Names for Ignite-Caches.
 */
public enum CacheNames {

  /**
   * Cache that holds Grid-OSH-Nodes.
   */
  NODES("grid_node"),
  /**
   * Cache that holds Grid-OSH-Ways.
   */
  WAYS("grid_way"),
  /**
   * Cache that holds Grid-OSH-Relations.
   */
  RELATIONS("grid_relation");

  private final String cachename;

  CacheNames(String name) {
    this.cachename = name;
  }

  public String withPrefix(String prefix){
    if(prefix != null && !prefix.trim().isEmpty())
        return prefix+"_"+cachename;
    return cachename;
  }
  
  @Override
  public String toString() {
    return cachename;
  }
}
