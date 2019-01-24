package org.heigit.bigspatialdata.oshdb.api.db;

import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.object.OSHDBMapReducible;

/**
 * OSHDB database backend connector.
 */
public abstract class OSHDBDatabase extends OSHDB implements AutoCloseable {
  private String prefix = "";

  /**
   * Factory function that creates a mapReducer object of the appropriate data type class for this
   * oshdb backend implemenation.
   *
   * @param forClass the data type class to iterate over in the `mapping` function of the generated
   *        MapReducer
   * @return a new mapReducer object operating on the given OSHDB backend
   */
  public abstract <X extends OSHDBMapReducible> MapReducer<X> createMapReducer(Class<X> forClass);

  /**
   * Returns metadata about the given OSHDB.
   *
   * <p>For example copyright information, currentness of the data, spatial extent, etc.</p>
   */
  public abstract String metadata(String property);

  /**
   * Sets the "table/cache" name prefix to be used with this oshdb.
   */
  public OSHDBDatabase prefix(String prefix) {
    this.prefix = prefix;
    return this;
  }

  /**
   * Returns the currently set db "table/cache" name prefix.
   */
  public String prefix() {
    return this.prefix;
  }
}
