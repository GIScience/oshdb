package org.heigit.ohsome.oshdb.filter;

import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.jetbrains.annotations.Contract;

/**
 * A filter which selects OSM entities by their id.
 */
public class IdFilterEquals implements Filter {
  private final long id;

  IdFilterEquals(long id) {
    this.id = id;
  }

  /**
   * Returns the OSM type of this filter.
   *
   * @return the OSM type of this filter.
   */
  @Contract(pure = true)
  public long getId() {
    return this.id;
  }

  @Override
  public boolean applyOSM(OSMEntity entity) {
    return entity.getId() == id;
  }

  @Override
  public boolean applyOSH(OSHEntity entity) {
    return entity.getId() == id;
  }

  @Override
  public FilterExpression negate() {
    return new IdFilterNotEquals(this.id);
  }

  @Override
  public String toString() {
    return "id:" + this.id;
  }
}
