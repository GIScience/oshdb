package org.heigit.ohsome.oshdb.filter;

import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.jetbrains.annotations.Contract;

/**
 * A filter which selects OSM entities by their id.
 */
public class IdFilterNotEquals implements Filter {
  private final long id;

  IdFilterNotEquals(long id) {
    this.id = id;
  }

  @Contract(pure = true)
  public long getId() {
    return this.id;
  }

  @Override
  public boolean applyOSM(OSMEntity entity) {
    return entity.getId() != id;
  }

  @Override
  public boolean applyOSH(OSHEntity entity) {
    return entity.getId() != id;
  }

  @Override
  public FilterExpression negate() {
    return new IdFilterEquals(this.id);
  }

  @Override
  public String toString() {
    return "not-id:" + this.id;
  }
}
