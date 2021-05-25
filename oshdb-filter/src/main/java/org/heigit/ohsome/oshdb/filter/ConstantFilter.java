package org.heigit.ohsome.oshdb.filter;

import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.jetbrains.annotations.Contract;

/**
 * A filter which always evaluates to true or false. Used internally to represent empty filters.
 */
public class ConstantFilter implements FilterExpression {
  private final boolean state;

  ConstantFilter(boolean state) {
    this.state = state;
  }

  /** Returns the true/false state of this filter. */
  @Contract(pure = true)
  public boolean getState() {
    return this.state;
  }

  @Override
  public boolean applyOSM(OSMEntity entity) {
    return this.state;
  }

  @Override
  public boolean applyOSH(OSHEntity entity) {
    return this.state;
  }

  @Override
  public FilterExpression negate() {
    return new ConstantFilter(!this.state);
  }

  @Override
  public String toString() {
    return Boolean.toString(this.state);
  }
}
