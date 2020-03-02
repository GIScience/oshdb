package org.heigit.ohsome.filter;

import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;

/**
 * A boolean "not" of a sub-expression.
 */
public class NotOperator extends UnaryOperator {
  NotOperator(FilterExpression sub) {
    super(sub);
  }

  @Override
  public boolean applyOSM(OSMEntity e) {
    return !op.applyOSM(e);
  }

  @Override
  public FilterExpression negate() {
    return op;
  }

  @Override
  public String toString() {
    return "not(" + op.toString() + ")";
  }
}
