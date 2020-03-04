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
  public boolean applyOSM(OSMEntity entity) {
    return !op.applyOSM(entity);
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
