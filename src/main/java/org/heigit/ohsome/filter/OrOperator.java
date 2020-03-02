package org.heigit.ohsome.filter;

import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;

/**
 * A boolean "or" of two sub-expressions.
 */
public class OrOperator extends BinaryOperator {
  OrOperator(FilterExpression op1, FilterExpression op2) {
    super(op1, op2);
  }

  @Override
  public boolean applyOSM(OSMEntity e) {
    return op1.applyOSM(e) || op2.applyOSM(e);
  }

  @Override
  public boolean applyOSH(OSHEntity e) {
    return op1.applyOSH(e) || op2.applyOSH(e);
  }

  @Override
  public FilterExpression negate() {
    return new AndOperator(op1.negate(), op2.negate());
  }

  @Override
  public String toString() {
    return op1.toString() + " or " + op2.toString();
  }
}
