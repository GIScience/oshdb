package org.heigit.bigspatialdata.ohsome.ohsomeapi.utils.tagfilter;

import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;

class BinaryOperator implements Operator {
  final FilterExpression e1;
  final String operator;
  final FilterExpression e2;

  BinaryOperator(FilterExpression e1, String operator, FilterExpression e2) {
    this.e1 = e1;
    this.operator = operator;
    this.e2 = e2;
  }

  @Override
  public boolean applyOSM(OSMEntity e) {
    switch (operator) {
      case "and":
        return e1.applyOSM(e) && e2.applyOSM(e);
      case "or":
        return e1.applyOSM(e) || e2.applyOSM(e);
      default:
        throw new RuntimeException("unknown operator: " + operator);
    }
  }

  @Override
  public String toString() {
    return e1.toString() + operator + e2.toString();
  }
}
