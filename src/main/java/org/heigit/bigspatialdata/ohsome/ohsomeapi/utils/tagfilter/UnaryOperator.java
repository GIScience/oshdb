package org.heigit.bigspatialdata.ohsome.ohsomeapi.utils.tagfilter;

import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;

class UnaryOperator extends Operator {
  final String operator;
  final FilterExpression sub;

  UnaryOperator(String operator, FilterExpression sub) {
    this.operator = operator;
    this.sub = sub;
  }

  @Override
  public boolean applyOSM(OSMEntity e) {
    //noinspection SwitchStatementWithTooFewBranches
    switch (operator) {
      case "not":
        return !sub.applyOSM(e);
      default:
        throw new RuntimeException("unknown operator: " + operator);
    }
  }

  @Override
  public String toString() {
    return operator + "(" + sub.toString() + ")";
  }
}
