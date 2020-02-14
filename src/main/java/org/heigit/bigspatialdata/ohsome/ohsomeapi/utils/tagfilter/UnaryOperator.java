package org.heigit.bigspatialdata.ohsome.ohsomeapi.utils.tagfilter;

abstract class UnaryOperator implements Operator {
  final FilterExpression sub;

  UnaryOperator(FilterExpression sub) {
    this.sub = sub;
  }

  public static UnaryOperator fromOperator(String operator, FilterExpression sub) {
    //noinspection SwitchStatementWithTooFewBranches
    switch (operator) {
      case "not":
        return new NotOperator(sub);
      default:
        throw new IllegalStateException("unknown operator: " + operator);
    }
  }
}
