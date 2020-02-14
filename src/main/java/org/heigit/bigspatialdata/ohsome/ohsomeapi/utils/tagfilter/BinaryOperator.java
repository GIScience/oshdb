package org.heigit.bigspatialdata.ohsome.ohsomeapi.utils.tagfilter;

abstract class BinaryOperator implements Operator {
  final FilterExpression e1;
  final FilterExpression e2;

  BinaryOperator(FilterExpression e1, FilterExpression e2) {
    this.e1 = e1;
    this.e2 = e2;
  }

  public static BinaryOperator fromOperator(
      FilterExpression e1,
      String operator,
      FilterExpression e2
  ) {
    switch (operator) {
      case "and":
        return new AndOperator(e1, e2);
      case "or":
        return new OrOperator(e1, e2);
      default:
        throw new IllegalStateException("unknown operator: " + operator);
    }
  }
}
