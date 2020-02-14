package org.heigit.bigspatialdata.ohsome.ohsomeapi.utils.tagfilter;

/**
 * A boolean operator with two sub-expressions.
 */
abstract class BinaryOperator implements FilterExpression {
  final FilterExpression e1;
  final FilterExpression e2;

  BinaryOperator(FilterExpression e1, FilterExpression e2) {
    this.e1 = e1;
    this.e2 = e2;
  }

  /**
   * Returns a new binary operator object fulfilling the given "operator" and two sub-expressions.
   *
   * @param e1 The first sub-expression.
   * @param operator The operator, such as "and" or "or".
   * @param e2 The second sub-expression.
   * @throws IllegalStateException if an unknown operator was given.
   * @return A new binary operator object fulfilling the given "operator" on two sub-expressions.
   */
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
