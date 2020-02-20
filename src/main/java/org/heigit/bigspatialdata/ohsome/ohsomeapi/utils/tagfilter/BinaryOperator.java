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
   * Returns the first sub-expression of this binary expression.
   *
   * @return the first sub-expression of a binary expression.
   */
  public FilterExpression getExpression1() {
    return e1;
  }

  /**
   * Returns the second sub-expression of this binary expression.
   *
   * @return the second sub-expression of a binary expression.
   */
  public FilterExpression getExpression2() {
    return e2;
  }

  /**
   * Returns a new binary operator object fulfilling the given "operator" and two sub-expressions.
   *
   * @param e1 The first sub-expression.
   * @param operator The operator, such as "and" or "or".
   * @param e2 The second sub-expression.
   * @return A new binary operator object fulfilling the given "operator" on two sub-expressions.
   * @throws IllegalStateException if an unknown operator was given.
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
