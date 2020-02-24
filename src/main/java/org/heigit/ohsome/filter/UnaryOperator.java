package org.heigit.ohsome.filter;

/**
 * A boolean operation on a single sub-expression.
 */
abstract class UnaryOperator implements FilterExpression {
  final FilterExpression sub;

  UnaryOperator(FilterExpression sub) {
    this.sub = sub;
  }

  /**
   * Returns the sub-expression of this unary expression.
   *
   * @return the sub-expression of a unary expression.
   */
  public FilterExpression getSubExpression() {
    return sub;
  }

  /**
   * Returns a new unary operator object fulfilling the given "operator" on a sub-expression.
   *
   * @param operator The operator, such as "and" or "or".
   * @param sub The second sub-expression.
   * @return A new unary operator object fulfilling the given "operator" on a single sub-expression.
   * @throws IllegalStateException if an unknown operator was given.
   */
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
