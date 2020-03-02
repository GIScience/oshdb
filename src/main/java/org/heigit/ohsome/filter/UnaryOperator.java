package org.heigit.ohsome.filter;

/**
 * A boolean operation on a single sub-expression.
 */
abstract class UnaryOperator implements FilterExpression {
  final FilterExpression op;

  UnaryOperator(FilterExpression op) {
    this.op = op;
  }

  /**
   * Returns the operand of this unary expression.
   *
   * @return the operand of a unary expression.
   */
  public FilterExpression getOperand() {
    return op;
  }

  /**
   * Returns a new unary operator object fulfilling the given "operator" on an operand
   * (a single sub-expression).
   *
   * @param operator The operator, such as "not".
   * @param operand The operand of this unary operation.
   * @return A new unary operator object fulfilling the given "operator" on a single operand.
   * @throws IllegalStateException if an unknown operator was given.
   */
  public static UnaryOperator fromOperator(String operator, FilterExpression operand) {
    //noinspection SwitchStatementWithTooFewBranches
    switch (operator) {
      case "not":
        return new NotOperator(operand);
      default:
        throw new IllegalStateException("unknown operator: " + operator);
    }
  }
}
