package org.heigit.ohsome.filter;

/**
 * A boolean operator with two sub-expressions.
 */
abstract class BinaryOperator implements FilterExpression {
  final FilterExpression op1;
  final FilterExpression op2;

  BinaryOperator(FilterExpression op1, FilterExpression op2) {
    this.op1 = op1;
    this.op2 = op2;
  }

  /**
   * Returns the first (left) operand of this binary expression.
   *
   * @return the left operand of a binary expression.
   */
  public FilterExpression getLeftOperand() {
    return op1;
  }

  /**
   * Returns the right operand of this binary expression.
   *
   * @return the right operand of a binary expression.
   */
  public FilterExpression getRightOperand() {
    return op2;
  }

  /**
   * Returns a new binary operator object fulfilling the given "operator" and two operands
   * (a "left" and a "right" sub-expressions).
   *
   * @param leftOperand The left operand.
   * @param operator The operator, such as "and" or "or".
   * @param rightOperand The right operand.
   * @return A new binary operator object fulfilling the given "operator" on two sub-expressions.
   * @throws IllegalStateException if an unknown operator was given.
   */
  public static BinaryOperator fromOperator(
      FilterExpression leftOperand,
      String operator,
      FilterExpression rightOperand
  ) {
    switch (operator) {
      case "and":
        return new AndOperator(leftOperand, rightOperand);
      case "or":
        return new OrOperator(leftOperand, rightOperand);
      default:
        throw new IllegalStateException("unknown operator: " + operator);
    }
  }
}
