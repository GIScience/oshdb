package org.heigit.ohsome.oshdb.filter;

import javax.annotation.Nonnull;
import org.jetbrains.annotations.Contract;

/**
 * A boolean operator with two sub-expressions.
 */
public abstract class BinaryOperator implements FilterExpression {
  enum Type {
    AND,
    OR
  }

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
   */
  @Contract(pure = true)
  public static BinaryOperator fromOperator(
      FilterExpression leftOperand,
      @Nonnull Type operator,
      FilterExpression rightOperand
  ) {
    switch (operator) {
      case AND:
        return new AndOperator(leftOperand, rightOperand);
      case OR:
        return new OrOperator(leftOperand, rightOperand);
      default:
        assert false : "invalid or null binary operator encountered";
        throw new IllegalStateException("invalid or null binary operator encountered");
    }
  }
}
