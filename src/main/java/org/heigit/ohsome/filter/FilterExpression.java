package org.heigit.ohsome.filter;

import com.google.common.collect.Streams;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.jetbrains.annotations.Contract;

/**
 * Represents a filter expression which can be applied on OSM/OSH entities.
 *
 * <p>Such an expression might be a simple key=value tag filter, or a more complex combination
 * of boolean operators, parentheses, tag filters and/or other filters.</p>
 */
public interface FilterExpression {
  /**
   * Apply the filter to an OSM entity.
   *
   * @param e the OSM entity to check.
   * @return true if the entity fulfills the specified filter, otherwise false.
   */
  @Contract(pure = true)
  boolean applyOSM(OSMEntity e);

  /**
   * Apply the filter to an OSH entity.
   *
   * <p>Must return the same as <code>oshEntity.getVersions().….anyMatch(applyOSM)</code>.</p>
   *
   * @param e the OSH entity to check.
   * @return true if the at least one of the OSH entity's versions fulfills the specified filter,
   *         false otherwise.
   */
  @Contract(pure = true)
  default boolean applyOSH(OSHEntity e) {
    return Streams.stream(e.getVersions()).anyMatch(this::applyOSM);
  }

  /**
   * Returns the opposite of the current filter expression.
   *
   * <p>Must never return anything based on a NotOperator.
   * Non-negatable expressions should throw a runtime exception instead.</p>
   *
   * @return the opposite of the current filter expression.
   */
  @Contract(pure = true)
  FilterExpression negate();

  /**
   * Converts a random boolean expression into a disjunctive normal form.
   *
   * <p>for example: A∧(B∨C) ⇔ (A∧B)∨(A∧C)</p>
   *
   * @return a disjunction of conjunctions of filter expressions: A∧B∧… ∨ C∧D∧… ∨ …
   */
  default List<List<Filter>> normalize() {
    if (this instanceof Filter) {
      return Collections.singletonList(Collections.singletonList((Filter) this));
    } else if (this instanceof NotOperator) {
      return ((NotOperator) this).getOperand().negate().normalize();
    } else if (this instanceof AndOperator) {
      List<List<Filter>> exp1 = ((BinaryOperator) this).getLeftOperand().normalize();
      List<List<Filter>> exp2 = ((BinaryOperator) this).getRightOperand().normalize();
      // return cross product of exp1 and exp2
      List<List<Filter>> c = new LinkedList<>();
      for (List<Filter> e1 : exp1) {
        for (List<Filter> e2 : exp2) {
          List<Filter> and = new ArrayList<>(e1.size() + e2.size());
          and.addAll(e1);
          and.addAll(e2);
          c.add(and);
        }
      }
      return c;
    } else if (this instanceof OrOperator) {
      List<List<Filter>> exp1 = ((BinaryOperator) this).getLeftOperand().normalize();
      List<List<Filter>> exp2 = ((BinaryOperator) this).getRightOperand().normalize();
      List<List<Filter>> or = new ArrayList<>(exp1.size() + exp2.size());
      or.addAll(exp1);
      or.addAll(exp2);
      return or;
    } else {
      String error = "unsupported state during filter normalization";
      assert false : error;
      throw new IllegalStateException(error);
    }
  }
}
