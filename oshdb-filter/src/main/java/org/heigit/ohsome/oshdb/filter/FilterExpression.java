package org.heigit.ohsome.oshdb.filter;

import com.google.common.collect.Streams;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.jetbrains.annotations.Contract;
import org.locationtech.jts.geom.Geometry;

/**
 * Represents a filter expression which can be applied on OSM/OSH entities.
 *
 * <p>Such an expression might be a simple key=value tag filter, or a more complex combination
 * of boolean operators, parentheses, tag filters and/or other filters.</p>
 */
public interface FilterExpression extends Serializable {

  /**
   * Apply the filter to an OSM entity.
   *
   * @param entity the OSM entity to check.
   * @return true if the entity fulfills the specified filter, otherwise false.
   */
  @Contract(pure = true)
  boolean applyOSM(OSMEntity entity);

  /**
   * Apply the filter to an OSH entity.
   *
   * <p>Must return the same as <code>oshEntity.getVersions().….anyMatch(applyOSM)</code>.</p>
   *
   * @param entity the OSH entity to check.
   * @return true if the at least one of the OSH entity's versions fulfills the specified filter,
   *         false otherwise.
   */
  @Contract(pure = true)
  default boolean applyOSH(OSHEntity entity) {
    return Streams.stream(entity.getVersions()).anyMatch(this::applyOSM);
  }

  /**
   * Apply the filter to an "OSM feature" (i.e. an entity with a geometry).
   *
   * <p>The default implementation doesn't perform a geometry type check, but any Filter can
   * override it to do so.</p>
   *
   * @param entity the OSM entity to check.
   * @param geometrySupplier a function returning the geometry of this OSM feature to check.
   * @return true if the OSM feature fulfills the specified filter, otherwise false.
   */
  @Contract(pure = true)
  default boolean applyOSMGeometry(OSMEntity entity, Supplier<Geometry> geometrySupplier) {
    return applyOSM(entity);
  }

  /**
   * Apply the filter to an "OSM feature" (i.e. an entity with a geometry).
   *
   * <p>The default implementation doesn't perform a geometry type check, but any Filter can
   * override it to do so.</p>
   *
   * @param entity the OSM entity to check.
   * @param geometry the geometry of this OSM feature to check.
   * @return true if the OSM feature fulfills the specified filter, otherwise false.
   */
  @Contract(pure = true)
  default boolean applyOSMGeometry(OSMEntity entity, Geometry geometry) {
    return applyOSMGeometry(entity, () -> geometry);
  }

  /**
   * Returns the opposite of the current filter expression.
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
  @Contract(pure = true)
  default List<List<Filter>> normalize() {
    if (this instanceof Filter) {
      return Collections.singletonList(Collections.singletonList((Filter) this));
    } else if (this instanceof AndOperator) {
      List<List<Filter>> exp1 = ((BinaryOperator) this).getLeftOperand().normalize();
      List<List<Filter>> exp2 = ((BinaryOperator) this).getRightOperand().normalize();
      // return cross product of exp1 and exp2
      List<List<Filter>> combined = new LinkedList<>();
      for (List<Filter> e1 : exp1) {
        for (List<Filter> e2 : exp2) {
          List<Filter> crossProduct = new ArrayList<>(e1.size() + e2.size());
          crossProduct.addAll(e1);
          crossProduct.addAll(e2);
          combined.add(crossProduct);
        }
      }
      return combined;
    } else if (this instanceof OrOperator) {
      List<List<Filter>> exp1 = ((BinaryOperator) this).getLeftOperand().normalize();
      List<List<Filter>> exp2 = ((BinaryOperator) this).getRightOperand().normalize();
      List<List<Filter>> combined = new ArrayList<>(exp1.size() + exp2.size());
      combined.addAll(exp1);
      combined.addAll(exp2);
      return combined;
    } else {
      String error = "unsupported state during filter normalization";
      assert false : error;
      throw new IllegalStateException(error);
    }
  }
}
