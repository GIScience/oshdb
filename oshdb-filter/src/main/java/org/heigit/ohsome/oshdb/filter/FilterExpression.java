package org.heigit.ohsome.oshdb.filter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.celliterator.ContributionType;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
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
   * Apply the filter to an OSH entity.
   *
   * <p>Must be compatible with the result of {@link #applyOSM}, e.g. that it must not return false
   * when <code>oshEntity.getVersions().….anyMatch(applyOSM)</code> would evaluate to true.</p>
   *
   * @param entity the OSH entity to check.
   * @return false if the filter knows that none of the versions of the OSH entity can fulfill the
   *         specified filter, true otherwise.
   */
  @Contract(pure = true)
  default boolean applyOSH(OSHEntity entity) {
    // dummy implementation for basic filters: pass all OSH entities -> check its versions instead
    return true;
  }

  /**
   * Apply the filter to an OSM entity.
   *
   * @param entity the OSM entity to check.
   * @return true if the entity fulfills the specified filter, otherwise false.
   */
  @Contract(pure = true)
  boolean applyOSM(OSMEntity entity);

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
    // dummy implementation for basic filters: ignores the geometry, just looks at the OSM entity
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
   * Apply a filter to a snapshot ({@link OSMEntitySnapshot}) of an OSM entity.
   *
   * @param snapshot a snapshot of the OSM entity to check
   * @return true if the the OSM entity snapshot fulfills the specified filter, otherwise false.
   */
  @Contract(pure = true)
  default boolean applyOSMEntitySnapshot(OSMEntitySnapshot snapshot) {
    return applyOSMGeometry(snapshot.getEntity(), snapshot::getGeometryUnclipped);
  }

  /**
   * Apply a filter to a contribution ({@link OSMEntitySnapshot}) to an OSM entity.
   *
   * <p>A contribution matches the given filter if either the state of the OSM entity before the
   * modification or the state of it after the modification matches the filter.</p>
   *
   * @param contribution a modification of the OSM entity to check
   * @return true if the the OSM contribution fulfills the specified filter, otherwise false.
   */
  @Contract(pure = true)
  default boolean applyOSMContribution(OSMContribution contribution) {
    if (contribution.is(ContributionType.CREATION)) {
      return applyOSMGeometry(contribution.getEntityAfter(),
          contribution::getGeometryUnclippedAfter);
    } else if (contribution.is(ContributionType.DELETION)) {
      return applyOSMGeometry(contribution.getEntityBefore(),
          contribution::getGeometryUnclippedBefore);
    } else {
      return applyOSMGeometry(contribution.getEntityBefore(),
          contribution::getGeometryUnclippedBefore)
          || applyOSMGeometry(contribution.getEntityAfter(),
          contribution::getGeometryUnclippedAfter);
    }
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
   * @throws IllegalStateException if the filter cannot be normalized (all filters provided by the
   *                               oshdb-filter module are normalizable, but this can occur for
   *                               user defined filter expressions) or if the size of the
   *                               normalized filter expression exceeds a defined maximum.
   */
  @Contract(pure = true)
  default List<List<Filter>> normalize() {
    final int maxResultSize = 10_000; // maximum size of a "reasonable" query filter

    if (this instanceof Filter filter) {
      return Collections.singletonList(Collections.singletonList(filter));
    } else if (this instanceof AndOperator operator) {
      List<List<Filter>> exp1 = operator.getLeftOperand().normalize();
      List<List<Filter>> exp2 = operator.getRightOperand().normalize();
      // return cross product of exp1 and exp2
      if (exp1.size() * exp2.size() > maxResultSize) {
        throw new IllegalStateException("size of normalized exceeds maximum of "
            + maxResultSize);
      }
      List<List<Filter>> crossProduct = new ArrayList<>(exp1.size() * exp2.size());
      for (List<Filter> e1 : exp1) {
        for (List<Filter> e2 : exp2) {
          List<Filter> combined = new ArrayList<>(e1.size() + e2.size());
          combined.addAll(e1);
          combined.addAll(e2);
          crossProduct.add(combined);
        }
      }
      return crossProduct;
    } else if (this instanceof OrOperator operator) {
      List<List<Filter>> exp1 = operator.getLeftOperand().normalize();
      List<List<Filter>> exp2 = operator.getRightOperand().normalize();
      List<List<Filter>> combined = new ArrayList<>(exp1.size() + exp2.size());
      combined.addAll(exp1);
      combined.addAll(exp2);
      return combined;
    } else {
      throw new IllegalStateException("unsupported state during filter normalization");
    }
  }
}
