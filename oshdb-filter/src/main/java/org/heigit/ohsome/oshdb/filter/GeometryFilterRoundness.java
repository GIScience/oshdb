package org.heigit.ohsome.oshdb.filter;

import javax.annotation.Nonnull;
import org.heigit.ohsome.oshdb.util.geometry.Geo;

/**
 * A filter which checks the roundness of polygonal OSM feature geometries.
 *
 * <p>Uses the Polsby-Popper test score, see
 * <a href="https://en.wikipedia.org/wiki/Polsby%E2%80%93Popper_test">wikipedia</a> for details.</p>
 */
public class GeometryFilterRoundness extends GeometryFilter {
  /**
   * Creates a new "roundness" filter object.
   *
   * @param range the allowed range (inclusive) of values to pass the filter
   */
  public GeometryFilterRoundness(@Nonnull ValueRange range) {
    super(range, GeometryMetricEvaluator.fromLambda(Geo::roundness, "geometry.roundness"));
  }
}
