package org.heigit.ohsome.oshdb.filter;

import javax.annotation.Nonnull;
import org.heigit.ohsome.oshdb.util.geometry.Geo;

/**
 * A filter which checks the perimeter of polygonal OSM feature geometries.
 */
public class GeometryFilterSquareness extends GeometryFilter {
  /**
   * Creates a new perimeter filter object.
   *
   * @param range the allowed range (inclusive) of values to pass the filter
   */
  public GeometryFilterSquareness(@Nonnull ValueRange range) {
    super(range, GeometryMetricEvaluator.fromLambda(Geo::rectilinearity, "squareness"));
  }
}
