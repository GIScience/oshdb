package org.heigit.ohsome.oshdb.filter;

import javax.annotation.Nonnull;
import org.heigit.ohsome.oshdb.util.geometry.Geo;

/**
 * A filter which checks the squareness of OSM feature geometries.
 *
 * <p>For the measure for the rectilinearity (or squareness) of a geometry, a methods adapted from the
 * paper "A Rectilinearity Measurement for Polygons" by Joviša Žunić and Paul L. Rosin
 * (DOI:10.1007/3-540-47967-8_50, https://link.springer.com/chapter/10.1007%2F3-540-47967-8_50,
 * https://www.researchgate.net/publication/221304067_A_Rectilinearity_Measurement_for_Polygons) is used.</p>
 */
public class GeometryFilterSquareness extends GeometryFilter {
  /**
   * Creates a new squareness filter object.
   *
   * @param range the allowed range (inclusive) of values to pass the filter
   */
  public GeometryFilterSquareness(@Nonnull ValueRange range) {
    super(range, GeometryMetricEvaluator.fromLambda(Geo::rectilinearity, "squareness"));
  }
}
