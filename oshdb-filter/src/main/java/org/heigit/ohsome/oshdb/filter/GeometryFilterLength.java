package org.heigit.ohsome.oshdb.filter;

import javax.annotation.Nonnull;
import org.heigit.ohsome.oshdb.util.geometry.Geo;

/**
 * A filter which checks the length of OSM feature geometries.
 */
public class GeometryFilterLength extends GeometryFilter {
  public GeometryFilterLength(@Nonnull ValueRange range) {
    super(range, GeometryMetricEvaluator.fromLambda(Geo::lengthOf, "length"));
  }
}
