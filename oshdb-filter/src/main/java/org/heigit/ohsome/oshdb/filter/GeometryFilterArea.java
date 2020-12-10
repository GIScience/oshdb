package org.heigit.ohsome.oshdb.filter;

import javax.annotation.Nonnull;
import org.heigit.ohsome.oshdb.util.geometry.Geo;

/**
 * A filter which checks the area of OSM feature geometries.
 */
public class GeometryFilterArea extends GeometryFilter {
  GeometryFilterArea(@Nonnull ValueRange range) {
    super(range, GeometryMetricEvaluator.fromLambda(Geo::areaOf, "area"));
  }
}
