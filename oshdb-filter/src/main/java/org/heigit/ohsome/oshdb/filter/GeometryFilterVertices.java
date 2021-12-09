package org.heigit.ohsome.oshdb.filter;

import javax.annotation.Nonnull;
import org.locationtech.jts.geom.Geometry;

/**
 * A filter which checks the number of vertices of OSM feature geometries.
 */
public class GeometryFilterVertices extends GeometryFilter {
  public GeometryFilterVertices(@Nonnull ValueRange range) {
    super(range, GeometryMetricEvaluator.fromLambda(Geometry::getNumPoints, "geometry.vertices"));
  }
}
