package org.heigit.ohsome.oshdb.filter;

import javax.annotation.Nonnull;
import org.heigit.ohsome.oshdb.util.geometry.Geo;
import org.locationtech.jts.geom.Polygonal;

/**
 * A filter which checks the perimeter of polygonal OSM feature geometries.
 */
public class GeometryFilterPerimeter extends GeometryFilter {
  /**
   * Creates a new perimeter filter object.
   *
   * @param range the allowed range (inclusive) of values to pass the filter
   */
  public GeometryFilterPerimeter(@Nonnull ValueRange range) {
    super(range, GeometryMetricEvaluator.fromLambda(geometry -> {
      if (!(geometry instanceof Polygonal)) {
        return 0;
      }
      var boundary = geometry.getBoundary();
      return Geo.lengthOf(boundary);
    }, "perimeter"));
  }
}
