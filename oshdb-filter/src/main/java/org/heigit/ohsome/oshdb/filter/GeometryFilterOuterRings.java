package org.heigit.ohsome.oshdb.filter;

import javax.annotation.Nonnull;
import org.locationtech.jts.geom.Polygonal;

/**
 * A filter which checks the number of outer rings of a multipolygon relation.
 */
public class GeometryFilterOuterRings extends GeometryFilter {
  /**
   * Creates a new outer rings filter object.
   *
   * @param range the allowed range (inclusive) of values to pass the filter
   */
  public GeometryFilterOuterRings(@Nonnull ValueRange range) {
    super(range, GeometryMetricEvaluator.fromLambda(geometry ->
            geometry instanceof Polygonal ? geometry.getNumGeometries() : -1,
        "outers"));
  }
}
