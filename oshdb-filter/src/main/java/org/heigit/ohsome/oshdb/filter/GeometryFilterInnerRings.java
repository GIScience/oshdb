package org.heigit.ohsome.oshdb.filter;

import javax.annotation.Nonnull;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;

/**
 * A filter which checks the number of inner rings of a multipolygon relation.
 */
public class GeometryFilterInnerRings extends GeometryFilter {
  /**
   * Creates a new inner rings filter object.
   *
   * @param range the allowed range (inclusive) of values to pass the filter
   */
  public GeometryFilterInnerRings(@Nonnull ValueRange range) {
    super(range, GeometryMetricEvaluator.fromLambda(geometry -> {
      if (geometry instanceof Polygon) {
        return ((Polygon) geometry).getNumInteriorRing();
      } else if (geometry instanceof MultiPolygon) {
        var counter = 0;
        for (var i = 0; i < geometry.getNumGeometries(); i++) {
          counter += ((Polygon) geometry.getGeometryN(i)).getNumInteriorRing();
        }
        return counter;
      } else {
        return -1;
      }
    }, "geometry.inners"));
  }
}
