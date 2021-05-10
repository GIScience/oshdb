package org.heigit.ohsome.oshdb.util.geometry.fip;

import java.util.function.Predicate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygonal;

/**
 * Fast point in (multi)polygon test inspired by
 * <a href="https://blog.jochentopf.com/2017-02-06-expedicious-and-exact-extracts-with-osmium.html">
 * https://blog.jochentopf.com/2017-02-06-expedicious-and-exact-extracts-with-osmium.html</a>.
 */
public class FastPointInPolygon extends FastInPolygon implements Predicate<Point> {
  public <P extends Geometry & Polygonal> FastPointInPolygon(P geom) {
    super(geom);
  }

  /**
   * Tests if the given bounding box is fully inside of the polygon.
   */
  @Override
  public boolean test(Point point) {
    return crossingNumber(point, true) % 2 == 1;
  }
}
