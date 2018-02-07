package org.heigit.bigspatialdata.oshdb.util.geometry.fip;

import com.vividsolutions.jts.geom.*;

import java.util.function.Predicate;

/**
 * Fast point in (multi)polygon test inspired by
 * https://blog.jochentopf.com/2017-02-06-expedicious-and-exact-extracts-with-osmium.html
 */
public class FastPointInPolygon extends FastInPolygon implements Predicate<Point> {
  public <P extends Geometry & Polygonal> FastPointInPolygon(P geom) {
    super(geom);
  }

  /**
   * Tests if the given bounding box is fully inside of the polygon
   */
  @Override
  public boolean test(Point point) {
    return crossingNumber(point, true) % 2 == 1;
  }
}
