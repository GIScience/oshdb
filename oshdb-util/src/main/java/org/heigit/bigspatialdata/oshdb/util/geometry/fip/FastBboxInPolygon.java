package org.heigit.bigspatialdata.oshdb.util.geometry.fip;

import com.vividsolutions.jts.geom.*;
import java.io.Serializable;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Predicate;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;

/**
 * Fast bounding-box in (multi)polygon test inspired by
 * https://blog.jochentopf.com/2017-02-06-expedicious-and-exact-extracts-with-osmium.html
 */
public class FastBboxInPolygon extends FastInPolygon implements Predicate<OSHDBBoundingBox>,
    Serializable {
  private Collection<Envelope> innerBboxes = new ArrayList<>();

  public <P extends Geometry & Polygonal> FastBboxInPolygon(P geom) {
    super(geom);

    List<Polygon> polys = new LinkedList<>();
    if (geom instanceof Polygon) {
      polys.add((Polygon)geom);
    } else if (geom instanceof MultiPolygon) {
      MultiPolygon mp = (MultiPolygon)geom;
      for (int i=0; i<mp.getNumGeometries(); i++)
        polys.add((Polygon)mp.getGeometryN(i));
    }
    for (Polygon poly : polys) {
      for (int i=0; i<poly.getNumInteriorRing(); i++) {
        innerBboxes.add(poly.getInteriorRingN(i).getEnvelopeInternal());
      }
    }
  }

  /**
   * Tests if the given bounding box is fully inside of the polygon
   */
  @Override
  public boolean test(OSHDBBoundingBox boundingBox) {
    Polygon g = OSHDBGeometryBuilder.getGeometry(boundingBox);
    Point p1 = g.getExteriorRing().getPointN(0);
    if (crossingNumber(p1, true) % 2 == 0) {
      return false;
    }
    Point p2 = g.getExteriorRing().getPointN(1);
    Point p3 = g.getExteriorRing().getPointN(2);
    Point p4 = g.getExteriorRing().getPointN(3);
    if (crossingNumber(p1, true) != crossingNumber(p2, true) ||
        crossingNumber(p3, true) != crossingNumber(p4, true) ||
        crossingNumber(p2, false) != crossingNumber(p3, false) ||
        crossingNumber(p4, false) != crossingNumber(p1, false)) {
      return false; // at least one of the bbox'es edges crosses the polygon
    }
    for (Envelope innerBBox : innerBboxes) {
      if (boundingBox.getMinLat() <= innerBBox.getMinY() && boundingBox.getMaxLat() >= innerBBox.getMaxY() &&
          boundingBox.getMinLon() <= innerBBox.getMinX() && boundingBox.getMaxLon() >= innerBBox.getMaxX()) {
        return false; // the bounding box fully covers at least one of the polygon's inner rings
      }
    }
    return true;
  }
}
