package org.heigit.bigspatialdata.oshdb.util.geometry.fip;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Predicate;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.Polygonal;

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
    GeometryFactory gf = new GeometryFactory();
    Point p1 = gf.createPoint(new Coordinate(boundingBox.getMinLon(), boundingBox.getMinLat()));
    if (crossingNumber(p1, true) % 2 == 0) {
      return false;
    }
    Point p2 = gf.createPoint(new Coordinate(boundingBox.getMaxLon(), boundingBox.getMinLat()));
    Point p3 = gf.createPoint(new Coordinate(boundingBox.getMaxLon(), boundingBox.getMaxLat()));
    Point p4 = gf.createPoint(new Coordinate(boundingBox.getMinLon(), boundingBox.getMaxLat()));
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
