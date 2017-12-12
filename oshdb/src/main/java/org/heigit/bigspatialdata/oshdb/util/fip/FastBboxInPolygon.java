package org.heigit.bigspatialdata.oshdb.util.fip;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.Polygonal;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;

import java.util.function.Predicate;

/**
 * Fast bounding-box in (multi)polygon test inspired by
 * https://blog.jochentopf.com/2017-02-06-expedicious-and-exact-extracts-with-osmium.html
 */
public class FastBboxInPolygon extends FastInPolygon implements Predicate<BoundingBox> {
    public <P extends Geometry & Polygonal> FastBboxInPolygon(P geom) {
        super(geom);
    }

    /**
     * Tests if the given bounding box is fully inside of the polygon
     */
    @Override
    public boolean test(BoundingBox boundingBox) {
        if (crossingNumber(boundingBox.getGeometry().getCentroid(), true) % 2 == 0) {
            return false;
        }
        Polygon g = boundingBox.getGeometry();
        Point p1 = g.getExteriorRing().getPointN(0);
        Point p2 = g.getExteriorRing().getPointN(1);
        Point p3 = g.getExteriorRing().getPointN(2);
        Point p4 = g.getExteriorRing().getPointN(3);
        return crossingNumber(p1, true) == crossingNumber(p2, true)
            && crossingNumber(p3, true) == crossingNumber(p4, true)
            && crossingNumber(p2, false) == crossingNumber(p3, false)
            && crossingNumber(p4, false) == crossingNumber(p1, false);
    }
}
