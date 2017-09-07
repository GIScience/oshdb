package org.heigit.bigspatialdata.oshdb.util;

import java.util.Locale;

import com.vividsolutions.jts.geom.Polygon;
import org.geotools.geometry.jts.JTS;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

public class BoundingBox {

  public final double minLon;
  public final double maxLon;
  public final double minLat;
  public final double maxLat;

  public BoundingBox(double minLon, double maxLon, double minLat, double maxLat) {
    this.minLon = minLon;
    this.maxLon = maxLon;
    this.minLat = minLat;
    this.maxLat = maxLat;
  }

  public BoundingBox(Envelope envelope) {
    this(envelope.getMinX(), envelope.getMaxX(), envelope.getMinY(), envelope.getMaxY());
  }

  @Override
  public String toString() {
    return String.format(Locale.ENGLISH,"(%f,%f) (%f,%f)", minLon, minLat, maxLon, maxLat);
  }
  
  /**
   * returns JTS geometry object for convenience
   * @return com.vividsolutions.jts.geom.Geometry
   */
  public Polygon getGeometry() {
    return JTS.toGeometry(new Envelope(minLon, maxLon, minLat, maxLon));
  }

  /**
   * calculates the intersection of two bounding boxes
   * @param first the first bounding box
   * @param second the second bounding box
   * @return the intersection of the two bboxes
   */
  public static BoundingBox intersect(BoundingBox first, BoundingBox second) {
    return new BoundingBox(
        Math.min(first.minLon, second.minLon),
        Math.max(first.maxLon, second.maxLon),
        Math.min(first.minLat, second.minLat),
        Math.max(first.maxLat, second.maxLat)
    );
  }

}
