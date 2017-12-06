package org.heigit.bigspatialdata.oshdb.util;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Polygon;

import java.io.Serializable;
import java.util.Locale;
import org.geotools.geometry.jts.JTS;

public class BoundingBox implements Serializable {

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
    return String.format(Locale.ENGLISH, "(%f,%f) (%f,%f)", minLon, minLat, maxLon, maxLat);
  }

  /**
   * returns JTS geometry object for convenience
   *
   * @return com.vividsolutions.jts.geom.Geometry
   */
  public Polygon getGeometry() {
    return JTS.toGeometry(new Envelope(minLon, maxLon, minLat, maxLat));
  }

  /**
   * calculates the intersection of two bounding boxes
   *
   * @param first the first bounding box
   * @param second the second bounding box
   * @return the intersection of the two bboxes
   */
  public static BoundingBox intersect(BoundingBox first, BoundingBox second) {
    return new BoundingBox(
            Math.max(first.minLon, second.minLon),
            Math.min(first.maxLon, second.maxLon),
            Math.max(first.minLat, second.minLat),
            Math.min(first.maxLat, second.maxLat)
    );
  }

  /**
   * calculates the union of two bounding boxes
   *
   * @param first the first bounding box
   * @param second the second bounding box
   * @return the union of the two bboxes
   */
  public static BoundingBox union(BoundingBox first, BoundingBox second) {
    return new BoundingBox(
            Math.min(first.minLon, second.minLon),
            Math.max(first.maxLon, second.maxLon),
            Math.min(first.minLat, second.minLat),
            Math.max(first.maxLat, second.maxLat)
    );
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 29 * hash + (int) (Double.doubleToLongBits(this.minLon) ^ (Double.doubleToLongBits(this.minLon) >>> 32));
    hash = 29 * hash + (int) (Double.doubleToLongBits(this.maxLon) ^ (Double.doubleToLongBits(this.maxLon) >>> 32));
    hash = 29 * hash + (int) (Double.doubleToLongBits(this.minLat) ^ (Double.doubleToLongBits(this.minLat) >>> 32));
    hash = 29 * hash + (int) (Double.doubleToLongBits(this.maxLat) ^ (Double.doubleToLongBits(this.maxLat) >>> 32));
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final BoundingBox other = (BoundingBox) obj;
    if (Double.doubleToLongBits(this.minLon) != Double.doubleToLongBits(other.minLon)) {
      return false;
    }
    if (Double.doubleToLongBits(this.maxLon) != Double.doubleToLongBits(other.maxLon)) {
      return false;
    }
    if (Double.doubleToLongBits(this.minLat) != Double.doubleToLongBits(other.minLat)) {
      return false;
    }
    return Double.doubleToLongBits(this.maxLat) == Double.doubleToLongBits(other.maxLat);
  }

}
