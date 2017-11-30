package org.heigit.bigspatialdata.oshdb.util;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Polygon;
import java.io.Serializable;
import java.util.Locale;
import org.geotools.geometry.jts.JTS;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BoundingBox implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(BoundingBox.class);

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

  public static OVERLAP overlap(BoundingBox a, BoundingBox b) {
    if (b.minLon >= a.maxLon
            || b.maxLon <= a.minLon
            || b.minLat >= a.maxLat
            || b.maxLat <= a.minLat) {
      return OVERLAP.OVERLAP.NONE; // no overlap
    }
    // fit bbox in test
    if (a.minLon >= b.minLon && a.maxLon <= b.maxLon && a.minLat >= b.minLat
            && a.maxLat <= b.maxLat) {
      return OVERLAP.A_COMPLETE_IN_B;
    }
    // fit test in bbox
    if (b.minLon >= a.minLon && b.maxLon <= a.maxLon && b.minLat >= a.minLat
            && b.maxLat <= a.maxLat) {
      return OVERLAP.OVERLAP.B_COMPLETE_IN_A;
    }
    return OVERLAP.OVERLAP.OVERLAP;
  }

  public final long minLon;
  public final long maxLon;
  public final long minLat;
  public final long maxLat;

  public BoundingBox(long minLon, long maxLon, long minLat, long maxLat) {
    this.minLon = minLon;
    this.maxLon = maxLon;
    this.minLat = minLat;
    this.maxLat = maxLat;
  }

  public BoundingBox(double minLon, double maxLon, double minLat, double maxLat) {
    this.minLon = (long) (minLon * OSHDB.GEOM_PRECISION_TO_LONG);
    this.maxLon = (long) (maxLon * OSHDB.GEOM_PRECISION_TO_LONG);
    this.minLat = (long) (minLat * OSHDB.GEOM_PRECISION_TO_LONG);
    this.maxLat = (long) (maxLat * OSHDB.GEOM_PRECISION_TO_LONG);
  }

  public BoundingBox(int minLon, int maxLon, int minLat, int maxLat) {
    this((double) minLon, (double) maxLon, (double) minLat, (double) maxLat);
  }

  public BoundingBox(Envelope envelope) {
    this(envelope.getMinX(), envelope.getMaxX(), envelope.getMinY(), envelope.getMaxY());
  }

  public BoundingBox(long[] lon, long[] lat) {
    this(lon[0], lon[1], lat[0], lat[1]);
  }

  public double getMinLon() {
    return minLon * OSHDB.GEOM_PRECISION;
  }

  public double getMaxLon() {
    return maxLon * OSHDB.GEOM_PRECISION;
  }

  public double getMinLat() {
    return minLat * OSHDB.GEOM_PRECISION;
  }

  public double getMaxLat() {
    return maxLat * OSHDB.GEOM_PRECISION;
  }

  public long[] getLon() {
    return new long[]{minLon, maxLon};
  }

  public long[] getLat() {
    return new long[]{minLat, maxLat};
  }

  @Override
  public String toString() {
    return String.format(Locale.ENGLISH, "(%f,%f) (%f,%f)", this.getMinLon(), this.getMaxLon(), this.getMinLat(), this.getMaxLat());
  }

  /**
   * returns JTS geometry object for convenience
   *
   * @return com.vividsolutions.jts.geom.Geometry
   */
  public Polygon getGeometry() {
    return JTS.toGeometry(new Envelope(this.getMinLon(), this.getMaxLon(), this.getMinLat(), this.getMaxLat()));
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 79 * hash + (int) (this.minLon ^ (this.minLon >>> 32));
    hash = 79 * hash + (int) (this.maxLon ^ (this.maxLon >>> 32));
    hash = 79 * hash + (int) (this.minLat ^ (this.minLat >>> 32));
    hash = 79 * hash + (int) (this.maxLat ^ (this.maxLat >>> 32));
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
    if (this.minLon != other.minLon) {
      return false;
    }
    if (this.maxLon != other.maxLon) {
      return false;
    }
    if (this.minLat != other.minLat) {
      return false;
    }
    if (this.maxLat != other.maxLat) {
      return false;
    }
    return true;
  }

  public enum OVERLAP {
    NONE,
    OVERLAP,
    A_COMPLETE_IN_B,
    B_COMPLETE_IN_A
  }
}
