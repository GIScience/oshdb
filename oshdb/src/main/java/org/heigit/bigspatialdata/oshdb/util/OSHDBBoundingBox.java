package org.heigit.bigspatialdata.oshdb.util;

import java.io.Serializable;
import java.util.Locale;

import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OSHDBBoundingBox implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(OSHDBBoundingBox.class);

  
  public static final OSHDBBoundingBox EMPTY = new OSHDBBoundingBox(0L, 0L, 0L, 0L);


  /**
   * calculates the intersection of two bounding boxes
   *
   * @param first the first bounding box
   * @param second the second bounding box
   * @return the intersection of the two bboxes
   */
  public static OSHDBBoundingBox intersect(OSHDBBoundingBox first, OSHDBBoundingBox second) {
    return new OSHDBBoundingBox(
        Math.max(first.minLon, second.minLon),
        Math.max(first.minLat, second.minLat),
        Math.min(first.maxLon, second.maxLon),
        Math.min(first.maxLat, second.maxLat)
    );
  }

  public static OVERLAP overlap(OSHDBBoundingBox a, OSHDBBoundingBox b) {
    if (b.minLon >= a.maxLon
        || b.maxLon <= a.minLon
        || b.minLat >= a.maxLat
        || b.maxLat <= a.minLat) {
      return OVERLAP.NONE; // no overlap
    }
    // fit bbox in test
    if (a.minLon >= b.minLon && a.maxLon <= b.maxLon && a.minLat >= b.minLat
            && a.maxLat <= b.maxLat) {
      return OVERLAP.A_COMPLETE_IN_B;
    }
    // fit test in bbox
    if (b.minLon >= a.minLon && b.maxLon <= a.maxLon && b.minLat >= a.minLat
            && b.maxLat <= a.maxLat) {
      return OVERLAP.B_COMPLETE_IN_A;
    }
    return OVERLAP.OVERLAPPING;
  }

  public long minLon;
  public long maxLon;
  public long minLat;
  public long maxLat;

  public OSHDBBoundingBox(long minLon, long minLat, long maxLon, long maxLat) {
    this.minLon = minLon;
    this.maxLon = maxLon;
    this.minLat = minLat;
    this.maxLat = maxLat;
  }
  
  public void set(long minLon, long minLat, long maxLon, long maxLat) {
    this.minLon = minLon;
    this.maxLon = maxLon;
    this.minLat = minLat;
    this.maxLat = maxLat;
  }
  
  public OSHDBBoundingBox(double minLon, double minLat, double maxLon, double maxLat) {
    this.minLon = (long) (minLon * OSHDB.GEOM_PRECISION_TO_LONG);
    this.maxLon = (long) (maxLon * OSHDB.GEOM_PRECISION_TO_LONG);
    this.minLat = (long) (minLat * OSHDB.GEOM_PRECISION_TO_LONG);
    this.maxLat = (long) (maxLat * OSHDB.GEOM_PRECISION_TO_LONG);
  }

  public OSHDBBoundingBox(int minLon, int minLat, int maxLon, int maxLat) {
    this((double) minLon, (double) minLat, (double) maxLon, (double) maxLat);
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
    return String.format(Locale.ENGLISH, "(%f,%f) (%f,%f)", this.getMinLon(), this.getMinLat(), this.getMaxLon(), this.getMaxLat());
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
    final OSHDBBoundingBox other = (OSHDBBoundingBox) obj;
    if (this.minLon != other.minLon) {
      return false;
    }
    if (this.maxLon != other.maxLon) {
      return false;
    }
    if (this.minLat != other.minLat) {
      return false;
    }
    return this.maxLat == other.maxLat;
  }

  public enum OVERLAP {
    NONE,
    OVERLAPPING,
    A_COMPLETE_IN_B,
    B_COMPLETE_IN_A
  }
}
