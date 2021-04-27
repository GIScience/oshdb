package org.heigit.ohsome.oshdb;

import java.io.Serializable;
import java.util.Locale;

public class OSHDBBoundingBox implements OSHDBBoundable, Serializable {
  private static final long serialVersionUID = 1L;
  public static final OSHDBBoundingBox INVALID = new OSHDBBoundingBox(1L, 1L, -1L, -1L);

  public static OVERLAP overlap(OSHDBBoundingBox a, OSHDBBoundingBox b) {
    if (b.minLon >= a.maxLon
        || b.maxLon <= a.minLon
        || b.minLat >= a.maxLat
        || b.maxLat <= a.minLat) {
      return OVERLAP.NONE; // no overlap
    }
    // fit bbox in test
    if (a.minLon >= b.minLon
        && a.maxLon <= b.maxLon
        && a.minLat >= b.minLat
        && a.maxLat <= b.maxLat) {
      return OVERLAP.A_COMPLETE_IN_B;
    }
    // fit test in bbox
    if (b.minLon >= a.minLon
        && b.maxLon <= a.maxLon
        && b.minLat >= a.minLat
        && b.maxLat <= a.maxLat) {
      return OVERLAP.B_COMPLETE_IN_A;
    }
    return OVERLAP.OVERLAPPING;
  }

  private final long minLon;
  private final long maxLon;
  private final long minLat;
  private final long maxLat;

  public OSHDBBoundingBox(long minLon, long minLat, long maxLon, long maxLat) {
    this.minLon = minLon;
    this.maxLon = maxLon;
    this.minLat = minLat;
    this.maxLat = maxLat;
  }

  public OSHDBBoundingBox(double minLon, double minLat, double maxLon, double maxLat) {
    this.minLon = Math.round(minLon * OSHDB.GEOM_PRECISION_TO_LONG);
    this.maxLon = Math.round(maxLon * OSHDB.GEOM_PRECISION_TO_LONG);
    this.minLat = Math.round(minLat * OSHDB.GEOM_PRECISION_TO_LONG);
    this.maxLat = Math.round(maxLat * OSHDB.GEOM_PRECISION_TO_LONG);
  }

  public OSHDBBoundingBox(int minLon, int minLat, int maxLon, int maxLat) {
    this((double) minLon, (double) minLat, (double) maxLon, (double) maxLat);
  }

  @Override
  public long getMinLonLong() {
    return minLon;
  }

  @Override
  public long getMaxLonLong() {
    return maxLon;
  }

  @Override
  public long getMinLatLong() {
    return minLat;
  }

  @Override
  public long getMaxLatLong() {
    return maxLat;
  }

  public long[] getLon() {
    return new long[] {minLon, maxLon};
  }

  public long[] getLat() {
    return new long[] {minLat, maxLat};
  }

  @Override
  public String toString() {
    return String.format(Locale.ENGLISH,
        "(%3.7f,%3.7f,%3.7f,%3.7f)",
        this.getMinLon(),
        this.getMinLat(),
        this.getMaxLon(),
        this.getMaxLat());
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
