package org.heigit.bigspatialdata.oshdb.util;

import java.io.Serializable;
import java.util.Locale;

import org.heigit.bigspatialdata.oshdb.OSHDB;


public class OSHDBBoundingBox implements Serializable {
  private static final long serialVersionUID = 1L;
  public static final OSHDBBoundingBox EMPTY = new OSHDBBoundingBox(0L, 0L, 0L, 0L);
  public static final OSHDBBoundingBox INVALID = new OSHDBBoundingBox(Long.MAX_VALUE,Long.MAX_VALUE,Long.MIN_VALUE,Long.MIN_VALUE);


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

  private long minLon;
  private long maxLon;
  private long minLat;
  private long maxLat;

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

  public void set(OSHDBBoundingBox bbox) {
   this.minLon = bbox.minLon;
   this.maxLon = bbox.maxLon;
   this.minLat = bbox.minLat;
   this.maxLat = bbox.maxLat;
  }
  
  public void add(OSHDBBoundingBox bbox){
    this.minLon = Math.min(minLon,bbox.minLon);
    this.maxLon = Math.max(maxLon,bbox.maxLon);
    this.minLat = Math.min(minLat,bbox.minLat);
    this.maxLat = Math.max(maxLat,bbox.maxLat);
  }

  public double getMinLon() {
    return minLon * OSHDB.GEOM_PRECISION;
  }
  
  public long getMinLonLong(){
    return minLon;
  }

  public double getMaxLon() {
    return maxLon * OSHDB.GEOM_PRECISION;
  }

  public long getMaxLonLong(){
    return maxLon;
  }
  
  public double getMinLat() {
    return minLat * OSHDB.GEOM_PRECISION;
  }
  
  public long getMinLatLong(){
    return minLat;
  }

  public double getMaxLat() {
    return maxLat * OSHDB.GEOM_PRECISION;
  }
  
  public long getMaxLatLong(){
    return maxLat;
  }

  public long[] getLon() {
    return new long[]{minLon, maxLon};
  }

  public long[] getLat() {
    return new long[]{minLat, maxLat};
  }

  public boolean intersects(OSHDBBoundingBox otherBbox) {
    return (otherBbox != null)
        && (maxLat >= otherBbox.minLat) && (minLat <= otherBbox.maxLat) 
        && (maxLon >= otherBbox.minLon) && (minLon <= otherBbox.maxLon);
  }
  
  public boolean isInside(OSHDBBoundingBox otherBbox) {
    return (otherBbox != null) 
        && (minLat >= otherBbox.minLat) && (maxLat <= otherBbox.maxLat)
        && (minLon >= otherBbox.minLon) && (maxLon <= otherBbox.maxLon);
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

  public boolean isPoint() {
    return minLon == maxLon && minLat == maxLat;
  }
  
  public boolean isValid(){
    return minLon <= maxLon && minLat <= maxLat;
  }

  
}
