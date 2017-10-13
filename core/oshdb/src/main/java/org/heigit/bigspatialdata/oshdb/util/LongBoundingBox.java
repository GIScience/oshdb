package org.heigit.bigspatialdata.oshdb.util;

public class LongBoundingBox {

  
  public final long minLon;
  public final long maxLon;
  public final long minLat;
  public final long maxLat;
 
  
  public LongBoundingBox(long minLon, long maxLon, long minLat, long maxLat){
    this.minLon = minLon;
    this.maxLon = maxLon;
    this.minLat = minLat;
    this.maxLat = maxLat;
  }
  
  public LongBoundingBox(long[] lon, long[] lat) {
    this(lon[0],lon[1],lat[0],lat[1]);   
  } 
  
  public long[] getLon(){
    return new long[]{minLon,maxLon};
  }
  
  public long[] getLat(){
    return new long[]{minLat,maxLat};
  }
  
  public enum OVERLAP {
    NONE,
    OVERLAP,
    A_COMPLETE_IN_B,
    B_COMPLETE_IN_A
  }
  
  public static OVERLAP overlap(LongBoundingBox a, LongBoundingBox b) {
    if (b.minLon >= a.maxLon ||
        b.maxLon <= a.minLon ||
        b.minLat >= a.maxLat ||
        b.maxLat <= a.minLat)
      return OVERLAP.OVERLAP.NONE; // no overlap

    // fit bbox in test
    if (a.minLon >= b.minLon && a.maxLon <= b.maxLon && a.minLat >= b.minLat
        && a.maxLat <= b.maxLat)
      return OVERLAP.A_COMPLETE_IN_B;
    // fit test in bbox
    if (b.minLon >= a.minLon && b.maxLon <= a.maxLon && b.minLat >= a.minLat
        && b.maxLat <= a.maxLat)
      return OVERLAP.OVERLAP.B_COMPLETE_IN_A;
    return OVERLAP.OVERLAP.OVERLAP;
  }
  
  @Override
  public String toString() {
    return String.format("(%d,%d)(%d,%d)", minLon,minLat,maxLon,maxLat);
  }
  
}
