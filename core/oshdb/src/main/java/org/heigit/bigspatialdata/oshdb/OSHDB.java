package org.heigit.bigspatialdata.oshdb;

public abstract class OSHDB {

  public static final int MAXZOOM = 12;
  public static final long GEOM_PRECISION_TO_LONG = 10000000L;
  public static final double GEOM_PRECISION = 1.0E-7; // osm only support 7 decimals
}
