package org.heigit.bigspatialdata.oshdb.util;

import java.util.Locale;

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

  @Override
  public String toString() {

    return String.format(Locale.ENGLISH,"(%f,%f) (%f,%f)", minLon, minLat, maxLon, maxLat);
  }

}
