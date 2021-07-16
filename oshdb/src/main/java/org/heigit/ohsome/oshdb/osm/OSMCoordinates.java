package org.heigit.ohsome.oshdb.osm;

/**
 * Helper class for converting double precision floating point lon/lat to
 * osm-coordinate fix presision 7 decimal integer system.
 *
 */
public class OSMCoordinates {

  // osm only stores 7 decimals for each coordinate
  public static final double GEOM_PRECISION_TO_LONG = 1E7;
  public static final double GEOM_PRECISION = 1.0 / GEOM_PRECISION_TO_LONG;

  public static int toOSM(double value) {
    return (int) (value * GEOM_PRECISION_TO_LONG);
  }

  public static double toDouble(int value) {
    return value * GEOM_PRECISION;
  }

  public static double toDouble(long value) {
    return value * GEOM_PRECISION;
  }

  public static double toDouble(double value) {
    return value * GEOM_PRECISION;
  }

  public static boolean validLon(int lon) {
    return Math.abs(lon) < 180_0000000;
  }

  public static boolean validLat(int lat) {
    return Math.abs(lat) < 90_0000000;
  }

  private OSMCoordinates() {}
}
