package org.heigit.ohsome.oshdb.osm;

public abstract class Coordinates {

  // osm only stores 7 decimals for each coordinate
  public static final long GEOM_PRECISION_TO_LONG = 10000000L;
  public static final double GEOM_PRECISION = 1.0 / GEOM_PRECISION_TO_LONG;
  
  /**
   * Converts a double precision coordinate to a fixed precision long value.
   *
   * @param coordinate double value coordinate
   * @return converted coordinate
   */
  public static long coordinateToLong(double coordinate) {
    return (long) (coordinate * GEOM_PRECISION_TO_LONG);
  }

  /**
   * Returns the double precision coordinate of a fixed precision long value.
   *
   * @param coordinate long value coordinate
   * @return converted coordinate
   */
  public static double coordinateToDouble(long coordinate) {
    return coordinate * GEOM_PRECISION;
  }
  
  private Coordinates() {}
}
