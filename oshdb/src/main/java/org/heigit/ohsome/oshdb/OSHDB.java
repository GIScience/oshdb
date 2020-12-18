package org.heigit.ohsome.oshdb;

public abstract class OSHDB {

  public static final int MAXZOOM = 14;

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
    return coordinate / (double) GEOM_PRECISION_TO_LONG;
  }

  /**
   * Returns various metadata properties of this OSHDB instance.
   *
   * <p>For example, metadata("extract.region") returns the geographic region for which the
   * current oshdb extract has been generated in GeoJSON format.</p>
   *
   * @param property the metadata property to request
   * @return the value of the requested metadata field
   */
  public abstract String metadata(String property);
}
