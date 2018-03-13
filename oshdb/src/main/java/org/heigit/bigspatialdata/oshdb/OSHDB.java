package org.heigit.bigspatialdata.oshdb;

public abstract class OSHDB {

  public static final int MAXZOOM = 15;

  // osm only stores 7 decimals for each coordinate
  public static final long GEOM_PRECISION_TO_LONG = 10000000L;
  public static final double GEOM_PRECISION = 1.0 / GEOM_PRECISION_TO_LONG;

  /**
   * Returns various metadata properties of this OSHDB instance
   *
   * For example, metadata("extract.region") returns the geographic region for which the current
   * oshdb extract has been generated in GeoJSON format.
   *
   * @param property the metadata property to request
   * @return the value of the requested metadata field
   */
  public abstract String metadata(String property);
}
