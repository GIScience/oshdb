package org.heigit.ohsome.oshdb;

public abstract class OSHDB {

  public static final int MAXZOOM = 14;

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
