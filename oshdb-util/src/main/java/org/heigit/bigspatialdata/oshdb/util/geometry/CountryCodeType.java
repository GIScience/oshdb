package org.heigit.bigspatialdata.oshdb.util.geometry;

/**
 * Defines the type of input for the Country class.
 */
public enum CountryCodeType {

  /**
   * Two letter encoding of countries as defined in
   * <a href="https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2">ISO 3166-1
   * alpha-2</a>.
   */
  ISO_A2,
  /**
   * Name of the sovereign country. This will include all dependencies. E.g.
   * Coral Sea Islands will have no result but be part of Australia.
   */
  SOVEREIGNT,
  /**
   * Name of the geographical unit. This will return separate results for Coral
   * Sea Islands and Australia.
   */
  GEOUNIT;
}
