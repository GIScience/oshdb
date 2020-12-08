package org.heigit.bigspatialdata.oshdb.util.time;

/**
 * An exception representing a problem of handling timestamps in the OSHDB.
 */
public class OSHDBTimestampException extends RuntimeException {

  public OSHDBTimestampException(String message) {
    super(message);
  }
}
