package org.heigit.bigspatialdata.oshdb.util.time;

/**
 * An exception marking a problem with (for the OSHDB) illegal timestamps.
 */
public class OSHDBTimestampIllegalArgumentException extends OSHDBTimestampException {

  public OSHDBTimestampIllegalArgumentException(String message) {
    super(message);
  }
}
