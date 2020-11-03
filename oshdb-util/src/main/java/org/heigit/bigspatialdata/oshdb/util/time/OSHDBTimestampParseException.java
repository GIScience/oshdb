package org.heigit.bigspatialdata.oshdb.util.time;

/**
 * An exception marking a parsing problem.
 */
public class OSHDBTimestampParseException extends OSHDBTimestampException {

  public OSHDBTimestampParseException(String message) {
    super(message);
  }
}
