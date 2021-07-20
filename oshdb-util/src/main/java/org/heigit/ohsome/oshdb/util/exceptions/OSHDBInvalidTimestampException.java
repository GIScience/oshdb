package org.heigit.ohsome.oshdb.util.exceptions;

/**
 * An exception caused by invalid time string input.
 */
public class OSHDBInvalidTimestampException extends OSHDBException {
  public OSHDBInvalidTimestampException(String message) {
    super(message);
  }
}
