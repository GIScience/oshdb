package org.heigit.ohsome.oshdb.util.exceptions;

/**
 * An exception which is thrown when a particular feature is not implemented in the OSHDB.
 *
 * <p>
 *   Mostly used internally for specific code paths for which fallback routines are implemented.
 * </p>
 */
public class OSHDBNotImplementedException extends OSHDBException {
  public OSHDBNotImplementedException(String message) {
    super(message);
  }
}
