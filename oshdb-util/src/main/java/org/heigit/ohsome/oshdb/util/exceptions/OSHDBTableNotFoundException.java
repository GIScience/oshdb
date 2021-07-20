package org.heigit.ohsome.oshdb.util.exceptions;

/**
 * An exception caused by missing tables or caches.
 */
public class OSHDBTableNotFoundException extends OSHDBException {
  public OSHDBTableNotFoundException(String missingTable) {
    super("Database doesn't contain the required table(s)/cache(s): " + missingTable + ".");
  }
}
