package org.heigit.ohsome.oshdb.util.exceptions;

/**
 * Exception used if the OSHDB keytable cannot be found.
 */
public class OSHDBKeytablesNotFoundException extends Exception {
  /**
   * Creates an exception with a message explaining that the OSHDB keytables db was not found.
   */
  public OSHDBKeytablesNotFoundException() {
    super("Keytables database not found, or db doesn't contain the required \"keytables\" tables. "
        + "Make sure you have specified the right keytables database, for example by calling "
        + "`keytables()` when using the OSHDB-API.");
  }
}
