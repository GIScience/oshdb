package org.heigit.ohsome.oshdb.util.exceptions;

public class OSHDBKeytablesNotFoundException extends Exception {

  /**
   * Exception used if the OSHDB keytable cannot be found.
   */
  public OSHDBKeytablesNotFoundException() {
    super("Keytables database not found, or db doesn't contain the required \"keytables\" tables. "
        + "Make sure you have specified the right keytables database, for example by calling "
        + "`keytables()` when using the OSHDB-API.");
  }
}
