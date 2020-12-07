package org.heigit.ohsome.oshdb.util.exceptions;

public class OSHDBTableNotFoundException extends RuntimeException {
  public OSHDBTableNotFoundException(String missingTable) {
    super("Database doesn't contain the required table(s)/cache(s): " + missingTable + ".");
  }
}
