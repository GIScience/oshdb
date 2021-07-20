package org.heigit.ohsome.oshdb.util.exceptions;

/**
 * An exception caused by corrupt keytables data.
 */
public class OSHDBTagOrRoleNotFoundException extends OSHDBException {
  public OSHDBTagOrRoleNotFoundException(String msg) {
    super(msg);
  }
}
