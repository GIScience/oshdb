package org.heigit.ohsome.oshdb.util.exceptions;

/**
 * General OSHDB exception. This exception is used to indicate any error condition while using
 * OSHDB.
 */
public class OSHDBException extends RuntimeException {

  /**
   * Create empty exception.
   */
  public OSHDBException() {
    super();
  }

  /**
   * Creates new exception with given error message.
   *
   * @param message Error message.
   */
  public OSHDBException(String message) {
    super(message);
  }

  /**
   * Creates new grid exception with given throwable as a cause and source of error message.
   *
   * @param cause Non-null throwable cause.
   */
  public OSHDBException(Throwable cause) {
    this(cause.getMessage(), cause);
  }

  /**
   * Creates new exception with given error message and optional nested exception.
   *
   * @param message Error message.
   * @param cause Optional nested exception (can be {@code null}).
   */
  public OSHDBException(String message, Throwable cause) {
    super(message, cause);
  }
}
