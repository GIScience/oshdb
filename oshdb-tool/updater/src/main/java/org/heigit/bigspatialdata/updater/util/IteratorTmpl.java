package org.heigit.bigspatialdata.updater.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A template for resued iterator-code.
 *
 * @param <T> The type this iterator provides
 */
public abstract class IteratorTmpl<T> implements Iterator<T> {

  protected Exception ex;
  protected T next;

  /**
   * Get a possible exeption thrown during the iteration-process.
   *
   * @return the exception
   */
  public Exception getException() {
    return ex;
  }

  /**
   * Check if the process threw an exception.
   *
   * @return true if an exception occured
   */
  public boolean hasException() {
    return ex != null;
  }

  @Override
  public boolean hasNext() {
    try {
      return next != null || (next = getNext()) != null;
    } catch (Exception e) {
      this.ex = e;
    }
    return false;
  }

  @Override
  public T next() {
    if (!hasNext()) {
      if (ex != null) {
        throw new RuntimeException(ex);
      }
      throw new NoSuchElementException();
    }
    T ret = next;
    next = null;
    return ret;
  }

  /**
   * A wrapper around hasNext -> next.
   *
   * @return null if no next element extists, the element otherwise
   * @throws Exception May throw excpetions
   */
  protected abstract T getNext() throws Exception;

}
