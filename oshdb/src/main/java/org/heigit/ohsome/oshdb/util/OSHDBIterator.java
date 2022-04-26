package org.heigit.ohsome.oshdb.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class OSHDBIterator<T> implements Iterator<T> {

  public static <T> OSHDBIterator<T> peeking(Iterable<T> iterable) {
    return peeking(iterable.iterator());
  }

  public static <T> OSHDBIterator<T> peeking(Iterator<T> iterator) {
    if (iterator instanceof OSHDBIterator) {
      return (OSHDBIterator<T>) iterator;
    }
    return new IteratorWrapper<>(iterator);
  }

  private T next;

  public OSHDBIterator() {}

  public OSHDBIterator(T first) {
    this.next = first;
  }

  @Override
  public boolean hasNext() {
    return next != null || (next = getNext()) != null;
  }

  @Override
  public T next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    var ret = next;
    next = null;
    return ret;
  }

  public T peek() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return next;
  }

  protected T endOfData() {
    return null;
  }

  protected abstract T getNext();

  private static class IteratorWrapper<T> extends OSHDBIterator<T> {
    private final Iterator<? extends T> iter;

    public IteratorWrapper(Iterator<? extends T> iterator) {
      this.iter = iterator;
    }

    @Override
    protected T getNext() {
      if (iter.hasNext()) {
        return iter.next();
      }
      return endOfData();
    }
  }
}
