package org.heigit.bigspatialdata.oshdb.util.celliterator;

import java.util.function.Supplier;

/**
 * A lazily evaluated object.
 * 
 * <p>Useful as a wrapper to hold values which are potentially expensive to
 * calculate, but might sometimes not be requested.</p>
 *
 * @param <T> the (arbitrary) type of data to hold
 */
public class LazyEvaluatedObject<T> implements Supplier<T> {
  private T value = null;
  private boolean evaluated = false;
  private Supplier<T> evaluator;

  /**
   * Constructs a {@link LazyEvaluatedObject} using a {@code evaluator} (supplier function) which
   * returns a value of generic type {@code T}, when requested from this object.
   *
   * @param evaluator a {@link Supplier} function which returns the value of interest when executed
   */
  public LazyEvaluatedObject(Supplier<T> evaluator) {
    this.evaluator = evaluator;
  }

  /**
   * Constructs a {@link LazyEvaluatedObject} using an already known {@code value} of generic
   * type {@code T}.
   *
   * <p>This simply wraps the value with a supplier method, but can be useful in situations where
   * sometimes an expensive to calculate value is already know, but one wants to use the lazy
   * evaluated interface for the remaining cases.</p>
   *
   * @param value the value to store
   */
  public LazyEvaluatedObject(T value) {
    this.value = value;
    this.evaluated = true;
    this.evaluator = null;
  }

  @Override
  public T get() {
    if (!this.evaluated) {
      this.value = this.evaluator.get();
      this.evaluated = true;
      this.evaluator = null;
    }
    return this.value;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof LazyEvaluatedObject) {
      LazyEvaluatedObject<?> lazyO = (LazyEvaluatedObject<?>)o;
      return this.get().equals(lazyO.get());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return this.get().hashCode();
  }

  public boolean wasEvaluated() {
    return this.evaluated;
  }
}
