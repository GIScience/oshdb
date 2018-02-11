package org.heigit.bigspatialdata.oshdb.util.celliterator;

import java.util.function.Supplier;

public class LazyEvaluatedObject<T> implements Supplier<T> {
  private T value = null;
  private boolean evaluated = false;
  private Supplier<T> evaluator;

  public LazyEvaluatedObject(Supplier<T> evaluator) {
    this.evaluator = evaluator;
  }

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
      LazyEvaluatedObject lazyO = (LazyEvaluatedObject)o;
      return this.get().equals(lazyO.get());
    }
    return false;
  }
}
