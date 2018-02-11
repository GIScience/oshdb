package org.heigit.bigspatialdata.oshdb.util.celliterator;

import java.util.function.Supplier;

public class LazyEvaluatedObject<T> implements Supplier<T> {
  private T value = null;
  private boolean hasBeenEvaluated = false;
  private Supplier<T> evaluator;

  public LazyEvaluatedObject(Supplier<T> evaluator) {
    this.evaluator = evaluator;
  }

  public LazyEvaluatedObject(T value) {
    this.value = value;
    this.hasBeenEvaluated = true;
    this.evaluator = null;
  }

  @Override
  public T get() {
    if (!this.hasBeenEvaluated) {
      this.value = this.evaluator.get();
      this.hasBeenEvaluated = true;
      this.evaluator = null;
    }
    return this.value;
  }
}
