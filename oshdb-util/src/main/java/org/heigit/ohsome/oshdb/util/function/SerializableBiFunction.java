package org.heigit.ohsome.oshdb.util.function;

import java.io.Serializable;
import java.util.function.BiFunction;

/**
 * A serializable {@link BiFunction}.
 */
public interface SerializableBiFunction<T, U, R> extends BiFunction<T, U, R>, Serializable {

  default <V> SerializableBiFunction<T, U, V> andThen(
      SerializableFunction<? super R, ? extends V> after) {
    return (T t, U u) -> after.apply(apply(t, u));
  }
}
