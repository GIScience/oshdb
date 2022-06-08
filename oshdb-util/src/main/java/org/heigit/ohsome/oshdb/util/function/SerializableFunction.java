package org.heigit.ohsome.oshdb.util.function;

import java.io.Serializable;
import java.util.function.Function;

/**
 * A serializable {@link Function}.
 */
public interface SerializableFunction<T, R> extends Function<T, R>, Serializable {

  default <V> SerializableFunction<T, V> andThen(SerializableFunction<? super R, ? extends V> after) {
    return (T t) -> after.apply(apply(t));
  }

}
