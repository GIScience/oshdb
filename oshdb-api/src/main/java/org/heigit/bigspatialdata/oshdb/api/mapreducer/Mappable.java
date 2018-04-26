package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializablePredicate;
import org.jetbrains.annotations.Contract;

public interface Mappable<X> {
  /**
   * Set an arbitrary `map` transformation function.
   *
   * @param mapper function that will be applied to each data entry (osm entity
   * snapshot or contribution)
   * @param <R> an arbitrary data type which is the return type of the
   * transformation `map` function
   * @return a modified copy of the current "Mappable" object operating on the
   * transformed type (&lt;R&gt;)
   */
  @Contract(pure = true)
  <R> Mappable<R> map(SerializableFunction<X, R> mapper);

  /**
   * Set an arbitrary `flatMap` transformation function, which returns list with
   * an arbitrary number of results per input data entry. The results of this
   * function will be "flattened", meaning that they can be for example
   * transformed again by setting additional `map` functions.
   *
   * @param flatMapper function that will be applied to each data entry (osm
   * entity snapshot or contribution) and returns a list of results
   * @param <R> an arbitrary data type which is the return type of the
   * transformation `map` function
   * @return a modified copy of the current "Mappable" object operating on the
   * transformed type (&lt;R&gt;)
   */
  @Contract(pure = true)
  <R> Mappable<R> flatMap(SerializableFunction<X, Iterable<R>> flatMapper);

  /**
   * Adds a custom arbitrary filter that gets executed in the current
   * transformation chain.
   *
   * @param f the filter function that determines if the respective data should
   * be passed on (when f returns true) or discarded (when f returns false)
   * @return a modified copy of this "Mappable" (can be used to chain multiple
   * commands together)
   */
  @Contract(pure = true)
  Mappable<X> filter(SerializablePredicate<X> f);
}
