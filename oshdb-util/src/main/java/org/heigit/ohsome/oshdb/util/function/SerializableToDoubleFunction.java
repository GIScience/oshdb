package org.heigit.ohsome.oshdb.util.function;

import java.io.Serializable;
import java.util.function.ToDoubleFunction;

/**
 * A serializable {@link ToDoubleFunction}.
 */
public interface SerializableToDoubleFunction<T> extends ToDoubleFunction<T>, Serializable {}
