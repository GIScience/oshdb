package org.heigit.ohsome.oshdb.util.function;

import java.io.Serializable;
import java.util.function.Function;

/**
 * A serializable {@link Function}.
 */
public interface SerializableFunction<T, R> extends Function<T, R>, Serializable {}
