package org.heigit.ohsome.oshdb.util.function;

import java.io.Serializable;
import java.util.function.Supplier;

/**
 * A serializable {@link Supplier}.
 */
public interface SerializableSupplier<R> extends Supplier<R>, Serializable {}
