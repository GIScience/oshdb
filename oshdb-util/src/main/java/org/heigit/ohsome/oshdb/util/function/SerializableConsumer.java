package org.heigit.ohsome.oshdb.util.function;

import java.io.Serializable;
import java.util.function.Consumer;

/**
 * A serializable {@link Consumer}.
 */
public interface SerializableConsumer<T> extends Consumer<T>, Serializable {}
