package org.heigit.ohsome.oshdb.util.function;

import java.io.Serializable;
import java.util.function.Predicate;

/**
 * A serializable {@link Predicate}.
 */
public interface SerializablePredicate<T> extends Predicate<T>, Serializable {}
