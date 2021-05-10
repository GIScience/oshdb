package org.heigit.ohsome.oshdb.util.function;

import org.heigit.ohsome.oshdb.osh.OSHEntity;

/**
 * A serializable {@link java.util.function.Predicate} on {@link OSHEntity} objects.
 */
public interface OSHEntityFilter extends SerializablePredicate<OSHEntity> {}
