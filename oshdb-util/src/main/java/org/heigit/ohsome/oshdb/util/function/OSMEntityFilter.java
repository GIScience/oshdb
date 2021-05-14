package org.heigit.ohsome.oshdb.util.function;

import org.heigit.ohsome.oshdb.osm.OSMEntity;

/**
 * A serializable {@link java.util.function.Predicate} on {@link OSMEntity} objects.
 */
public interface OSMEntityFilter extends SerializablePredicate<OSMEntity> {}
