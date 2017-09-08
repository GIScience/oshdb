package org.heigit.bigspatialdata.oshdb.api.mapreducer;

/**
 * Base class of concrete MapReducer factories like OSMEntitySnapshotView.
 * Used to let external code accept arbitrary mappers.
 *
 * Any class derived from this base class <b>must</b> define a static method
 * called "on" with the following structure:
 *
 * {@code
 *   public static MapReducer<R> on(OSHDB oshdb) {…}
 * }
 *
 * where R is the class of the objects that are passed though to the reduce
 * function in the particular mapper implementation (e.g. OSMEntitySnapshot)
 */
public abstract class MapperFactory {
}
