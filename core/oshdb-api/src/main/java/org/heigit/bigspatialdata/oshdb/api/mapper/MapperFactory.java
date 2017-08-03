package org.heigit.bigspatialdata.oshdb.api.mapper;

/**
 * Base class of concrete Mapper factories like OSMEntitySnapshotMapper.
 * Used to let external code accept arbitrary mappers.
 *
 * Any class derived from this base class <b>must</b> define a static method
 * called "using" with the following structure:
 *
 * {@code
 *   public static Mapper<R> using(OSHDB oshdb) {…}
 * }
 *
 * where R is the class of the objects that are passed though to the reduce
 * function in the particular mapper implementation (e.g. OSMEntitySnapshot)
 */
public abstract class MapperFactory {
}
