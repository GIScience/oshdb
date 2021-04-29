package org.heigit.ohsome.oshdb.osh;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.osm.OSMEntity;

public final class OSHEntities {
  private OSHEntities() {
    throw new IllegalStateException("utility class");
  }

  /**
   * Collects all versions of an OSH entity from an iterable ({@link OSHEntity#getVersions()})
   * into a list.
   *
   * @param versions the versions of an OSH entity, as returned from {@link OSHEntity#getVersions()}
   * @param <T> the type of the OSM entities: {@link org.heigit.ohsome.oshdb.osm.OSMNode},
   *            {@link org.heigit.ohsome.oshdb.osm.OSMWay} or
   *            {@link org.heigit.ohsome.oshdb.osm.OSMRelation}
   * @return all versions of the OSH entity as a list, with the most recent version first.
   */
  public static <T extends OSMEntity> List<T> toList(Iterable<T> versions) {
    return toList(versions, osm -> osm);
  }

  /**
   * Collects all versions of an OSH entity from an iterable ({@link OSHEntity#getVersions()})
   * into a list after applying a transformation function.
   *
   * @param versions the versions of an OSH entity, as returned from {@link OSHEntity#getVersions()}
   * @param transformer a function which is called for each version
   * @param <T> the type of the OSM entities: {@link org.heigit.ohsome.oshdb.osm.OSMNode},
   *            {@link org.heigit.ohsome.oshdb.osm.OSMWay} or
   *            {@link org.heigit.ohsome.oshdb.osm.OSMRelation}
   * @param <R> the type of the returned list's items
   * @return all versions of the OSH entity as a list, with the most recent version first.
   */
  public static <T extends OSMEntity, R> List<R> toList(
      Iterable<T> versions, Function<T, R> transformer) {
    final Iterator<T> itr = versions.iterator();
    if (!itr.hasNext()) {
      return Collections.emptyList();
    }
    final T last = itr.next();
    List<R> list = new ArrayList<>(last.getVersion());
    list.add(transformer.apply(last));
    while (itr.hasNext()) {
      list.add(transformer.apply(itr.next()));
    }
    return list;
  }

  /**
   * Returns the OSM entity ("version") of the OSH entity which was current at the given timestamp.
   *
   * @param osh the OSH entity to process.
   * @param timestamp the timestamp for which to return the state of the OSH entity.
   * @return the version (OSM entity) of the OSH entity which was current at the given timestamp or
   *         {@code null} if none could be found.
   */
  public static OSMEntity getByTimestamp(OSHEntity osh, OSHDBTimestamp timestamp) {
    for (OSMEntity osm : osh.getVersions()) {
      if (osm.getEpochSecond() <= timestamp.getEpochSecond()) {
        return osm;
      }
    }
    return null;
  }
}
