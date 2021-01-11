package org.heigit.ohsome.oshdb.osh;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.OSHDBTimestamp;

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
    Iterator<T> itr = versions.iterator();
    if (!itr.hasNext()) {
      return Collections.emptyList();
    }
    T last = itr.next();
    List<T> list = new ArrayList<>(last.getVersion());
    list.add(last);
    while (itr.hasNext()) {
      list.add(itr.next());
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
      if (osm.getTimestamp().getRawUnixTimestamp() <= timestamp.getRawUnixTimestamp()) {
        return osm;
      }
    }
    return null;
  }
}
