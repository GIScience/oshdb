package org.heigit.ohsome.oshdb.store;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.source.ReplicationInfo;
import org.heigit.ohsome.oshdb.util.CellId;
import org.heigit.ohsome.oshdb.util.tagtranslator.TagTranslator;

public interface OSHDBStore extends AutoCloseable {

  /**
   * Get current Replication Info from store.
   * @return
   */
  ReplicationInfo state();

  /**
   * Update Replication Info
   * @param state new Replication Info
   */
  void state(ReplicationInfo state);

  TagTranslator getTagTranslator();

  default OSHData entity(OSMType type, long id) {
    return entities(type, Set.of(id)).get(id);
  }

  Map<Long, OSHData> entities(OSMType type, Set<Long> ids);

  void entities(Set<OSHData> entities);

  List<OSHData> grid(OSMType type, CellId cellId);

  default BackRef backRef(OSMType type, long id) {
    return backRefs(type, Set.of(id)).get(id);
  }

  Map<Long, BackRef> backRefs(OSMType type, Set<Long> ids);

  void backRefsMerge(BackRefType type, long backRef, Set<Long> ids);
}
