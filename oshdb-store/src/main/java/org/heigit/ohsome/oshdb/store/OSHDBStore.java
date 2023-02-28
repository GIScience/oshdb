package org.heigit.ohsome.oshdb.store;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.CellId;

public interface OSHDBStore extends AutoCloseable {

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

  void backRefs(Set<BackRef> backRefs);
}
