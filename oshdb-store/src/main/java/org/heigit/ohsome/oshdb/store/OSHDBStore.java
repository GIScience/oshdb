package org.heigit.ohsome.oshdb.store;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.CellId;

public interface OSHDBStore extends AutoCloseable {

      OSHData entity(OSMType type, long id);

      Map<Long, OSHData> entities(OSMType type, Collection<Long> ids);

      void entities(Set<OSHData> entities);

      List<OSHData> grid(CellId gridId);

      BackRef backRef(OSMType type, long id);

      Map<Long, BackRef> backRefs(OSMType type, Collection<Long> ids);

      void backRefs(Set<BackRef> backRefs);
}
