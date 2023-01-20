package org.heigit.ohsome.oshdb.store;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.heigit.ohsome.oshdb.osm.OSMType;

public abstract class OSHDBStore implements AutoCloseable {

  public abstract Map<Long, OSHDBData> entities(OSMType type, List<Long> ids);

  public abstract void entities(List<OSHDBData> entities);

  public abstract List<OSHDBData> entitiesByGrid(OSMType type, long gridId);

  public abstract Map<Long, BackRefs> backRefs(OSMType type, Collection<Long> ids);

}

