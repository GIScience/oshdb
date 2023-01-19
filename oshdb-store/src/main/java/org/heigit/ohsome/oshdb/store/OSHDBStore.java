package org.heigit.ohsome.oshdb.store;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.CellId;

public abstract class OSHDBStore implements AutoCloseable {

  public abstract Map<Long, OSHDBData> entities(OSMType type, List<Long> ids);

  public abstract void entities(List<OSHDBData> entities);

  public abstract Map<Long, List<OSHDBData>> entitiesByGrid(OSMType type, Collection<Long> gridIds);

  public abstract Map<CellId, List<OSHDBData>> grids(OSMType type, Collection<CellId> cellIds);

  public abstract Map<Long, BackRefs> backRefs(OSMType type, Collection<Long> ids);



}

