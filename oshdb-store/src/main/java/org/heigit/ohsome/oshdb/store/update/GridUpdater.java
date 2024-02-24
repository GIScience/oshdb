package org.heigit.ohsome.oshdb.store.update;

import org.heigit.ohsome.oshdb.grid.GridOSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.CellId;

@FunctionalInterface
public interface GridUpdater {

  void update(OSMType type, CellId cellId, GridOSHEntity grid);

}
