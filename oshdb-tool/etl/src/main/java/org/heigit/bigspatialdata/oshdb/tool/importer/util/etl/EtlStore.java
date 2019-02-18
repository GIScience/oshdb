package org.heigit.bigspatialdata.oshdb.tool.importer.util.etl;

import java.util.Set;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;

/**
 *
 */
public interface EtlStore {

  /**
   *
   * @param type
   * @param id
   * @return
   */
  OSHEntity getEntity(OSMType type, long id);

  /**
   *
   * @param entity
   * @param newMemberNodes
   * @param newMemberWays
   * @param newMemberRelations
   */
  void appendEntity(OSHEntity entity, Set<Long> newMemberNodes, Set<Long> newMemberWays,
      Set<Long> newMemberRelations);

}
