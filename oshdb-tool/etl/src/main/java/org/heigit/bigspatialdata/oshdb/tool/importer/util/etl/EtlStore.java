package org.heigit.bigspatialdata.oshdb.tool.importer.util.etl;

import java.util.Map;
import java.util.Set;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.CellId;

/**
 * During the ETL procedure of creating an OSHDB the planet-pbf-file has to be paresed multiple
 * times and a complete, sorted and indexed list of OSH-Objects is created. This class mimics a
 * wrapper around the output.
 */
public interface EtlStore {

  /**
   * Reads the latest version of an OSHEntity from an ETL-Store. Entities can be read fast by their
   * ID.
   *
   * @param type Type of Entity
   * @param id ID of the Entity
   * @return the current version of the requested entity
   */
  OSHEntity getEntity(OSMType type, long id);

  /**
   * Reads dependend entities of the provided entity.
   *
   * @param type Type of Entity
   * @param id ID of the Entity
   * @return A Map that contains the all dependend elements (ways depending on the node etc.)
   */
  Map<OSMType, Map<Long, OSHEntity>> getDependent(OSMType type, long id);

  /**
   * Writes a new version of an OSHEntity to the ETL-Store.
   *
   * @param entity The new or updated entity to be written.
   * @param newMemberNodes New member nodes of the entity (if any, null otherwise).
   * @param newMemberWays New memeber ways of the entity (if any, null otherwise).
   */
  void appendEntity(
      OSHEntity entity,
      Set<Long> newMemberNodes,
      Set<Long> newMemberWays
  );

  /**
   * Retrieves the CellId an Object is currently located in.
   *
   * @param type
   * @param id
   * @return The cellId the object currently resides in
   */
  CellId getCurrentCellId(OSMType type, long id);

  /**
   * Updates the CellId an Object is currently located in.
   *
   * @param type
   * @param id
   * @param newId
   */
  void writeCurrentCellId(OSMType type, long id, CellId newId);

}
