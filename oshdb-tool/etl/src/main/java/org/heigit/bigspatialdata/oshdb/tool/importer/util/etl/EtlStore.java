package org.heigit.bigspatialdata.oshdb.tool.importer.util.etl;

import java.util.Map;
import java.util.Set;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.CellId;

/**
 * During the ETL procedure of creating an OSHDB the planet-pbf-file has to be
 * paresed multiple times and a complete, sorted and indexed list of OSH-Objects
 * is created. This class mimics a wrapper around the output.
 */
public interface EtlStore {

	/**
	 * Reads the latest version of an OSHEntity from an ETL-Store. Entities can be
	 * read fast by their ID.
	 *
	 * @param type Type of Entity
	 * @param id   ID of the Entity
	 * @return EtlStoreContainer containing the current version of the requested
	 *         entity and metadata
	 */
	EtlStoreContainer getEntity(OSMType type, long id);

	/**
	 * Reads dependent entities of the provided entity.
	 *
	 * @param container Container with metadata for retrieving dependent
	 * @return A Map that contains all dependent elements (ways depending on the
	 *         node etc.)
	 */
	Map<OSMType, Set<EtlStoreContainer>> getDependent(EtlStoreContainer container);

	/**
	 * Writes a new version of an OSHEntity to the ETL-Store.
	 * 
	 * @param container previous ETL-Store container, could be NULL
	 * @param entity The new or updated entity to be written.
	 * @return EtlStoreContainer containing updated entity and metadata
	 */
	EtlStoreContainer appendEntity(EtlStoreContainer container, OSHEntity entity);
	
	

	/**
	 * Updates the CellId an Object is currently located in.
	 *
	 * @param type  the type of the entity to update
	 * @param id    the id of the entity to update
	 * @param newId the new CellId of the entity
	 */
	void writeCurrentCellId(OSMType type, long id, CellId newId);

	/**
	 * Updates all dependent of an OSHEntity to the ETL-Store.
	 * 
	 * @param container      Container with metadata for updating dependent
	 * @param newMemberNodes New member nodes of the entity (if any, null
	 *                       otherwise).
	 * @param newMemberWays  New member ways of the entity (if any, null otherwise).
	 */
	void updateBackRefs(EtlStoreContainer container, Set<Long> newMemberNodes, Set<Long> newMemberWays);

	CellId getCurrentCellId(OSMType type, long id);

}
