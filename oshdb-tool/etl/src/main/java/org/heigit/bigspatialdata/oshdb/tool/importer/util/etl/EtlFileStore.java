package org.heigit.bigspatialdata.oshdb.tool.importer.util.etl;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An ETL-Store that stores the list of OSH-Entites in a file.
 */
public class EtlFileStore implements EtlStore {

	private static final Logger LOG = LoggerFactory.getLogger(EtlFileStore.class);

	public EtlFileStore(Path etlPath) {
		// TODO Auto-generated constructor stub
	}

	@Override
	public EtlStoreContainer getEntity(OSMType type, long id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<OSMType, Set<EtlStoreContainer>> getDependent(EtlStoreContainer container) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public EtlStoreContainer appendEntity(EtlStoreContainer container, OSHEntity entity) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void writeCurrentCellId(OSMType type, long id, CellId newId) {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBackRefs(EtlStoreContainer container, Set<Long> newMemberNodes, Set<Long> newMemberWays) {
		// TODO Auto-generated method stub

	}

	public CellId getCurrentCellId(OSMType type, long id) {
		// TODO Auto-generated method stub
		return null;
	}

}
