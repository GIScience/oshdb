package org.heigit.bigspatialdata.oshdb.tool.importer.util.etl;

import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.CellId;

public class EtlStoreContainer {
	public CellId cellId;
	public OSMType type;
	public long id;
	public OSHEntity entity;
	public long backRefOffset;
}
