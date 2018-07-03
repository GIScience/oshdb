package org.heigit.bigspatialdata.oshdb.tool.importer.load.handle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.heigit.bigspatialdata.oshdb.grid.GridOSHNodes;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHRelations;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHWays;
import org.heigit.bigspatialdata.oshdb.index.XYGrid;
import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osh.OSHRelation;
import org.heigit.bigspatialdata.oshdb.osh.OSHWay;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransfomRelation;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHNode;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHWay;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import it.unimi.dsi.fastutil.longs.Long2ObjectAVLTreeMap;

public abstract class OSHDbHandler extends LoaderHandler {

	public abstract void handleNodeGrid(GridOSHNodes grid);

	public abstract void handleWayGrid(GridOSHWays grid);

	public abstract void handleRelationsGrid(GridOSHRelations grid);

	protected final Roaring64NavigableMap bitmapNodeRelation;
	protected final Roaring64NavigableMap bitmapWayRelation;

	protected OSHDbHandler(Roaring64NavigableMap bitmapNodeRelation, Roaring64NavigableMap bitmapWayRelation) {
		this.bitmapNodeRelation = bitmapNodeRelation;
		this.bitmapWayRelation = bitmapWayRelation;
	}

	@Override
	public void handleNodeGrid(long zId, Collection<TransformOSHNode> nodes) {
		if (zId < 0)
			return;
		int zoom = ZGrid.getZoom(zId);
		XYGrid xyGrid = new XYGrid(zoom);

		OSHDBBoundingBox bbox = ZGrid.getBoundingBox(zId);
		long longitude = bbox.getMinLonLong() + (bbox.getMaxLonLong() - bbox.getMinLonLong()) / 2;
		long latitude = bbox.getMinLatLong() + (bbox.getMaxLatLong() - bbox.getMinLatLong()) / 2;
		long xyId = xyGrid.getId(longitude, latitude);

		List<OSHNode> gridNodes = nodes.stream().map(osh2 -> {
			List<OSMNode> versions = osh2.stream().collect(Collectors.toList());
			try {
				OSHNode osh = OSHNode.build(versions);
				return osh;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}).collect(Collectors.toList());

		try {
			if (gridNodes.size() != 0) {

				GridOSHNodes grid = GridOSHNodes.rebase(xyId, zoom, gridNodes.get(0).getId(), 0, longitude, latitude,
						gridNodes);
				handleNodeGrid(grid);
			} else {
				System.out.println("no noded at " + xyId);
			}

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	protected Long2ObjectAVLTreeMap<OSHWay> waysForRelation = new Long2ObjectAVLTreeMap<>();

	@Override
	public void handleWayGrid(long zId, Collection<TransformOSHWay> ways, Collection<TransformOSHNode> nodes) {
		if (zId < 0)
			return;
		int zoom = ZGrid.getZoom(zId);
		XYGrid xyGrid = new XYGrid(zoom);

		OSHDBBoundingBox bbox = ZGrid.getBoundingBox(zId);
		long longitude = bbox.getMinLonLong() + (bbox.getMaxLonLong() - bbox.getMinLonLong()) / 2;
		long latitude = bbox.getMinLatLong() + (bbox.getMaxLatLong() - bbox.getMinLatLong()) / 2;
		long xyId = xyGrid.getId(longitude, latitude);

		Map<Long, OSHNode> idOshMap = new HashMap<>(nodes.size());
		nodes.forEach(osh2 -> {
			Long id = osh2.getId();
			List<OSMNode> versions = osh2.stream().collect(Collectors.toList());
			OSHNode osh;
			try {
				osh = OSHNode.build(versions);
				idOshMap.put(id, osh);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});

		List<OSHWay> gridWays = ways.stream().map(tosh -> {
			List<OSMWay> versions = tosh.stream().collect(Collectors.toList());

			List<OSHNode> nodesForThisWay = new ArrayList<>(tosh.getNodeIds().length);
			for (long id : tosh.getNodeIds()) {
				if (idOshMap.get(id) == null) {
					continue;
				}
				nodesForThisWay.add(idOshMap.get(id));
			}

			try {
				OSHWay osh = OSHWay.build(versions, nodesForThisWay);
				if (bitmapWayRelation.contains(osh.getId()))
					waysForRelation.put(osh.getId(), osh);
				return osh;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}).collect(Collectors.toList());

		gridWays.sort((a, b) -> Long.compare(a.getId(), b.getId()));

		try {
			if (gridWays.size() != 0) {
				GridOSHWays grid = GridOSHWays.compact(xyId, zoom, gridWays.get(0).getId(), 0, longitude, latitude,
						gridWays);
				handleWayGrid(grid);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void handleRelationGrid(long zId, Collection<TransfomRelation> entities, Collection<TransformOSHNode> nodes,
			Collection<TransformOSHWay> ways) {
		if (zId < 0)
			return;
		int zoom = ZGrid.getZoom(zId);
		XYGrid xyGrid = new XYGrid(zoom);

		OSHDBBoundingBox bbox = ZGrid.getBoundingBox(zId);
		long longitude = bbox.getMinLonLong() + (bbox.getMaxLonLong() - bbox.getMinLonLong()) / 2;
		long latitude = bbox.getMinLatLong() + (bbox.getMaxLatLong() - bbox.getMinLatLong()) / 2;
		long xyId = xyGrid.getId(longitude, latitude);

		Map<Long, OSHNode> idOshMap = new HashMap<>(nodes.size());
		nodes.forEach(osh2 -> {
			Long id = osh2.getId();
			List<OSMNode> versions = osh2.stream().collect(Collectors.toList());
			OSHNode osh;
			try {
				osh = OSHNode.build(versions);
				idOshMap.put(id, osh);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});

		List<OSHRelation> gridRelation = entities.stream().map(osh2 -> {
			List<OSMRelation> versions = osh2.stream().collect(Collectors.toList());

			long rid = versions.get(0).getId();

			List<OSHNode> nodesForThisRelation = new ArrayList<>(osh2.getNodeIds().length);
			for (long id : osh2.getNodeIds()) {
				OSHNode node = idOshMap.get(id);
				if (node != null)
					nodesForThisRelation.add(node);

			}

			List<OSHWay> waysForThisRelation = new ArrayList<>(osh2.getWayIds().length);
			for (long id : osh2.getWayIds()) {
				OSHWay way = waysForRelation.get(id);
				if (way != null)
					waysForThisRelation.add(way);
			}

			try {

				OSHRelation ret = OSHRelation.build(versions, nodesForThisRelation, waysForThisRelation);

				return ret;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}).collect(Collectors.toList());

		try {
			GridOSHRelations grid = GridOSHRelations.compact(xyId, zoom, gridRelation.get(0).getId(), 0, longitude,
					latitude, gridRelation);
			handleRelationsGrid(grid);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
