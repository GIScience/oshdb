package org.heigit.bigspatialdata.oshdb.examples.activity;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHNodes;
import org.heigit.bigspatialdata.oshdb.index.XYGrid;
import org.heigit.bigspatialdata.oshdb.index.XYGridTree;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;

public class ActivityIndicatorFromPolygon {
	
	
	Map<Long, Map<Long, Long>> cellTimestampActcivity = new TreeMap<>(); // cellId,
																			// Timestamp,
																			// Indicator

	public static void main(String[] args) throws ClassNotFoundException, ParseException {
		Class.forName("org.h2.Driver");

		GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory( null );
		WKTReader wktReader = new WKTReader(geometryFactory);
		String polygonString = "POLYGON ((8.69282133333848961 49.41176730111411075, 8.69436717935256631 49.40753302724946394, 8.70713721164276855 49.40934771604860032, 8.71271569943269952 49.41089356206267524, 8.71842860861515767 49.41459015035720626, 8.71069937854477239 49.41351477921698176, 8.70081940619393279 49.41284267225433524, 8.69282133333848961 49.41176730111411075))";
		Polygon inputPolygon = (Polygon) wktReader.read(polygonString);
		//Polygon inputEnvelope = (Polygon) inputPolygon.getEnvelope();
		
		Double minLon = JTS.toEnvelope(inputPolygon).getMinX();
		Double maxLon = JTS.toEnvelope(inputPolygon).getMaxX();
		Double minLat = JTS.toEnvelope(inputPolygon).getMinY();
		Double maxLat = JTS.toEnvelope(inputPolygon).getMaxY();
		
		BoundingBox inputBbox = new BoundingBox(minLon, maxLon, minLat, maxLat);
		
		//System.out.println(JTS.toEnvelope(inputPolygon));
		
		List<Long> timestamps = new ArrayList<>();
		final SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
		formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
		for (int year = 2016; year <= 2016; year++) {
			for (int month = 1; month <= 12; month++) {
				try {
					timestamps.add(formatter.parse(String.format("%d%02d01", year, month)).getTime());
				} catch (java.text.ParseException e) {
					System.err.println("basdoawrd");
				}
				;
			}
		}
		Collections.sort(timestamps, Collections.reverseOrder());

		//BoundingBox bbox = new BoundingBox(8.61, 8.76, 49.40, 49.41);

		XYGridTree grid = new XYGridTree(12);

		final List<Long> cellIds = new ArrayList<>();
		grid.bbox2CellIds(inputBbox, false).forEach(cell -> {
			if (cell.getZoomLevel() == 12) {
				cellIds.add(cell.getId());
			}
		});

		// connect to the "Big"DB
		try (Connection conn = DriverManager.getConnection("jdbc:h2:tcp://localhost/~/git/OSH-BigDB/core/oshdb-examples/src/main/resources/heidelberg-ccbysa",
				"sa", "")) {

			cellIds.parallelStream().flatMap(cellId -> {
				try (final PreparedStatement pstmt = conn
						.prepareStatement("(select data from grid_node where level = ? and id = ?)")) {
					pstmt.setInt(1, 12);
					pstmt.setLong(2, cellId);

					try (final ResultSet rst2 = pstmt.executeQuery()) {
						List<GridOSHNodes> cells = new LinkedList<>();
						while (rst2.next()) {
							final ObjectInputStream ois = new ObjectInputStream(rst2.getBinaryStream(1));
							cells.add((GridOSHNodes) ois.readObject());
						}
						return cells.stream();
					}
				} catch (IOException | SQLException | ClassNotFoundException e) {
					e.printStackTrace();
					return null;
				}
			}).map(oshCell -> {

				GridOSHNodes cell = (GridOSHNodes) oshCell;

				Map<Long, Map<Long, Long>> result = new TreeMap<>();

				Map<Long, Long> timestampActivity = new TreeMap<>();

				result.put(cell.getId(), timestampActivity);

				
				
				
				
				Iterator<OSHNode> itr = cell.iterator();
				while (itr.hasNext()) {
					OSHNode osh = itr.next();
					//System.out.println(osh.getBoundingBox().toJTSGeometry());
					//	TODO osh.getBoundingBox delivers wrong precision
 
					if (!  osh.getBoundingBox().toJTSGeometry().intersects(inputPolygon)) { 
						continue; 
						}
 
					List<OSMNode> versions = new ArrayList<>();
					for (OSMNode osm : osh) {
//						System.out.println(osm);
						if (osm.toJTSGeometry().intersects(inputPolygon))
						{
							versions.add(osm);
						}
					}
					//osh.forEach(osm -> 	versions.add(osm));
					List <OSMNode> numberOfAllVersions = osh.getVersions();
					if(numberOfAllVersions.size()!=versions.size()){
					System.out.println("Number of all Versions in OSH object: " + numberOfAllVersions.size() + " number of versions in polygon: " + versions.size());
					}
					int v = 0;
					for (int i = 0; i < timestamps.size(); i++) {
						long ts = timestamps.get(i);
						long count = 0;
						while (v < versions.size() && versions.get(v).getTimestamp() > ts) {
							count++;
							v++;
						}

						if (timestampActivity.containsKey(ts)) {
							timestampActivity.put(ts, timestampActivity.get(ts) + count);
						} else {
							timestampActivity.put(ts, count);
						}

						if (v >= versions.size())
							break;

					}

				}
				
				
				XYGrid xy = new XYGrid(12);
				MultiDimensionalNumericData dimensions = xy.getCellDimensions(cell.getId());
				dimensions.getMinValuesPerDimension();
				dimensions.getMaxValuesPerDimension();
				
				Envelope e = new Envelope(
						dimensions.getMinValuesPerDimension()[0],
						dimensions.getMaxValuesPerDimension()[0],
						dimensions.getMinValuesPerDimension()[1],
						dimensions.getMaxValuesPerDimension()[1]);
//				System.out.println(e);
				
				Polygon p = JTS.toGeometry(e);
				
//				System.out.println(p.toText());
				
				StringBuilder sb = new StringBuilder();
				for(Map.Entry<Long,Long> entry : timestampActivity.entrySet()){
					sb.append(entry.getValue()).append(";");
				}
				
				
				//System.out.printf("%s;%s\n",p.toText(),sb.toString());
				
				//System.out.println(result);
				return result;
			}).count();

			

		} catch (

		SQLException e) { // TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
