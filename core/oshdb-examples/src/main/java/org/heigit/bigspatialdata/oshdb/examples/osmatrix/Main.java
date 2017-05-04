package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

import org.geotools.data.DataUtilities;
import org.geotools.data.collection.SpatialIndexFeatureCollection;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.factory.GeoTools;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.grid.Grids;
import org.heigit.bigspatialdata.oshdb.examples.bbox.Heidelberg;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHNodes;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHRelations;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHWays;
import org.heigit.bigspatialdata.oshdb.index.XYGrid;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.util.CellId.cellIdExeption;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;

public class Main {

	public static void main(String[] args) throws ClassNotFoundException, SQLException, cellIdExeption, ParseException, IOException {
		Class.forName("org.h2.Driver");

		List<Long> timestamps = new ArrayList<>();
		final SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
		formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
		for (int year = 2016; year <= 2016; year++) {
			for (int month = 1; month <= 12; month++) {
				try {
					timestamps.add(formatter.parse(String.format("%d%02d01", year, month)).getTime() / 1000);
				} catch (java.text.ParseException e) {
					System.err.println("basdoawrd");
				}
			}
		}
		Collections.sort(timestamps, Collections.reverseOrder());

		WKTReader r = new WKTReader();
		// http://arthur-e.github.io/Wicket/sandbox-gmaps3.html
		Polygon inputPolygon = (Polygon) r.read(Heidelberg.wkt);

		Double minLon = JTS.toEnvelope(inputPolygon).getMinX();
		Double maxLon = JTS.toEnvelope(inputPolygon).getMaxX();
		Double minLat = JTS.toEnvelope(inputPolygon).getMinY();
		Double maxLat = JTS.toEnvelope(inputPolygon).getMaxY();

		
		ReferencedEnvelope gridBounds =  JTS.toEnvelope(inputPolygon);// new ReferencedEnvelope(minLon, maxLon,minLat, maxLat,  DefaultGeographicCRS.WGS84);

		// length of each hexagon edge
		double sideLen = 0.01;
		SimpleFeatureSource grid = Grids.createHexagonalGrid(gridBounds, sideLen);	
	
		/*
		try (SimpleFeatureIterator iterator = grid.getFeatures().features()){
		     while( iterator.hasNext() ){
		           SimpleFeature feature = iterator.next();
		           System.out.println(feature);
		     }
		}
		*/
		
		
		SpatialIndexFeatureCollection gridIndex  = new SpatialIndexFeatureCollection(grid.getFeatures());
		

		Point p = (Point) r.read("Point(11.073178799999898 17.454303637844333)");
		
		
		final FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2( GeoTools.getDefaultHints() );
		
		
		// Fast spatial Access
		final SimpleFeatureSource source = DataUtilities.source( gridIndex );
		
	
		
		

		
		

		
		//try (Connection conn = DriverManager.getConnection("jdbc:h2:/home/rtroilo/heigit_git/data/heidelberg-ccbysa",
		try (Connection conn = DriverManager.getConnection("jdbc:h2:tcp://localhost/d:/eclipseNeon2Workspace/OSH-BigDB/core/hosmdb/resources/oshdb/heidelberg-ccbysa",
				"sa", "")) {
			Statement stmt = conn.createStatement();

			List<CellId> cellIds = new ArrayList<>();
			try (ResultSet rst = stmt.executeQuery(
					"select level, id from grid_node union select level, id from grid_way union select level, id from grid_relation")) {
				while (rst.next()) {
					cellIds.add(new CellId(rst.getInt(1), rst.getLong(2)));
				}
			}

			cellIds.parallelStream().flatMap(cellId -> {
				List<GridOSHEntity> cells = new LinkedList<>();

				try (final PreparedStatement pstmtNode = conn
						.prepareStatement("(select data from grid_node where level = ? and id = ?)");
						final PreparedStatement pstmtWay = conn
								.prepareStatement("(select data from grid_way where level = ? and id = ?)");
						final PreparedStatement pstmtRel = conn
								.prepareStatement("(select data from grid_relation where level = ? and id = ?)")) {

					PreparedStatement pstmt = pstmtNode;

					pstmt.setInt(1, cellId.getZoomLevel());
					pstmt.setLong(2, cellId.getId());

					try (final ResultSet rst = pstmt.executeQuery()) {
						while (rst.next()) {
							final ObjectInputStream ois = new ObjectInputStream(rst.getBinaryStream(1));
							cells.add((GridOSHEntity) ois.readObject());
						}
					}

					pstmt = pstmtWay;

					pstmt.setInt(1, cellId.getZoomLevel());
					pstmt.setLong(2, cellId.getId());

					try (final ResultSet rst = pstmt.executeQuery()) {
						while (rst.next()) {
							final ObjectInputStream ois = new ObjectInputStream(rst.getBinaryStream(1));
							cells.add((GridOSHEntity) ois.readObject());
						}
					}

					pstmt = pstmtRel;

					pstmt.setInt(1, cellId.getZoomLevel());
					pstmt.setLong(2, cellId.getId());

					try (final ResultSet rst = pstmt.executeQuery()) {
						while (rst.next()) {
							final ObjectInputStream ois = new ObjectInputStream(rst.getBinaryStream(1));
							cells.add((GridOSHEntity) ois.readObject());
						}
					}

				} catch (IOException | SQLException | ClassNotFoundException e) {
					e.printStackTrace();
				}

				return cells.stream();
			}).map(oshCell -> {
				
				
				XYGrid xy = new XYGrid(oshCell.getLevel());
				MultiDimensionalNumericData dimensions = xy.getCellDimensions(oshCell.getId());
				dimensions.getMinValuesPerDimension();
				dimensions.getMaxValuesPerDimension();		
				Envelope e = new Envelope(
						dimensions.getMinValuesPerDimension()[0],
						dimensions.getMaxValuesPerDimension()[0],
						dimensions.getMinValuesPerDimension()[1],
						dimensions.getMaxValuesPerDimension()[1]);
				
				Polygon bbox = JTS.toGeometry(e);
				
//				System.out.println(bbox);
				
				Filter filter = ff.intersects( ff.property( "element"), ff.literal( bbox ) );
				SimpleFeatureCollection features;
				try {
					features = source.getFeatures( filter );
					try (SimpleFeatureIterator iterator = features.features()){
					     while( iterator.hasNext() ){
					           SimpleFeature feature = iterator.next();
					           System.out.println(feature);
					     }
					}
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
				
				
				
				
				if (oshCell instanceof GridOSHNodes) {
					GridOSHNodes cell = (GridOSHNodes) oshCell;
					System.out.println(cell);
				} else if (oshCell instanceof GridOSHWays) {
					GridOSHWays cell = (GridOSHWays) oshCell;
					System.out.println(cell);
				} else if (oshCell instanceof GridOSHRelations) {
					GridOSHRelations cell = (GridOSHRelations) oshCell;
					System.out.println(cell);
				}

				return Collections.emptyMap();
			}).count();

		}

	}

}
