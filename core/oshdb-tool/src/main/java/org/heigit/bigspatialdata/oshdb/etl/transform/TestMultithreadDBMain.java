package org.heigit.bigspatialdata.oshdb.etl.transform;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalDouble;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.index.XYGrid;
import org.heigit.bigspatialdata.oshdb.index.XYGridTree;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.CellId;

public class TestMultithreadDBMain {
	
	public static class ZoomId {
		public final int zoom;
		public final long id;

		public ZoomId(final int zoom, final long id) {
			this.zoom = zoom;
			this.id = id;
		}
	}

	public static void main(String[] args) throws ClassNotFoundException {
		Class.forName("org.h2.Driver");

		try (Connection conn = DriverManager.getConnection("jdbc:h2:./hosmdb", "sa", "");
				final Statement stmt = conn.createStatement()) {
			List<ZoomId> zoomIds = new ArrayList<>();

			
			
			
			
			XYGridTree grid = new XYGridTree(13);
			
			List<CellId> cellIds = new ArrayList();
			
			grid.getIds(85.31544,  27.70783).forEach(cellId -> cellIds.add(cellId));
			
			
			
//			long totalSum = 
					
			//(Stream<CellId>)
			//StreamSupport.stream(grid.getIds(85.31544,  27.70783).spliterator(), false)
			
			OptionalDouble maxTagKeyId = cellIds.parallelStream()
			
			.flatMap(cellId -> {
				System.out.println(cellId);
				XYGrid g = new XYGrid(OSHDB.MAXZOOM);
				System.out.println(Arrays.toString(g.getCellDimensions(((CellId)cellId).getId()).getMaxValuesPerDimension()));
				System.out.println(Arrays.toString(g.getCellDimensions(((CellId)cellId).getId()).getMinValuesPerDimension()));
				
				List<GridOSHEntity> cells = new ArrayList<>();
				try (final PreparedStatement pstmt = conn
						.prepareStatement("select data from grid_way where level = ? and id = ?")) {
					pstmt.setInt(1, ((CellId)cellId).getZoomLevel());
					pstmt.setLong(2, ((CellId)cellId).getId());

					try (final ResultSet rst2 = pstmt.executeQuery()) {
						while(rst2.next()) {
							final ObjectInputStream ois = new ObjectInputStream(rst2.getBinaryStream(1));
							final GridOSHEntity hosmCell = (GridOSHEntity) ois.readObject();
							// TODO cache the hosmCell here!

							cells.add(hosmCell);
						}
					}
				} catch (IOException | SQLException | ClassNotFoundException e) {
					e.printStackTrace();
				}
				return cells.stream();
			})
			.flatMap(hosmCell -> (Stream<OSHEntity>)StreamSupport.stream(((GridOSHEntity)hosmCell).spliterator(), false))
		//	.filter(hosm -> hosm.getId() == 5352989l)
			
			//.limit(10)
			.flatMap(hosm -> StreamSupport.stream(((OSHEntity)hosm).spliterator(),false))
			
			.mapToInt(osm -> {
				
				int[] tags = ((OSMEntity)osm).getTags();
			
				if(tags.length > 0)
					return tags[tags.length-2];
				return -1;
			})
			
			
			.average();
			
			
			System.out.println(maxTagKeyId.getAsDouble());
			
			
			
		//	.forEach(System.out::println);
			//.count();
			//.forEach(hosm -> {System.out.printf("id:%d\n",hosm.getId());});
			
			
			//System.out.println(totalSum);
			
			/*
			.forEach(hosm -> {
				try {
					
					List<OSMRelation> versions = ((HOSMRelation)hosm).getVersions();
					versions.forEach(v -> {
						System.out.println(v);
						OSMMember[] members = v.getMembers();
						for(OSMMember member : members){
							System.out.printf("t:%d i:%d e:%s\n",member.getType(),member.getId(),member.getEntity());
						}
					});
					
					List<HOSMWay> ways = ((HOSMRelation)hosm).getWays();
					ways.forEach(System.out::println);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				
			});
			
			*/
				
			

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
