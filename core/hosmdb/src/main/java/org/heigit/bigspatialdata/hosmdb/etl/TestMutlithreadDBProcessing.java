package org.heigit.bigspatialdata.hosmdb.etl;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.heigit.bigspatialdata.hosmdb.grid.HOSMCellWays;

public class TestMutlithreadDBProcessing {

	public static class ZoomId {
		public final int zoom;
		public final long id;

		public ZoomId(final int zoom, final long id) {
			this.zoom = zoom;
			this.id = id;
		}
	}

	public static class Result {
		public final int zoom;
		public final long id;
		public final long count;

		public Result(final int zoom, final long id, final long count) {
			this.zoom = zoom;
			this.id = id;
			this.count = count;
		}

		public Result reduce(Result other) {
			return new Result(zoom, id, count + other.count);
		}

		@Override
		public String toString() {
			return String.format("zoom:%d id:%d -> %d", zoom, id, count);
		}
	}

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Class.forName("org.h2.Driver");

		try (Connection conn = DriverManager.getConnection("jdbc:h2:./hosmdb", "sa", "");
				final Statement stmt = conn.createStatement()) {

			List<ZoomId> zoomIds = new ArrayList<>();

			System.out.println("Select ids from DB");
			ResultSet rst = stmt.executeQuery("select level,id from grid_way");
			while (rst.next()) {
				zoomIds.add(new ZoomId(rst.getInt(1), rst.getLong(2)));
			}
			rst.close();

			System.out.println("Process in parallel");
			Map<Integer, LongSummaryStatistics> totalPerZoom = zoomIds.parallelStream().map(zoomId -> {

				try (final PreparedStatement pstmt = conn
						.prepareStatement("select data from grid_way where level = ? and id = ?")) {
					pstmt.setInt(1, zoomId.zoom);
					pstmt.setLong(2, zoomId.id);

					try (final ResultSet rst2 = pstmt.executeQuery()) {
						if (rst2.next()) {
							final ObjectInputStream ois = new ObjectInputStream(rst2.getBinaryStream(1));
							final HOSMCellWays hosmCell = (HOSMCellWays) ois.readObject();
							// TODO cache the hosmCell here!

							return hosmCell;
						} else {
							System.err.printf("ERROR: no result for level:%d and id:%d\n", zoomId.zoom, zoomId.id);
							return null;
						}
					}
				} catch (IOException | SQLException | ClassNotFoundException e) {
					e.printStackTrace();
					return null;
				}
			}).map(hosmCell -> {
				
				// here is the actual logic
				
				final int zoom = hosmCell.getLevel();
				final long id = hosmCell.getId();
				final long count = StreamSupport.stream(hosmCell.spliterator(), false).count();

				return new Result(zoom, id, count);
				
				
			}).collect(Collectors.groupingBy(result -> result.zoom, Collectors.summarizingLong(result -> result.count)))
			// .forEach(System.out::println);
			;

			totalPerZoom.entrySet().stream()
					.map(entry -> String.format("Zoom:%d Stats:%s", entry.getKey(), entry.getValue()))
					.forEach(System.out::println);
		}
	}
}
