package org.heigit.bigspatialdata.oshdb.examples.workshop.workshop1;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHNodes;
import org.heigit.bigspatialdata.oshdb.index.XYGridTree;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultithreadDBProcessingAndCountNodes {
	private static final Logger LOG = LoggerFactory.getLogger(MultithreadDBProcessingAndCountNodes.class);

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		// load H2-support
		Class.forName("org.h2.Driver");

		// connect to the "Big"DB
		// set path to DB
		try (Connection conn = DriverManager.getConnection("jdbc:h2:D:/heidelberg-ccbysa", "sa", "");

				Statement stmt = conn.createStatement()) {

			XYGridTree tree = new XYGridTree(12);
			BoundingBox bbox = new BoundingBox(0, 10, 30, 60);
			Iterable<CellId> centralIt = tree.bbox2CellIds(bbox, true);

			Stream<CellId> a = StreamSupport.stream(centralIt.spliterator(), true);

			Stream<Integer> theInt = a.map(theCellId -> {
				int count = 0;
				try (final PreparedStatement pstmt = conn
						.prepareStatement("select data from grid_node where level = ? and id = ?")) {
					pstmt.setInt(1, theCellId.getZoomLevel());
					pstmt.setLong(2, theCellId.getId());

					try (final ResultSet rst2 = pstmt.executeQuery()) {
						if (rst2.next()) {

							final ObjectInputStream ois = new ObjectInputStream(rst2.getBinaryStream(1));
							final GridOSHNodes hosmCell = (GridOSHNodes) ois.readObject();
							return hosmCell;

						}
					} catch (IOException | ClassNotFoundException ex) {
						LOG.error(ex.toString());
					}
				} catch (SQLException ex) {
					LOG.error(ex.toString());
				}
				return null;

			}).map(cell -> {

				if (cell == null) {
					return 0;
				}
				int count = 0;
				Iterator<OSHNode> it = cell.iterator();
				while (it.hasNext()) {
					it.next();
					count++;
				}
				return count;
			});

			System.out.println(theInt.reduce((ff, pp) -> {
				return ff + pp;
			}).get());

		}
	}
}