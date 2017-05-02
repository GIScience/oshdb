package org.heigit.bigspatialdata.oshdb.examples.workshop;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.logging.Logger;

import org.heigit.bigspatialdata.oshdb.grid.GridOSHWays;
import org.heigit.bigspatialdata.oshdb.index.XYGridTree;
import org.heigit.bigspatialdata.oshdb.osh.OSHWay;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.CellId;

public class GetOshObject {
	private static final Logger LOG = Logger.getLogger(GetHosmCell.class.getName());

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		Class.forName("org.h2.Driver");

		// connect to the "Big"DB
		try (Connection conn = DriverManager.getConnection("jdbc:h2:D:/heidelberg-ccbysa", "sa", "");
				Statement stmt = conn.createStatement()) {

			// prepare a SQL-statement
			PreparedStatement pstmt = conn.prepareStatement("select data from grid_way where level = ? and id = ?");
			pstmt.setInt(1, 12);
			pstmt.setLong(2, 6486113L);

			// execute statement
			ResultSet rst2 = pstmt.executeQuery();

			// iterate over result (this should be only one)
			while (rst2.next()) {
				// convert to Object-Stream (Cells) [has only one object, which
				// is the Cell]
				ObjectInputStream ois = new ObjectInputStream(rst2.getBinaryStream(1));
				// get one object (Cell) from the stream
				GridOSHWays hosmCell = (GridOSHWays) ois.readObject();

				// get iterator over OSHWays
				Iterator<OSHWay> it = hosmCell.iterator();
				// iterate over OSH-Ways
				while (it.hasNext()) {
					OSHWay curr = it.next();
					// get iterator over OSM-Ways
					Iterator<OSMWay> it2 = curr.iterator();
					// iterate over OSM-Ways (versions)
					while (it2.hasNext()) {
						OSMWay curr2 = it2.next();
					}
					
				}

			}
		}
	}
}

