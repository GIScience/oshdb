package org.heigit.bigspatialdata.oshdb.examples.workshop.workshop1;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

public class ConnectToDB {
	private static final Logger LOG = Logger.getLogger(ConnectToDB.class.getName());
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		// load H2-support
		Class.forName("org.h2.Driver");

		// connect to the "Big"DB
		// set path to DB
		try (Connection conn = DriverManager.getConnection("jdbc:h2:D:/heidelberg-ccbysa", "sa", "");
				Statement stmt = conn.createStatement()) {

			// YOUR CODE GOES HERE

		}
	}

}
