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
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.CellId;

public class GetHosmCell {
	private static final Logger LOG = Logger.getLogger(GetHosmCell.class.getName());
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		// load H2-support
		Class.forName("org.h2.Driver");
		
        //get all needed cell-ids:
        XYGridTree grid = new XYGridTree(12);
        BoundingBox bbox = new BoundingBox(0, 10, 30, 60);
        Iterator<CellId> cellIds = grid.bbox2CellIds(bbox, true).iterator();

        //connect to the "Big"DB
		// set path to DB
        try (Connection conn = DriverManager.getConnection("jdbc:h2:D:/heidelberg-ccbysa", "sa", "");
                Statement stmt = conn.createStatement()) {
            //iterate over cellIds
            while (cellIds.hasNext()) {
                CellId curr = cellIds.next();

                //prepare a SQL-statement
                PreparedStatement pstmt = conn.prepareStatement("select data from grid_way where level = ? and id = ?");
                pstmt.setInt(1, curr.getZoomLevel());
                pstmt.setLong(2, curr.getId());

                //execute statement
                ResultSet rst2 = pstmt.executeQuery();

                //iterate over result (this should be only one)
                while (rst2.next()) {
                    //convert to Object-Stream (Cells) [has only one object, which is the Cell]
                    ObjectInputStream ois = new ObjectInputStream(rst2.getBinaryStream(1));
                    //get one object (Cell) from the stream
                    GridOSHWays hosmCell = (GridOSHWays) ois.readObject();

                    //Your now have a hosmCell of ways, you can do funny things with
                }
            }

        }
    }
}
		


