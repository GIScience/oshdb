package org.heigit.bigspatialdata.oshdb.util.celliterator;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import org.heigit.bigspatialdata.oshdb.TableNames;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.celliterator.CellIterator.IterateAllEntry;
import org.heigit.bigspatialdata.oshdb.util.celliterator.CellIterator.TimestampInterval;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.DefaultTagInterpreter;
import org.json.simple.parser.ParseException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class IterateAllTest {
  private static Connection conn;

  @BeforeClass
  public static void setUpClass() throws ClassNotFoundException, SQLException {
    // load H2-support
    Class.forName("org.h2.Driver");

    // connect to the "Big"DB
    IterateAllTest.conn =
        DriverManager.getConnection("jdbc:h2:./src/test/resources/test-data;ACCESS_MODE_DATA=r", "sa", "");

  }

  @AfterClass
  public static void breakDownClass() throws SQLException {
    IterateAllTest.conn.close();
  }

  public IterateAllTest() {}

  @Test
  public void testIssue108()
      throws SQLException, IOException, ClassNotFoundException, ParseException {
    ResultSet oshCellsRawData = conn.prepareStatement("select data from " + TableNames.T_NODES).executeQuery();

    int countTotal = 0;
    int countCreated = 0;
    int countOther = 0;
    while (oshCellsRawData.next()) {
      // get one cell from the raw data stream
      GridOSHEntity oshCellRawData = (GridOSHEntity) (new ObjectInputStream(
          oshCellsRawData.getBinaryStream(1))
      ).readObject();

      List<IterateAllEntry> result = CellIterator.iterateAll(
          oshCellRawData,
          new BoundingBox(8, 9, 49, 50),
          new TimestampInterval(1325376000L, 1516375698L),
          DefaultTagInterpreter.fromJDBC(conn),
          oshEntity -> oshEntity.getId() == 617308093,
          osmEntity -> true,
          false
      ).collect(Collectors.toList());
      countTotal += result.size();
      for (IterateAllEntry entry : result) {
        if (entry.activities.contains(ContributionType.CREATION))
          countCreated++;
        else
          countOther++;
      }
    }

    assertEquals(countTotal, 4);
    assertEquals(countCreated, 0);
    assertEquals(countOther, 4);
  }
}
