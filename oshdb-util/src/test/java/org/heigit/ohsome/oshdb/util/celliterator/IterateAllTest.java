package org.heigit.ohsome.oshdb.util.celliterator;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.heigit.ohsome.oshdb.grid.GridOSHEntity;
import org.heigit.ohsome.oshdb.util.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.util.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.util.TableNames;
import org.heigit.ohsome.oshdb.util.celliterator.CellIterator.IterateAllEntry;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBKeytablesNotFoundException;
import org.heigit.ohsome.oshdb.util.taginterpreter.DefaultTagInterpreter;
import org.heigit.ohsome.oshdb.util.tagtranslator.TagTranslator;
import org.json.simple.parser.ParseException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class IterateAllTest {
  private static Connection conn;

  /**
   * Set up of test framework, loading H2 driver and connection via jdbc.
   */
  @BeforeClass
  public static void setUpClass() throws ClassNotFoundException, SQLException {
    // load H2-support
    Class.forName("org.h2.Driver");

    // connect to the "Big"DB
    IterateAllTest.conn = DriverManager.getConnection(
        "jdbc:h2:./src/test/resources/test-data;ACCESS_MODE_DATA=r",
        "sa",
        ""
    );
  }

  @AfterClass
  public static void breakDownClass() throws SQLException {
    IterateAllTest.conn.close();
  }

  public IterateAllTest() {
  }

  @SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection"})
  @Test
  public void testIssue108() throws SQLException, IOException, ClassNotFoundException,
      ParseException, OSHDBKeytablesNotFoundException {
    ResultSet oshCellsRawData = conn.prepareStatement(
        "select data from " + TableNames.T_NODES).executeQuery();

    int countTotal = 0;
    int countCreated = 0;
    int countOther = 0;

    try (TagTranslator tt = new TagTranslator(conn)) {
      while (oshCellsRawData.next()) {
        // get one cell from the raw data stream
        GridOSHEntity oshCellRawData = (GridOSHEntity) (new ObjectInputStream(
            oshCellsRawData.getBinaryStream(1))
        ).readObject();

        TreeSet<OSHDBTimestamp> timestamps = new TreeSet<>();
        timestamps.add(new OSHDBTimestamp(1325376000L));
        timestamps.add(new OSHDBTimestamp(1516375698L));

        List<IterateAllEntry> result = (new CellIterator(
            timestamps,
            new OSHDBBoundingBox(8, 9, 49, 50),
            new DefaultTagInterpreter(tt),
            oshEntity -> oshEntity.getId() == 617308093,
            osmEntity -> true,
            false
        )).iterateByContribution(
            oshCellRawData
        ).collect(Collectors.toList());
        countTotal += result.size();
        for (IterateAllEntry entry : result) {
          if (entry.activities.contains(ContributionType.CREATION)) {
            countCreated++;
          } else {
            countOther++;
          }
        }
      }
    }

    assertEquals(4, countTotal);
    assertEquals(0, countCreated);
    assertEquals(4, countOther);
  }
}
