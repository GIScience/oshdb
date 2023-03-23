package org.heigit.ohsome.oshdb.util.celliterator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ObjectInputStream;
import java.util.List;
import java.util.TreeSet;
import org.h2.jdbcx.JdbcConnectionPool;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.grid.GridOSHEntity;
import org.heigit.ohsome.oshdb.util.TableNames;
import org.heigit.ohsome.oshdb.util.celliterator.CellIterator.IterateAllEntry;
import org.heigit.ohsome.oshdb.util.taginterpreter.DefaultTagInterpreter;
import org.heigit.ohsome.oshdb.util.tagtranslator.JdbcTagTranslator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests the {@link CellIterator#iterateByContribution(GridOSHEntity)} method.
 */
class IterateByContributionTest {
  private static JdbcConnectionPool source;

  /**
   * Set up of test framework, loading H2 driver and connection via jdbc.
   */
  @BeforeAll
  static void setUpClass() {
    // connect to the "Big"DB
    source =  JdbcConnectionPool.create(
        "jdbc:h2:../data/test-data;ACCESS_MODE_DATA=r",
        "sa",
        ""
    );
  }

  @AfterAll
  static void breakDownClass() {
    source.dispose();
  }

  IterateByContributionTest() {}

  @SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection"})
  @Test
  void testIssue108() throws Exception {
    int countTotal = 0;
    int countCreated = 0;
    int countOther = 0;

    var tagTranslator = new JdbcTagTranslator(source);
    try (var conn = source.getConnection();
        var stmt = conn.createStatement();
        var oshCellsRawData = stmt.executeQuery("select data from " + TableNames.T_NODES)) {
      while (oshCellsRawData.next()) {
        // get one cell from the raw data stream
        GridOSHEntity oshCellRawData =
            (GridOSHEntity) (new ObjectInputStream(oshCellsRawData.getBinaryStream(1)))
                .readObject();

        TreeSet<OSHDBTimestamp> timestamps = new TreeSet<>();
        timestamps.add(new OSHDBTimestamp(1325376000L));
        timestamps.add(new OSHDBTimestamp(1516375698L));

        List<IterateAllEntry> result = (new CellIterator(
            timestamps,
            OSHDBBoundingBox.bboxWgs84Coordinates(8.0, 9.0, 49.0, 50.0),
            new DefaultTagInterpreter(tagTranslator),
            oshEntity -> oshEntity.getId() == 617308093,
            osmEntity -> true,
            false
        )).iterateByContribution(
            oshCellRawData
        ).toList();
        countTotal += result.size();
        for (IterateAllEntry entry : result) {
          if (entry.activities().contains(ContributionType.CREATION)) {
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
