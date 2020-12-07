package org.heigit.ohsome.oshdb.api.tests;

import static org.junit.Assert.assertEquals;

import java.util.SortedMap;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBH2;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.ohsome.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.util.tagtranslator.TagTranslator;
import org.heigit.ohsome.oshdb.filter.FilterParser;
import org.junit.Test;

/**
 * Tests integration of oshdb-filter library.
 *
 * <p>
 *   Only basic "is it working at all" tests are done, since the library itself has its own set
 *   of unit tests.
 * </p>
 */
public class TestOhsomeFilter {
  private final OSHDBDatabase oshdb;
  private final FilterParser filterParser;

  private final OSHDBBoundingBox bbox =
      new OSHDBBoundingBox(8.651133, 49.387611, 8.6561, 49.390513);

  /**
   * Creates a test runner using the H2 backend.
   * @throws Exception if something goes wrong.
   */
  public TestOhsomeFilter() throws Exception {
    OSHDBH2 oshdb = new OSHDBH2("./src/test/resources/test-data");
    filterParser = new FilterParser(new TagTranslator(oshdb.getConnection()));
    this.oshdb = oshdb;
  }

  private MapReducer<OSMEntitySnapshot> createMapReducer() throws Exception {
    return OSMEntitySnapshotView.on(oshdb)
        .areaOfInterest(bbox)
        .timestamps("2014-01-01");
  }

  @Test
  public void testFilterString() throws Exception {
    Number result = createMapReducer()
        .map(x -> 1)
        .filter("type:way and geometry:polygon and building=*")
        .sum();

    assertEquals(42, result.intValue());
  }

  @Test
  public void testFilterObject() throws Exception {
    MapReducer<OSMEntitySnapshot> mr = createMapReducer();
    Number result = mr
        .filter(filterParser.parse("type:way and geometry:polygon and building=*"))
        .count();

    assertEquals(42, result.intValue());
  }

  @Test
  public void testAggregateFilter() throws Exception {
    SortedMap<OSMType, Integer> result = createMapReducer()
        .aggregateBy(x -> x.getEntity().getType())
        .filter("(geometry:polygon or geometry:other) and building=*")
        .count();

    assertEquals(2, result.entrySet().size());
    assertEquals(42, result.get(OSMType.WAY).intValue());
    assertEquals(1, result.get(OSMType.RELATION).intValue());
  }

  @Test
  public void testAggregateFilterObject() throws Exception {
    MapReducer<OSMEntitySnapshot> mr = createMapReducer();
    SortedMap<OSMType, Integer> result = mr
        .aggregateBy(x -> x.getEntity().getType())
        .filter(filterParser.parse("(geometry:polygon or geometry:other) and building=*"))
        .count();

    assertEquals(42, result.get(OSMType.WAY).intValue());
  }
}
