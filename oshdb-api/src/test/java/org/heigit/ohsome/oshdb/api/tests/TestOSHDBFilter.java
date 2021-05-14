package org.heigit.ohsome.oshdb.api.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.SortedMap;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.api.db.OSHDBH2;
import org.heigit.ohsome.oshdb.api.db.OSHDBJdbc;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.ohsome.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.ohsome.oshdb.api.object.OSMContribution;
import org.heigit.ohsome.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.ohsome.oshdb.filter.FilterParser;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.tagtranslator.TagTranslator;
import org.junit.Test;

/**
 * Tests integration of oshdb-filter library.
 *
 * <p>
 *   Only basic "is it working at all" tests are done, since the library itself has its own set
 *   of unit tests.
 * </p>
 */
public class TestOSHDBFilter {
  private final OSHDBJdbc oshdb;
  private final FilterParser filterParser;

  private final OSHDBBoundingBox bbox =
      new OSHDBBoundingBox(8.651133, 49.387611, 8.6561, 49.390513);

  /**
   * Creates a test runner using the H2 backend.
   *
   * @throws Exception if something goes wrong.
   */
  public TestOSHDBFilter() throws Exception {
    OSHDBH2 oshdb = new OSHDBH2("./src/test/resources/test-data");
    filterParser = new FilterParser(new TagTranslator(oshdb.getConnection()));
    this.oshdb = oshdb;
  }

  private MapReducer<OSMEntitySnapshot> createMapReducerOSMEntitySnapshot() throws Exception {
    return OSMEntitySnapshotView.on(oshdb)
        .areaOfInterest(bbox)
        .timestamps("2014-01-01");
  }

  private MapReducer<OSMContribution> createMapReducerOSMContribution() throws Exception {
    return OSMContributionView.on(oshdb)
        .areaOfInterest(bbox)
        .timestamps("2008-01-01", "2014-01-01");
  }

  @Test
  public void testFilterString() throws Exception {
    Number result = createMapReducerOSMEntitySnapshot()
        .map(x -> 1)
        .filter("type:way and geometry:polygon and building=*")
        .sum();

    assertEquals(42, result.intValue());

    result = createMapReducerOSMContribution()
        .map(x -> 1)
        .filter("type:way and geometry:polygon and building=*")
        .sum();

    assertEquals(42, result.intValue());
  }

  @Test
  public void testFilterObject() throws Exception {
    Number result = createMapReducerOSMEntitySnapshot()
        .filter(filterParser.parse("type:way and geometry:polygon and building=*"))
        .count();

    assertEquals(42, result.intValue());
  }

  @Test
  public void testAggregateFilter() throws Exception {
    SortedMap<OSMType, Integer> result = createMapReducerOSMEntitySnapshot()
        .aggregateBy(x -> x.getEntity().getType())
        .filter("(geometry:polygon or geometry:other) and building=*")
        .count();

    assertEquals(2, result.entrySet().size());
    assertEquals(42, result.get(OSMType.WAY).intValue());
    assertEquals(1, result.get(OSMType.RELATION).intValue());
  }

  @Test
  public void testAggregateFilterObject() throws Exception {
    SortedMap<OSMType, Integer> result = createMapReducerOSMEntitySnapshot()
        .aggregateBy(x -> x.getEntity().getType())
        .filter(filterParser.parse("(geometry:polygon or geometry:other) and building=*"))
        .count();

    assertEquals(42, result.get(OSMType.WAY).intValue());
  }

  @Test
  public void testFilterGroupByEntity() throws Exception {
    MapReducer<?> mr = createMapReducerOSMEntitySnapshot();
    Number osmTypeFilterResult = mr.groupByEntity().osmType(OSMType.WAY).count();
    Number stringFilterResult = mr.groupByEntity().filter("type:way").count();

    assertEquals(osmTypeFilterResult, stringFilterResult);

    mr = createMapReducerOSMContribution();
    osmTypeFilterResult = mr.groupByEntity().osmType(OSMType.WAY).count();
    stringFilterResult = mr.groupByEntity().filter("type:way").count();

    assertEquals(osmTypeFilterResult, stringFilterResult);
  }

  @Test
  @SuppressWarnings("ResultOfMethodCallIgnored")
  public void testFilterNonExistentTag() throws Exception {
    FilterParser parser = new FilterParser(new TagTranslator(oshdb.getConnection()));
    try {
      createMapReducerOSMEntitySnapshot()
          .filter(parser.parse("type:way and nonexistentkey=*"))
          .count();
      createMapReducerOSMContribution()
          .filter(parser.parse("type:way and nonexistentkey=nonexistentvalue"))
          .count();
    } catch (Exception e) {
      fail("should not crash on non-existent tags");
    }
  }
}
