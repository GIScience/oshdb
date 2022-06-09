package org.heigit.ohsome.oshdb.api.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.SortedMap;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.api.db.OSHDBH2;
import org.heigit.ohsome.oshdb.api.db.OSHDBJdbc;
import org.heigit.ohsome.oshdb.api.mapreducer.aggregation.Agg;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSMContributionView;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSMEntitySnapshotView;
import org.heigit.ohsome.oshdb.filter.FilterExpression;
import org.heigit.ohsome.oshdb.filter.FilterParser;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.heigit.ohsome.oshdb.util.tagtranslator.TagTranslator;
import org.junit.jupiter.api.Test;

/**
 * Tests integration of oshdb-filter library.
 *
 * <p>
 *   Only basic "is it working at all" tests are done, since the library itself has its own set
 *   of unit tests.
 * </p>
 */
class TestOSHDBFilter {
  private final OSHDBJdbc oshdb;
  private final FilterParser filterParser;

  private final OSHDBBoundingBox bbox =
      OSHDBBoundingBox.bboxWgs84Coordinates(8.651133, 49.387611, 8.6561, 49.390513);

  /**
   * Creates a test runner using the H2 backend.
   *
   * @throws Exception if something goes wrong.
   */
  TestOSHDBFilter() throws Exception {
    OSHDBH2 oshdb = new OSHDBH2("./src/test/resources/test-data");
    filterParser = new FilterParser(new TagTranslator(oshdb.getConnection()));
    this.oshdb = oshdb;
  }

  private OSHDBView<OSMEntitySnapshot> createMapReducerOSMEntitySnapshot() throws Exception {
    return OSMEntitySnapshotView.on(oshdb)
        .areaOfInterest(bbox)
        .timestamps("2014-01-01");
  }

  private OSHDBView<OSMContribution> createMapReducerOSMContribution() throws Exception {
    return OSMContributionView.on(oshdb)
        .areaOfInterest(bbox)
        .timestamps("2008-01-01", "2014-01-01");
  }

  @Test
  void testFilterString() throws Exception {
    Number result = createMapReducerOSMEntitySnapshot()
        .filter("type:way and geometry:polygon and building=*")
        .view()
        .map(x -> 1)
        .aggregate(Agg::sumInt);


    assertEquals(42, result.intValue());

    result = createMapReducerOSMContribution()
        .filter("type:way and geometry:polygon and building=*")
        .view()
        .map(x -> 1)
        .aggregate(Agg::sumInt);

    assertEquals(42, result.intValue());
  }

  @Test
  void testFilterObject() throws Exception {
    Number result = createMapReducerOSMEntitySnapshot()
        .filter(filterParser.parse("type:way and geometry:polygon and building=*"))
        .view()
        .count();

    assertEquals(42, result.intValue());
  }

  @Test
  void testAggregateFilter() throws Exception {
    SortedMap<OSMType, Long> result = createMapReducerOSMEntitySnapshot()
        .filter("(geometry:polygon or geometry:other) and building=*")
        .view()
        .aggregateBy(x -> x.getEntity().getType())
        .count();

    assertEquals(2, result.entrySet().size());
    assertEquals(42, result.get(OSMType.WAY).intValue());
    assertEquals(1, result.get(OSMType.RELATION).intValue());
  }

  @Test
  void testAggregateFilterObject() throws Exception {
    SortedMap<OSMType, Long> result = createMapReducerOSMEntitySnapshot()
        .filter(filterParser.parse("(geometry:polygon or geometry:other) and building=*"))
        .view()
        .aggregateBy(x -> x.getEntity().getType())
        .count();

    assertEquals(42, result.get(OSMType.WAY).intValue());
  }

  @Test
  @SuppressWarnings("ResultOfMethodCallIgnored")
  void testFilterNonExistentTag() throws Exception {
    FilterParser parser = new FilterParser(new TagTranslator(oshdb.getConnection()));
    try {
      createMapReducerOSMEntitySnapshot()
          .filter(parser.parse("type:way and nonexistentkey=*"))
          .view()
          .count();
      createMapReducerOSMContribution()
          .filter(parser.parse("type:way and nonexistentkey=nonexistentvalue"))
          .view()
          .count();
    } catch (Exception e) {
      fail("should not crash on non-existent tags");
    }
  }

  @Test
  void testFilterNotCrashDuringNormalize() throws Exception {
    var mr = createMapReducerOSMContribution().filter(new FilterExpression() {
      @Override
      public boolean applyOSM(OSMEntity entity) {
        return false;
      }

      @Override
      public FilterExpression negate() {
        throw new RuntimeException("not implemented");
      }
    }).view();
    assertEquals(0, (long) mr.count());
  }
}
