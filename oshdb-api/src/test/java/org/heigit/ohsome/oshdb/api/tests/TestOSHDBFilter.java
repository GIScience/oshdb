package org.heigit.ohsome.oshdb.api.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Map;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.api.db.OSHDBH2;
import org.heigit.ohsome.oshdb.api.db.OSHDBJdbc;
import org.heigit.ohsome.oshdb.api.mapreducer.contribution.OSMContributionView;
import org.heigit.ohsome.oshdb.api.mapreducer.reduction.Reduce;
import org.heigit.ohsome.oshdb.api.mapreducer.snapshot.OSMEntitySnapshotView;
import org.heigit.ohsome.oshdb.filter.FilterExpression;
import org.heigit.ohsome.oshdb.filter.FilterParser;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMType;
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
    OSHDBH2 oshdb = new OSHDBH2("../data/test-data");
    filterParser = new FilterParser(new TagTranslator(oshdb.getConnection()));
    this.oshdb = oshdb;
  }

  private OSMEntitySnapshotView createMapReducerOSMEntitySnapshot() throws Exception {
    return OSMEntitySnapshotView.view()
        .areaOfInterest(bbox)
        .timestamps("2014-01-01");
  }

  private OSMContributionView createMapReducerOSMContribution() throws Exception {
    return OSMContributionView.view()
        .areaOfInterest(bbox)
        .timestamps("2008-01-01", "2014-01-01");
  }

  @Test
  void testFilterString() throws Exception {
    Number result = createMapReducerOSMEntitySnapshot()
        .filter("type:way and geometry:polygon and building=*")
        .on(oshdb)
        .map(x -> 1)
        .reduce(Reduce::sumInt);


    assertEquals(42, result.intValue());

    result = createMapReducerOSMContribution()
        .filter("type:way and geometry:polygon and building=*")
        .on(oshdb)
        .map(x -> 1)
        .reduce(Reduce::sumInt);

    assertEquals(42, result.intValue());
  }

  @Test
  void testFilterObject() throws Exception {
    Number result = createMapReducerOSMEntitySnapshot()
        .filter(filterParser.parse("type:way and geometry:polygon and building=*"))
        .on(oshdb)
        .count();

    assertEquals(42, result.intValue());
  }

  @Test
  void testAggregateFilter() throws Exception {
    Map<OSMType, Long> result = createMapReducerOSMEntitySnapshot()
        .filter("(geometry:polygon or geometry:other) and building=*")
        .on(oshdb)
        .aggregateBy(x -> x.getEntity().getType())
        .count();

    assertEquals(2, result.entrySet().size());
    assertEquals(42, result.get(OSMType.WAY).intValue());
    assertEquals(1, result.get(OSMType.RELATION).intValue());
  }

  @Test
  void testAggregateFilterObject() throws Exception {
    Map<OSMType, Long> result = createMapReducerOSMEntitySnapshot()
        .filter(filterParser.parse("(geometry:polygon or geometry:other) and building=*"))
        .on(oshdb)
        .aggregateBy(x -> x.getEntity().getType())
        .count();

    assertEquals(42, result.get(OSMType.WAY).intValue());
  }

  @Test
  void testFilterGroupByEntity() throws Exception {
    var mrSnapshot = createMapReducerOSMEntitySnapshot();
    Number osmTypeFilterResult = mrSnapshot
        .on(oshdb)
        .groupByEntity()
        .filter(x -> x.get(0).getEntity().getType() == OSMType.WAY)
        .count();
    Number stringFilterResult = mrSnapshot
        .filter("type:way")
        .on(oshdb)
        .groupByEntity()
        .count();

    assertEquals(osmTypeFilterResult, stringFilterResult);

    var mrContribution = createMapReducerOSMContribution();
    osmTypeFilterResult = mrContribution
        .on(oshdb)
        .groupByEntity()
        .filter(x -> x.get(0).getOSHEntity().getType() == OSMType.WAY)
        .count();
    stringFilterResult = mrContribution
        .filter("type:way")
        .on(oshdb)
        .groupByEntity()
        .count();

    assertEquals(osmTypeFilterResult, stringFilterResult);
  }

  @Test
  @SuppressWarnings("ResultOfMethodCallIgnored")
  void testFilterNonExistentTag() throws Exception {
    FilterParser parser = new FilterParser(new TagTranslator(oshdb.getConnection()));
    try {
      createMapReducerOSMEntitySnapshot()
          .filter(parser.parse("type:way and nonexistentkey=*"))
          .on(oshdb)
          .count();
      createMapReducerOSMContribution()
          .filter(parser.parse("type:way and nonexistentkey=nonexistentvalue"))
          .on(oshdb)
          .count();
    } catch (Exception e) {
      fail("should not crash on non-existent tags");
    }
  }

  @Test
  void testFilterNotCrashDuringNormalize() throws Exception {
    var mr = createMapReducerOSMContribution();
    mr = mr.filter(new FilterExpression() {
      @Override
      public boolean applyOSM(OSMEntity entity) {
        return false;
      }

      @Override
      public FilterExpression negate() {
        throw new RuntimeException("not implemented");
      }
    });
    assertEquals(0, mr.on(oshdb).count());
  }
}
