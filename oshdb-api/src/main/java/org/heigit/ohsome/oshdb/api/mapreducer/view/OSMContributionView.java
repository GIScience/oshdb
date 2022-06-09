package org.heigit.ohsome.oshdb.api.mapreducer.view;

import java.io.IOException;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBJdbc;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.api.mapreducer.improve.OSHDBJdbcImprove;
import org.heigit.ohsome.oshdb.api.object.OSMContributionImpl;
import org.heigit.ohsome.oshdb.api.object.OSMEntitySnapshotImpl;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.celliterator.CellIterator;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBException;
import org.heigit.ohsome.oshdb.util.function.OSHEntityFilter;
import org.heigit.ohsome.oshdb.util.function.OSMEntityFilter;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.json.simple.parser.ParseException;
import org.locationtech.jts.geom.Polygon;

/**
 * Returns all modifications to OSM elements within a given time period.
 */
public class OSMContributionView extends OSHDBView<OSMContribution> {
  private OSMContributionView(OSHDBDatabase oshdb, OSHDBJdbc keytables) {
    super(oshdb, keytables);
  }

  @Override
  public MapReducer<OSMContribution> view() {
    if (oshdb instanceof OSHDBJdbcImprove) {
      try {
        return improve((OSHDBJdbcImprove) oshdb);
      } catch (IOException | ParseException e) {
        throw new OSHDBException(e);
      }
    }
    return oshdb.createMapReducer(this);
  }

  private MapReducer<OSMContribution> improve(OSHDBJdbcImprove oshdb) throws IOException, ParseException {
    // OSHEntityFilter preFilter = x -> true;
    // OSMEntityFilter filter = x -> true;
    // SortedSet<OSHDBTimestamp> timestamps = null;
    // OSHDBBoundingBox bbox = null;
    // Polygon poly = null;
    // TagInterpreter tagInterpreter2 = null;
    OSHEntityFilter preFilter =
        preFilters.isEmpty() ? x -> true : osh -> preFilters.stream().allMatch(f -> f.test(osh));
    OSMEntityFilter filter =
        filters.isEmpty() ? x -> true : osm -> filters.stream().allMatch(f -> f.test(osm));

    var cellIterator = new CellIterator(tstamps.get(), bboxFilter, (Polygon) polyFilter,
        getTagInterpreter(), preFilter, filter, false);

    MapReducer<OSHEntity> mapReducer = oshdb.createMapReducerImprove(this);
    return mapReducer.flatMap(osh -> cellIterator.iterateByContribution(osh, false)) // filtering
        .map(data -> (OSMContribution) new OSMContributionImpl(data));
  }

  public static OSMContributionView on(OSHDBDatabase oshdb, OSHDBJdbc keytables) {
    return new OSMContributionView(oshdb, keytables);
  }

  public static OSMContributionView on(OSHDBJdbc oshdb) {
    return new OSMContributionView(oshdb, oshdb);
  }

  @Override
  public ViewType type() {
    return ViewType.CONTRIBUTION;
  }
}
