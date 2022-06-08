package org.heigit.ohsome.oshdb.api.mapreducer.view;

import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;

/**
 * Returns all modifications to OSM elements within a given time period.
 */
public class OSMContributionView extends OSHDBView<OSMContribution> {
  private OSMContributionView() {}

  public static OSMContributionView view() {
    return new OSMContributionView();
  }

  @Override
  public MapReducer<OSMContribution> on(OSHDBDatabase oshdb) {
//    OSHEntityFilter preFilter = x -> true;
//    OSMEntityFilter filter = x -> true;
//    SortedSet<OSHDBTimestamp> timestamps = null;
//    OSHDBBoundingBox bbox = null;
//    Polygon poly = null;
//    TagInterpreter tagInterpreter2 = null;
//
//    var cellIterator = new CellIterator(
//        timestamps,
//        bbox, poly,
//        tagInterpreter2, preFilter, filter, false);
//
//    MapReducer<OSHEntity> mapReducer = null; // oshdb.createMapReducer(this);
//    mapReducer
//        .flatMap(osh -> cellIterator.iterateByContribution(osh, false)) // filtering
//        .map(data -> (OSMContribution) new OSMContributionImpl(data));
    return oshdb.createMapReducer(this);
  }

  @Override
  public ViewType type() {
    return ViewType.CONTRIBUTION;
  }
}
