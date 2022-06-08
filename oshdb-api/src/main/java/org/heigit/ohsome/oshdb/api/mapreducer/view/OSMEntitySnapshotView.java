package org.heigit.ohsome.oshdb.api.mapreducer.view;

import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;

/**
 * Returns the state of OSM elements at specific given points in time.
 */
public class OSMEntitySnapshotView extends OSHDBView<OSMEntitySnapshot> {
  private OSMEntitySnapshotView() {}

  public static OSMEntitySnapshotView view() {
    return new OSMEntitySnapshotView();
  }

  @Override
  public MapReducer<OSMEntitySnapshot> on(OSHDBDatabase oshdb) {
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
//    return mapReducer
//        .flatMap(osh -> cellIterator.iterateByTimestamps(osh, false)) // filtering
//        .map(data -> (OSMEntitySnapshot) new OSMEntitySnapshotImpl(data));
    return oshdb.createMapReducer(this);
  }

  @Override
  public ViewType type() {
    return ViewType.SNAPSHOT;
  }
}
