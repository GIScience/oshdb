package org.heigit.ohsome.oshdb.api.mapreducer.view;

import java.io.IOException;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBJdbc;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.api.mapreducer.improve.OSHDBJdbcImprove;
import org.heigit.ohsome.oshdb.api.object.OSMEntitySnapshotImpl;
import org.heigit.ohsome.oshdb.util.celliterator.CellIterator;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.json.simple.parser.ParseException;
import org.locationtech.jts.geom.Polygon;

/**
 * Returns the state of OSM elements at specific given points in time.
 */
public class OSMEntitySnapshotView extends OSHDBView<OSMEntitySnapshot> {
  private OSMEntitySnapshotView(OSHDBDatabase oshdb, OSHDBJdbc keytables) {
    super(oshdb, keytables);
  }

  @Override
  public MapReducer<OSMEntitySnapshot> view() {
    return oshdb.createMapReducer(this);
  }

  public static OSMEntitySnapshotView on(OSHDBDatabase oshdb, OSHDBJdbc keytables) {
    return new OSMEntitySnapshotView(oshdb, keytables);
  }

  public static OSMEntitySnapshotView on(OSHDBDatabase oshdb) {
    return new OSMEntitySnapshotView(oshdb, null);
  }

  private MapReducer<OSMEntitySnapshot> improve(OSHDBJdbcImprove oshdb)
      throws IOException, ParseException {
    var cellIterator = new CellIterator(tstamps.get(), bboxFilter, (Polygon) polyFilter,
        getTagInterpreter(), getPreFilter(), getFilter(), false);

    return oshdb.createMapReducerImprove(this)
        .flatMap(osh -> cellIterator.iterateByTimestamps(osh, false))
        .map(data -> (OSMEntitySnapshot) new OSMEntitySnapshotImpl(data));
  }

  @Override
  public ViewType type() {
    return ViewType.SNAPSHOT;
  }
}
