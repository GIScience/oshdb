package org.heigit.ohsome.oshdb.api.mapreducer.snapshot;

import java.io.IOException;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBJdbc;
import org.heigit.ohsome.oshdb.api.db.OSHDBJdbcImprove;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.api.object.OSMEntitySnapshotImpl;
import org.heigit.ohsome.oshdb.util.celliterator.CellIterator;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBException;
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
    if (oshdb instanceof OSHDBJdbcImprove) {
      try {
        return improve((OSHDBJdbcImprove) oshdb);
      } catch (IOException | ParseException e) {
        throw new OSHDBException(e);
      }
    }
    return oshdb.createMapReducer(this);
  }

  private MapReducerSnapshot<OSMEntitySnapshot> improve(OSHDBJdbcImprove oshdb)
      throws IOException, ParseException {
    var cellIterator = new CellIterator(tstamps.get(), bboxFilter, (Polygon) polyFilter,
        getTagInterpreter(), getPreFilter(), getFilter(), false);

    return new MapReducerSnapshot<>(oshdb, this,
        osh -> cellIterator.iterateByTimestamps(osh, false)
          .map(data -> (OSMEntitySnapshot) new OSMEntitySnapshotImpl(data)),
        Stream::of);
  }

  public static OSMEntitySnapshotView on(OSHDBDatabase oshdb, OSHDBJdbc keytables) {
    return new OSMEntitySnapshotView(oshdb, keytables);
  }

  public static OSMEntitySnapshotView on(OSHDBDatabase oshdb) {
    return new OSMEntitySnapshotView(oshdb, null);
  }



  @Override
  public ViewType type() {
    return ViewType.SNAPSHOT;
  }
}
