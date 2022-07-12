package org.heigit.ohsome.oshdb.api.mapreducer.snapshot;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBJdbc;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.api.object.OSMEntitySnapshotImpl;
import org.heigit.ohsome.oshdb.util.celliterator.CellIterator;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.json.simple.parser.ParseException;
import org.locationtech.jts.geom.Polygon;

public class OSMEntitySnapshotView extends OSHDBView<OSMEntitySnapshotView> {


  protected OSMEntitySnapshotView(OSHDBDatabase oshdb, OSHDBJdbc keytables) {
    super(oshdb, keytables);
  }

  @Override
  protected OSMEntitySnapshotView instance() {
    return this;
  }

  public MapReducerSnapshot<OSMEntitySnapshot> view() throws IOException, ParseException {
    var cellIterator = new CellIterator(tstamps.get(), bboxFilter, (Polygon) polyFilter,
        getTagInterpreter(), getPreFilter(), getFilter(), false);
    return new MapReducerSnapshot<>(this,
        osh -> cellIterator.iterateByTimestamps(List.of(osh), false)
          .map(OSMEntitySnapshotImpl::new), Stream::of);
  }
}
