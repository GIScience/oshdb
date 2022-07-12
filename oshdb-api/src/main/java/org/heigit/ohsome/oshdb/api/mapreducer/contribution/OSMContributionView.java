package org.heigit.ohsome.oshdb.api.mapreducer.contribution;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBJdbc;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.api.object.OSMContributionImpl;
import org.heigit.ohsome.oshdb.util.celliterator.CellIterator;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.json.simple.parser.ParseException;
import org.locationtech.jts.geom.Polygon;

public class OSMContributionView extends OSHDBView<OSMContributionView> {

  public OSMContributionView(OSHDBDatabase oshdb, OSHDBJdbc keytables) {
    super(oshdb, keytables);
  }

  @Override
  protected OSMContributionView instance() {
    return this;
  }

  public MapReducerContribution<OSMContribution> view() throws IOException, ParseException {
    var cellIterator = new CellIterator(tstamps.get(), bboxFilter, (Polygon) polyFilter,
        getTagInterpreter(), getPreFilter(), getFilter(), false);
    return new MapReducerContribution<>(this,
        osh -> cellIterator.iterateByContribution(List.of(osh), false)
          .map(OSMContributionImpl::new), Stream::of);
  }

}
