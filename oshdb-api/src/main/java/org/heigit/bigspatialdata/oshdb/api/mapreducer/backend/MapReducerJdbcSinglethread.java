package org.heigit.bigspatialdata.oshdb.api.mapreducer.backend;

import com.google.common.collect.Streams;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Stream;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBiFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBinaryOperator;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableSupplier;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.backend.Kernels.CellProcessor;
import org.heigit.bigspatialdata.oshdb.api.object.OSHDBMapReducible;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.index.XYGridTree.CellIdRange;
import org.heigit.bigspatialdata.oshdb.util.celliterator.CellIterator;
import org.jetbrains.annotations.NotNull;
import org.json.simple.parser.ParseException;

public class MapReducerJdbcSinglethread<X> extends MapReducerJdbc<X> {
  public MapReducerJdbcSinglethread(OSHDBDatabase oshdb,
      Class<? extends OSHDBMapReducible> forClass) {
    super(oshdb, forClass);
  }

  // copy constructor
  private MapReducerJdbcSinglethread(MapReducerJdbcSinglethread obj) {
    super(obj);
  }

  @NotNull
  @Override
  protected MapReducer<X> copy() {
    return new MapReducerJdbcSinglethread<X>(this);
  }

  @Override
  public boolean isCancelable() {
    return true;
  }

  private <S> S reduce(
      CellProcessor<S> cellProcessor,
      SerializableSupplier<S> identitySupplier,
      SerializableBinaryOperator<S> combiner
  ) throws ParseException, SQLException, IOException, ClassNotFoundException {
    this.executionStartTimeMillis = System.currentTimeMillis();

    CellIterator cellIterator = new CellIterator(
        this.tstamps.get(),
        this.bboxFilter, this.getPolyFilter(),
        this.getTagInterpreter(), this.getPreFilter(), this.getFilter(), false
    );

    S result = identitySupplier.get();
    if (this.typeFilter.isEmpty()) {
      return result;
    }
    for (CellIdRange cellIdRange : this.getCellIdRanges()) {
      ResultSet oshCellsRawData = getOshCellsRawDataFromDb(cellIdRange);

      while (oshCellsRawData.next()) {
        GridOSHEntity oshCellRawData = readOshCellRawData(oshCellsRawData);
        result = combiner.apply(
            result,
            cellProcessor.apply(oshCellRawData, cellIterator)
        );
      }
    }
    return result;
  }

  private Stream<X> stream(
      CellProcessor<Stream<X>> cellProcessor
  ) throws ParseException, SQLException, IOException, ClassNotFoundException {
    this.executionStartTimeMillis = System.currentTimeMillis();

    CellIterator cellIterator = new CellIterator(
        this.tstamps.get(),
        this.bboxFilter, this.getPolyFilter(),
        this.getTagInterpreter(), this.getPreFilter(), this.getFilter(), false
    );

    return Streams.stream(this.getCellIdRanges())
        .flatMap(this::getOshCellsStream)
        .flatMap(oshCellRawData -> cellProcessor.apply(oshCellRawData, cellIterator));
  }

  // === map-reduce operations ===

  @Override
  protected <R, S> S mapReduceCellsOSMContribution(
      SerializableFunction<OSMContribution, R> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner
  ) throws Exception {
    return this.reduce(
        Kernels.getOSMContributionCellReducer(
            mapper,
            identitySupplier,
            accumulator,
            this
        ),
        identitySupplier,
        combiner
    );
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMContributionGroupedById(
      SerializableFunction<List<OSMContribution>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner
  ) throws Exception {
    return this.reduce(
        Kernels.getOSMContributionGroupingCellReducer(
            mapper,
            identitySupplier,
            accumulator,
            this
        ),
        identitySupplier,
        combiner
    );
  }

  @Override
  protected <R, S> S mapReduceCellsOSMEntitySnapshot(
      SerializableFunction<OSMEntitySnapshot, R> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner
  ) throws Exception {
    return this.reduce(
        Kernels.getOSMEntitySnapshotCellReducer(
            mapper,
            identitySupplier,
            accumulator,
            this
        ),
        identitySupplier,
        combiner
    );
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMEntitySnapshotGroupedById(
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner
  ) throws Exception {
    return this.reduce(
        Kernels.getOSMEntitySnapshotGroupingCellReducer(
            mapper,
            identitySupplier,
            accumulator,
            this
        ),
        identitySupplier,
        combiner
    );
  }

  // === stream operations ===

  @Override
  protected Stream<X> mapStreamCellsOSMContribution(
      SerializableFunction<OSMContribution, X> mapper) throws Exception {
    return this.stream(Kernels.getOSMContributionCellStreamer(mapper, this));
  }

  @Override
  protected Stream<X> flatMapStreamCellsOSMContributionGroupedById(
      SerializableFunction<List<OSMContribution>, Iterable<X>> mapper
  ) throws Exception {
    return this.stream(Kernels.getOSMContributionGroupingCellStreamer(mapper, this));
  }

  @Override
  protected Stream<X> mapStreamCellsOSMEntitySnapshot(
      SerializableFunction<OSMEntitySnapshot, X> mapper) throws Exception {
    return this.stream(Kernels.getOSMEntitySnapshotCellStreamer(mapper, this));
  }

  @Override
  protected Stream<X> flatMapStreamCellsOSMEntitySnapshotGroupedById(
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<X>> mapper) throws Exception {
    return this.stream(Kernels.getOSMEntitySnapshotGroupingCellStreamer(mapper, this));
  }

}
