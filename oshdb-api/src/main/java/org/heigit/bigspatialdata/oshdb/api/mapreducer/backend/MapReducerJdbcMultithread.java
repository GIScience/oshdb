package org.heigit.bigspatialdata.oshdb.api.mapreducer.backend;

import com.google.common.collect.Streams;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import org.heigit.bigspatialdata.oshdb.index.XYGridTree.CellIdRange;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.celliterator.CellIterator;
import org.heigit.bigspatialdata.oshdb.util.update.UpdateDbHelper;
import org.jetbrains.annotations.NotNull;
import org.json.simple.parser.ParseException;
import org.roaringbitmap.longlong.LongBitmapDataProvider;


public class MapReducerJdbcMultithread<X> extends MapReducerJdbc<X> {
  public MapReducerJdbcMultithread(OSHDBDatabase oshdb,
      Class<? extends OSHDBMapReducible> forClass) {
    super(oshdb, forClass);
  }

  // copy constructor
  private MapReducerJdbcMultithread(MapReducerJdbcMultithread obj) {
    super(obj);
  }

  @NotNull
  @Override
  protected MapReducer<X> copy() {
    return new MapReducerJdbcMultithread<X>(this);
  }

  @Override
  public boolean isCancelable() {
    return true;
  }

  private <S> S reduce(
      CellProcessor<S> processor,
      SerializableSupplier<S> identitySupplier,
      SerializableBinaryOperator<S> combiner
  ) throws ParseException, SQLException, IOException, ClassNotFoundException {
    this.executionStartTimeMillis = System.currentTimeMillis();

    CellIterator cellIterator = new CellIterator(
        this.tstamps.get(),
        this.bboxFilter, this.getPolyFilter(),
        this.getTagInterpreter(), this.getPreFilter(), this.getFilter(), false
    );
    
    Stream<S> updateStream = Stream.empty();
    if (this.update != null) {
      // get bitmap of changed entities
      Map<OSMType, LongBitmapDataProvider> bitMapIndex = UpdateDbHelper.getBitMap(
          this.update.getBitArrayDb()
      );
      // create a second celliterator for updates, copy settings from first
      // because streams are lazy we have to have two celliterators and cannot change the first one
      CellIterator updateIterator = new CellIterator(
          this.tstamps.get(),
          this.bboxFilter, this.getPolyFilter(),
          this.getTagInterpreter(), this.getPreFilter(), this.getFilter(), false
      );
      // exclude updated entities in original data and include in updates
      cellIterator.excludeIDs(bitMapIndex);
      updateIterator.includeIDsOnly(bitMapIndex);
      // create a stream of updaten data
      updateStream = Streams.stream(this.getUpdates())
          .parallel()
          .filter(ignored -> this.isActive())
          .map(oshCell -> processor.apply(oshCell, updateIterator));
    }

    final List<CellIdRange> cellIdRanges = new ArrayList<>();
    this.getCellIdRanges().forEach(cellIdRanges::add);

    Stream<S> result = cellIdRanges.parallelStream()
        .filter(ignored -> this.isActive())
        .flatMap(this::getOshCellsStream)
        .filter(ignored -> this.isActive())
        .map(oshCell -> processor.apply(oshCell, cellIterator));
    
    return Streams.concat(result, updateStream)
        .reduce(identitySupplier.get(), combiner);
  }

  private Stream<X> stream(
      CellProcessor<Stream<X>> processor
  ) throws ParseException, SQLException, IOException, ClassNotFoundException {
    this.executionStartTimeMillis = System.currentTimeMillis();

    CellIterator cellIterator = new CellIterator(
        this.tstamps.get(),
        this.bboxFilter, this.getPolyFilter(),
        this.getTagInterpreter(), this.getPreFilter(), this.getFilter(), false
    );
    
    Stream<X> updateStream = Stream.empty();
    if (this.update != null) {
      // get bitmap of changed entities
      Map<OSMType, LongBitmapDataProvider> bitMapIndex = UpdateDbHelper.getBitMap(
          this.update.getBitArrayDb()
      );
      // create a second celliterator for updates, copy settings from first
      // because streams are lazy we have to have two celliterators and cannot change the first one
      CellIterator updateIterator = new CellIterator(
          this.tstamps.get(),
          this.bboxFilter, this.getPolyFilter(),
          this.getTagInterpreter(), this.getPreFilter(), this.getFilter(), false
      );
      // exclude updated entities in original data and include in updates
      cellIterator.excludeIDs(bitMapIndex);
      updateIterator.includeIDsOnly(bitMapIndex);
      // create a stream of updaten data
      updateStream = Streams.stream(this.getUpdates())
          .parallel()
          .filter(ignored -> this.isActive())
          .flatMap(oshCellRawData -> processor.apply(oshCellRawData, updateIterator));
    }

    final List<CellIdRange> cellIdRanges = new ArrayList<>();
    this.getCellIdRanges().forEach(cellIdRanges::add);

    Stream<X> result = cellIdRanges.parallelStream()
        .filter(ignored -> this.isActive())
        .flatMap(this::getOshCellsStream)
        .filter(ignored -> this.isActive())
        .flatMap(oshCell -> processor.apply(oshCell, cellIterator));

    return Streams.concat(result, updateStream);
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
    return reduce(
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
      SerializableFunction<List<OSMEntitySnapshot>,Iterable<R>> mapper,
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
      SerializableFunction<List<OSMContribution>, Iterable<X>> mapper) throws Exception {
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
