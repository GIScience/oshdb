package org.heigit.ohsome.oshdb.api.mapreducer.backend;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.api.mapreducer.backend.Kernels.CellProcessor;
import org.heigit.ohsome.oshdb.index.XYGridTree.CellIdRange;
import org.heigit.ohsome.oshdb.util.celliterator.CellIterator;
import org.heigit.ohsome.oshdb.util.celliterator.OSHEntitySource;
import org.heigit.ohsome.oshdb.util.function.SerializableBiFunction;
import org.heigit.ohsome.oshdb.util.function.SerializableBinaryOperator;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.function.SerializableSupplier;
import org.heigit.ohsome.oshdb.util.mappable.OSHDBMapReducible;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.jetbrains.annotations.NotNull;
import org.json.simple.parser.ParseException;

/**
 * An implementation of the OSHDB API using a JDBC database as backend, where calculations run in
 * parallel.
 *
 * <p>This implementation uses JAVA's {@link Stream#parallel()} implementation to run some
 * operations concurrently.</p>
 */
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
  ) throws ParseException, IOException {
    this.executionStartTimeMillis = System.currentTimeMillis();

    CellIterator cellIterator = new CellIterator(
        this.tstamps.get(),
        this.bboxFilter, this.getPolyFilter(),
        this.getTagInterpreter(), this.getPreFilter(), this.getFilter(), false
    );

    final List<CellIdRange> cellIdRanges = new ArrayList<>();
    this.getCellIdRanges().forEach(cellIdRanges::add);

    return cellIdRanges.parallelStream()
        .filter(ignored -> this.isActive())
        .flatMap(this::getOshCellsStream)
        .filter(ignored -> this.isActive())
        .map(cell -> processor.apply(OSHEntitySource.fromGridOSHEntity(cell), cellIterator))
        .reduce(identitySupplier.get(), combiner);
  }

  private Stream<X> stream(
      CellProcessor<Stream<X>> processor
  ) throws ParseException, SQLException, IOException {
    this.executionStartTimeMillis = System.currentTimeMillis();

    CellIterator cellIterator = new CellIterator(
        this.tstamps.get(),
        this.bboxFilter, this.getPolyFilter(),
        this.getTagInterpreter(), this.getPreFilter(), this.getFilter(), false
    );

    final List<CellIdRange> cellIdRanges = new ArrayList<>();
    this.getCellIdRanges().forEach(cellIdRanges::add);

    return cellIdRanges.parallelStream()
        .filter(ignored -> this.isActive())
        .flatMap(this::getOshCellsStream)
        .filter(ignored -> this.isActive())
        .flatMap(cell -> processor.apply(OSHEntitySource.fromGridOSHEntity(cell), cellIterator));
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
