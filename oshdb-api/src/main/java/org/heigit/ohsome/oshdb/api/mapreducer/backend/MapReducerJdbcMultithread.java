package org.heigit.ohsome.oshdb.api.mapreducer.backend;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.mapreducer.backend.Kernels.CellProcessor;
import org.heigit.ohsome.oshdb.api.mapreducer.base.MapReducerBase;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.index.XYGridTree.CellIdRange;
import org.heigit.ohsome.oshdb.util.celliterator.CellIterator;
import org.heigit.ohsome.oshdb.util.function.SerializableBinaryOperator;
import org.heigit.ohsome.oshdb.util.function.SerializableSupplier;
import org.jetbrains.annotations.NotNull;

/**
 * An implementation of the OSHDB API using a JDBC database as backend, where calculations run in
 * parallel.
 *
 * <p>This implementation uses JAVA's {@link Stream#parallel()} implementation to run some
 * operations concurrently.</p>
 */
public class MapReducerJdbcMultithread<X> extends MapReducerJdbc<X> {
  public MapReducerJdbcMultithread(OSHDBDatabase oshdb,
      OSHDBView<X> view) {
    super(oshdb, view);
  }

  // copy constructor
  private MapReducerJdbcMultithread(MapReducerJdbcMultithread obj) {
    super(obj);
  }

  @NotNull
  @Override
  protected MapReducerBase<X> copy() {
    return new MapReducerJdbcMultithread<X>(this);
  }

  @Override
  public boolean isCancelable() {
    return true;
  }

  @Override
  protected <S> S reduce(
      CellProcessor<S> processor,
      SerializableSupplier<S> identitySupplier,
      SerializableBinaryOperator<S> combiner
  ) {
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
        .map(oshCell -> processor.apply(oshCell, cellIterator))
        .reduce(identitySupplier.get(), combiner);
  }

  @Override
  protected Stream<X> stream(CellProcessor<Stream<X>> processor) {
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
        .flatMap(oshCell -> processor.apply(oshCell, cellIterator));
  }
}
