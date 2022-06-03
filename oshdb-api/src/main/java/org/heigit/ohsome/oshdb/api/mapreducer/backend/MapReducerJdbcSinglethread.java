package org.heigit.ohsome.oshdb.api.mapreducer.backend;

import com.google.common.collect.Streams;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.mapreducer.backend.Kernels.CellProcessor;
import org.heigit.ohsome.oshdb.api.mapreducer.base.MapReducerBase;
import org.heigit.ohsome.oshdb.grid.GridOSHEntity;
import org.heigit.ohsome.oshdb.index.XYGridTree.CellIdRange;
import org.heigit.ohsome.oshdb.util.celliterator.CellIterator;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBException;
import org.heigit.ohsome.oshdb.util.function.SerializableBinaryOperator;
import org.heigit.ohsome.oshdb.util.function.SerializableSupplier;
import org.heigit.ohsome.oshdb.util.mappable.OSHDBMapReducible;
import org.jetbrains.annotations.NotNull;

/**
 * A simple implementation of the OSHDB API using a JDBC database as backend, where calculations
 * are run sequentially.
 */
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
  protected MapReducerBase<X> copy() {
    return new MapReducerJdbcSinglethread<X>(this);
  }

  @Override
  public boolean isCancelable() {
    return true;
  }

  @Override
  protected <S> S reduce(
      CellProcessor<S> cellProcessor,
      SerializableSupplier<S> identitySupplier,
      SerializableBinaryOperator<S> combiner
  ) {
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
    try {
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
    } catch (ClassNotFoundException | SQLException | IOException e) {
      throw new OSHDBException(e);
    }
  }

  @Override
  protected Stream<X> stream(CellProcessor<Stream<X>> cellProcessor) {
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
}
