package org.heigit.bigspatialdata.oshdb.api.mapreducer.backend;

import java.io.IOException;
import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBiFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBinaryOperator;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableSupplier;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.object.OSHDBMapReducible;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.util.celliterator.CellIterator;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.jetbrains.annotations.NotNull;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapReducerJdbcMultithread<X> extends MapReducerJdbc<X> {
  private static final Logger LOG = LoggerFactory.getLogger(MapReducer.class);

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

  private interface Callback<S> extends Serializable {
    S apply (
        GridOSHEntity oshEntityCell,
        CellIterator cellIterator
    );
  }

  private <S> S run(
      Callback<S> callback,
      SerializableSupplier<S> identitySupplier,
      SerializableBinaryOperator<S> combiner
  ) throws ParseException, SQLException, IOException {
    CellIterator cellIterator = new CellIterator(
        this._tstamps.get(),
        this._bboxFilter, this._getPolyFilter(),
        this._getTagInterpreter(), this._getPreFilter(), this._getFilter(), false
    );

    final List<Pair<CellId, CellId>> cellIdRanges = new ArrayList<>();
    this._getCellIdRanges().forEach(cellIdRanges::add);

    return cellIdRanges.parallelStream()
        .flatMap(this::getOshCellsStream)
        .map(oshCell -> callback.apply(oshCell, cellIterator))
        .reduce(identitySupplier.get(), combiner);
  }

  @Override
  protected <R, S> S mapReduceCellsOSMContribution(SerializableFunction<OSMContribution, R> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    return this.run((oshEntityCell, cellIterator) -> {
      // iterate over the history of all OSM objects in the current cell
      AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
      cellIterator.iterateByContribution(oshEntityCell).forEach(contribution -> {
        OSMContribution osmContribution = new OSMContribution(contribution);
        accInternal.set(accumulator.apply(accInternal.get(), mapper.apply(osmContribution)));
      });
      return accInternal.get();
    }, identitySupplier, combiner);
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMContributionGroupedById(
      SerializableFunction<List<OSMContribution>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    return this.run((oshEntityCell, cellIterator) -> {
      // iterate over the history of all OSM objects in the current cell
      AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
      List<OSMContribution> contributions = new ArrayList<>();
      cellIterator.iterateByContribution(oshEntityCell).forEach(contribution -> {
        OSMContribution thisContribution = new OSMContribution(contribution);
        if (contributions.size() > 0 && thisContribution.getEntityAfter()
            .getId() != contributions.get(contributions.size() - 1).getEntityAfter().getId()) {
          // immediately fold the results
          for (R r : mapper.apply(contributions)) {
            accInternal.set(accumulator.apply(accInternal.get(), r));
          }
          contributions.clear();
        }
        contributions.add(thisContribution);
      });
      // apply mapper and fold results one more time for last entity in current cell
      if (contributions.size() > 0) {
        for (R r : mapper.apply(contributions)) {
          accInternal.set(accumulator.apply(accInternal.get(), r));
        }
      }
      return accInternal.get();
    }, identitySupplier, combiner);
  }

  @Override
  protected <R, S> S mapReduceCellsOSMEntitySnapshot(
      SerializableFunction<OSMEntitySnapshot, R> mapper, SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner)
      throws Exception {
    return this.run((oshEntityCell, cellIterator) -> {
      // iterate over the history of all OSM objects in the current cell
      AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
      cellIterator.iterateByTimestamps(oshEntityCell).forEach(data -> {
        OSMEntitySnapshot snapshot = new OSMEntitySnapshot(data);
        // immediately fold the result
        accInternal.set(accumulator.apply(accInternal.get(), mapper.apply(snapshot)));
      });
      return accInternal.get();
    }, identitySupplier, combiner);
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMEntitySnapshotGroupedById(
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    return this.run((oshEntityCell, cellIterator) -> {
      // iterate over the history of all OSM objects in the current cell
      AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
      List<OSMEntitySnapshot> osmEntitySnapshots = new ArrayList<>();
      cellIterator.iterateByTimestamps(oshEntityCell).forEach(data -> {
        OSMEntitySnapshot thisSnapshot = new OSMEntitySnapshot(data);
        if (osmEntitySnapshots.size() > 0
            && thisSnapshot.getEntity().getId() != osmEntitySnapshots
            .get(osmEntitySnapshots.size() - 1).getEntity().getId()) {
          // immediately fold the results
          for (R r : mapper.apply(osmEntitySnapshots)) {
            accInternal.set(accumulator.apply(accInternal.get(), r));
          }
          osmEntitySnapshots.clear();
        }
        osmEntitySnapshots.add(thisSnapshot);
      });
      // apply mapper and fold results one more time for last entity in current cell
      if (osmEntitySnapshots.size() > 0) {
        for (R r : mapper.apply(osmEntitySnapshots)) {
          accInternal.set(accumulator.apply(accInternal.get(), r));
        }
      }
      return accInternal.get();
    }, identitySupplier, combiner);
  }

  @Nonnull
  private Stream<? extends GridOSHEntity> getOshCellsStream(Pair<CellId, CellId> cellIdRange) {
    try {
      ResultSet oshCellsRawData = getOshCellsRawDataFromDb(cellIdRange);
      // iterate over the result
      List<GridOSHEntity> cellsData = new ArrayList<>();
      while (oshCellsRawData.next()) {
        GridOSHEntity oshCellRawData = readOshCellRawData(oshCellsRawData);
        cellsData.add(oshCellRawData);
      }
      return cellsData.stream();
    } catch (SQLException | IOException | ClassNotFoundException e) {
      e.printStackTrace();
      return Stream.empty();
    }
  }
}
