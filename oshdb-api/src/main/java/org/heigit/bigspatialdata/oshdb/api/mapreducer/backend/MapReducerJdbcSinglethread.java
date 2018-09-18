package org.heigit.bigspatialdata.oshdb.api.mapreducer.backend;

import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import java.io.IOException;
import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
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


  private interface Callback<S> extends Serializable {
    void apply (
        GridOSHEntity oshEntityCell,
        CellIterator cellIterator,
        AtomicReference<S> inputOutputReference
    );
  }
  private interface StreamCallback<X> extends Serializable {
    Stream<X> apply (
        GridOSHEntity oshEntityCell,
        CellIterator cellIterator
    );
  }

  private <S> S run(
      Callback<S> callback,
      SerializableSupplier<S> identitySupplier,
      SerializableBinaryOperator<S> combiner
  ) throws ParseException, SQLException, IOException, ClassNotFoundException {
    CellIterator cellIterator = new CellIterator(
        this._tstamps.get(),
        this._bboxFilter, this._getPolyFilter(),
        this._getTagInterpreter(), this._getPreFilter(), this._getFilter(), false
    );

    AtomicReference<S> result = new AtomicReference<>(identitySupplier.get());
    for (Pair<CellId, CellId> cellIdRange : this._getCellIdRanges()) {
      ResultSet oshCellsRawData = getOshCellsRawDataFromDb(cellIdRange);

      while (oshCellsRawData.next()) {
        GridOSHEntity oshCellRawData = readOshCellRawData(oshCellsRawData);
        callback.apply(oshCellRawData, cellIterator, result);
      }
    }
    return combiner.apply(identitySupplier.get(), result.get());
  }

  private Stream<X> run(
      StreamCallback<X> callback
  ) throws ParseException, SQLException, IOException, ClassNotFoundException {
    CellIterator cellIterator = new CellIterator(
        this._tstamps.get(),
        this._bboxFilter, this._getPolyFilter(),
        this._getTagInterpreter(), this._getPreFilter(), this._getFilter(), false
    );

    return Streams.stream(this._getCellIdRanges())
        .flatMap(this::getOshCellsStream)
        .flatMap(oshCellRawData -> callback.apply(oshCellRawData, cellIterator));
  }

  @Override
  protected <R, S> S mapReduceCellsOSMContribution(SerializableFunction<OSMContribution, R> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    return this.run((oshEntityCell, cellIterator, accInternal) -> {
      // iterate over the history of all OSM objects in the current cell
      cellIterator.iterateByContribution(oshEntityCell)
          .forEach(contribution -> {
            OSMContribution osmContribution = new OSMContribution(contribution);
            accInternal.set(accumulator.apply(accInternal.get(), mapper.apply(osmContribution)));
          });
    }, identitySupplier, combiner);
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMContributionGroupedById(
      SerializableFunction<List<OSMContribution>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    return this.run((oshEntityCell, cellIterator, accInternal) -> {
      // iterate over the history of all OSM objects in the current cell
      List<OSMContribution> contributions = new ArrayList<>();
      cellIterator.iterateByContribution(oshEntityCell)
          .forEach(contribution -> {
            OSMContribution thisContribution = new OSMContribution(contribution);
            if (contributions.size() > 0
                && thisContribution.getEntityAfter().getId() != contributions
                .get(contributions.size() - 1).getEntityAfter().getId()) {
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
    }, identitySupplier, combiner);
  }


  @Override
  protected <R, S> S mapReduceCellsOSMEntitySnapshot(
      SerializableFunction<OSMEntitySnapshot, R> mapper, SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner)
      throws Exception {
    return this.run((oshEntityCell, cellIterator, accInternal) -> {
      // iterate over the history of all OSM objects in the current cell
      cellIterator.iterateByTimestamps(oshEntityCell).forEach(data -> {
        OSMEntitySnapshot snapshot = new OSMEntitySnapshot(data);
        // immediately fold the result
        accInternal.set(accumulator.apply(accInternal.get(), mapper.apply(snapshot)));
      });
    }, identitySupplier, combiner);
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMEntitySnapshotGroupedById(
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    return this.run((oshEntityCell, cellIterator, accInternal) -> {
        // iterate over the history of all OSM objects in the current cell
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
    }, identitySupplier, combiner);
  }

  // === streams ===

  @Override
  protected Stream<X> mapStreamCellsOSMContribution(
      SerializableFunction<OSMContribution, X> mapper) throws Exception {
    return this.run((oshEntityCell, cellIterator) -> {
      // iterate over the history of all OSM objects in the current cell
      return cellIterator.iterateByContribution(oshEntityCell)
          .map(OSMContribution::new)
          .map(mapper);
    });
  }

  @Override
  protected Stream<X> flatMapStreamCellsOSMContributionGroupedById(
      SerializableFunction<List<OSMContribution>, Iterable<X>> mapper) throws Exception {
    return this.run((oshEntityCell, cellIterator) -> {
      // iterate over the history of all OSM objects in the current cell
      List<OSMContribution> contributions = new ArrayList<>();
      List<X> result = new LinkedList<>();
      cellIterator.iterateByContribution(oshEntityCell)
          .map(OSMContribution::new)
          .forEach(contribution -> {
            if (contributions.size() > 0 && contribution.getEntityAfter().getId()
                != contributions.get(contributions.size() - 1).getEntityAfter().getId()) {
              // immediately flatten the results
              Iterables.addAll(result, mapper.apply(contributions));
              contributions.clear();
            }
            contributions.add(contribution);
          });
      // apply mapper and fold results one more time for last entity in current cell
      if (contributions.size() > 0) {
        Iterables.addAll(result, mapper.apply(contributions));
      }
      return result.stream();
    });
  }

  @Override
  protected Stream<X> mapStreamCellsOSMEntitySnapshot(
      SerializableFunction<OSMEntitySnapshot, X> mapper) throws Exception {
    return this.run((oshEntityCell, cellIterator) -> {
      // iterate over the history of all OSM objects in the current cell
      return cellIterator.iterateByTimestamps(oshEntityCell)
          .map(OSMEntitySnapshot::new)
          .map(mapper);
    });
  }

  @Override
  protected Stream<X> flatMapStreamCellsOSMEntitySnapshotGroupedById(
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<X>> mapper) throws Exception {
    return this.run((oshEntityCell, cellIterator) -> {
      // iterate over the history of all OSM objects in the current cell
      List<OSMEntitySnapshot> snapshots = new ArrayList<>();
      List<X> result = new LinkedList<>();
      cellIterator.iterateByTimestamps(oshEntityCell)
          .map(OSMEntitySnapshot::new)
          .forEach(contribution -> {
            if (snapshots.size() > 0 && contribution.getEntity().getId()
                != snapshots.get(snapshots.size() - 1).getEntity().getId()) {
              // immediately flatten the results
              Iterables.addAll(result, mapper.apply(snapshots));
              snapshots.clear();
            }
            snapshots.add(contribution);
          });
      // apply mapper and fold results one more time for last entity in current cell
      if (snapshots.size() > 0) {
        Iterables.addAll(result, mapper.apply(snapshots));
      }
      return result.stream();
    });
  }

}
