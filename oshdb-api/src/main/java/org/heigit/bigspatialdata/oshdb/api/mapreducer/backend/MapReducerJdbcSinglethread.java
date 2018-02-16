package org.heigit.bigspatialdata.oshdb.api.mapreducer.backend;

import java.io.ObjectInputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBiFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBinaryOperator;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableSupplier;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.object.OSHDBMapReducible;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.util.celliterator.CellIterator;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.TableNames;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestampInterval;
import org.jetbrains.annotations.NotNull;

public class MapReducerJdbcSinglethread<X> extends MapReducer<X> {
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
  protected <R, S> S mapReduceCellsOSMContribution(SerializableFunction<OSMContribution, R> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    CellIterator cellIterator = new CellIterator(
        this._bboxFilter, this._getPolyFilter(),
        this._getTagInterpreter(), this._getPreFilter(), this._getFilter(), false
    );
    OSHDBTimestampInterval timestampInterval = new OSHDBTimestampInterval(this._tstamps.get());

    S result = identitySupplier.get();
    for (Pair<CellId, CellId> cellIdRange : this._getCellIdRanges()) {
      String sqlQuery = this._typeFilter.stream()
          .map(osmType -> TableNames.forOSMType(osmType)
              .map(tn -> tn.toString(this._oshdb.prefix())))
          .filter(Optional::isPresent).map(Optional::get)
          .map(tn -> "(select data from " + tn + " where level = ?1 and id between ?2 and ?3)")
          .collect(Collectors.joining(" union all "));
      // fetch data from H2 DB
      PreparedStatement pstmt = ((OSHDBJdbc)this._oshdb).getConnection().prepareStatement(sqlQuery);
      pstmt.setInt(1, cellIdRange.getLeft().getZoomLevel());
      pstmt.setLong(2, cellIdRange.getLeft().getId());
      pstmt.setLong(3, cellIdRange.getRight().getId());

      // execute statement
      ResultSet oshCellsRawData = pstmt.executeQuery();

      // iterate over the result
      while (oshCellsRawData.next()) {
        // get one cell from the raw data stream
        GridOSHEntity oshCellRawData =
            (GridOSHEntity) (new ObjectInputStream(oshCellsRawData.getBinaryStream(1)))
                .readObject();

        // iterate over the history of all OSM objects in the current cell
        AtomicReference<S> accInternal = new AtomicReference<>(result);
        cellIterator.iterateByContribution(oshCellRawData, timestampInterval)
            .forEach(contribution -> {
              OSMContribution osmContribution = new OSMContribution(
                  contribution.timestamp,
                  contribution.previousGeometry, contribution.geometry,
                  contribution.previousOsmEntity, contribution.osmEntity, contribution.activities);
              accInternal.set(accumulator.apply(accInternal.get(), mapper.apply(osmContribution)));
            });
        result = accInternal.get();
      }
    }
    return combiner.apply(identitySupplier.get(), result);
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMContributionGroupedById(
      SerializableFunction<List<OSMContribution>, List<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    CellIterator cellIterator = new CellIterator(
        this._bboxFilter, this._getPolyFilter(),
        this._getTagInterpreter(), this._getPreFilter(), this._getFilter(), false
    );
    OSHDBTimestampInterval timestampInterval = new OSHDBTimestampInterval(this._tstamps.get());

    S result = identitySupplier.get();
    for (Pair<CellId, CellId> cellIdRange : this._getCellIdRanges()) {
      String sqlQuery = this._typeFilter.stream()
          .map(osmType -> TableNames.forOSMType(osmType)
              .map(tn -> tn.toString(this._oshdb.prefix())))
          .filter(Optional::isPresent).map(Optional::get)
          .map(tn -> "(select data from " + tn + " where level = ?1 and id between ?2 and ?3)")
          .collect(Collectors.joining(" union all "));
      // fetch data from H2 DB
      PreparedStatement pstmt = ((OSHDBJdbc)this._oshdb).getConnection().prepareStatement(sqlQuery);
      pstmt.setInt(1, cellIdRange.getLeft().getZoomLevel());
      pstmt.setLong(2, cellIdRange.getLeft().getId());
      pstmt.setLong(3, cellIdRange.getRight().getId());

      // execute statement
      ResultSet oshCellsRawData = pstmt.executeQuery();

      // iterate over the result
      while (oshCellsRawData.next()) {
        // get one cell from the raw data stream
        GridOSHEntity oshCellRawData =
            (GridOSHEntity) (new ObjectInputStream(oshCellsRawData.getBinaryStream(1)))
                .readObject();

        // iterate over the history of all OSM objects in the current cell
        AtomicReference<S> accInternal = new AtomicReference<>(result);
        List<OSMContribution> contributions = new ArrayList<>();
        cellIterator.iterateByContribution(oshCellRawData, timestampInterval)
            .forEach(contribution -> {
              OSMContribution thisContribution = new OSMContribution(
                  contribution.timestamp,
                  contribution.previousGeometry, contribution.geometry,
                  contribution.previousOsmEntity, contribution.osmEntity, contribution.activities);
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
        result = accInternal.get();
      }
    }
    return combiner.apply(identitySupplier.get(), result);
  }


  @Override
  protected <R, S> S mapReduceCellsOSMEntitySnapshot(
      SerializableFunction<OSMEntitySnapshot, R> mapper, SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner)
      throws Exception {
    CellIterator cellIterator = new CellIterator(
        this._bboxFilter, this._getPolyFilter(),
        this._getTagInterpreter(), this._getPreFilter(), this._getFilter(), false
    );
    SortedSet<OSHDBTimestamp> timestamps = this._tstamps.get();

    S result = identitySupplier.get();
    for (Pair<CellId, CellId> cellIdRange : this._getCellIdRanges()) {
      String sqlQuery = this._typeFilter.stream()
          .map(osmType -> TableNames.forOSMType(osmType)
              .map(tn -> tn.toString(this._oshdb.prefix())))
          .filter(Optional::isPresent).map(Optional::get)
          .map(tn -> "(select data from " + tn + " where level = ?1 and id between ?2 and ?3)")
          .collect(Collectors.joining(" union all "));
      // fetch data from H2 DB
      PreparedStatement pstmt = ((OSHDBJdbc)this._oshdb).getConnection().prepareStatement(sqlQuery);
      pstmt.setInt(1, cellIdRange.getLeft().getZoomLevel());
      pstmt.setLong(2, cellIdRange.getLeft().getId());
      pstmt.setLong(3, cellIdRange.getRight().getId());

      // execute statement
      ResultSet oshCellsRawData = pstmt.executeQuery();

      // iterate over the result
      while (oshCellsRawData.next()) {
        // get one cell from the raw data stream
        GridOSHEntity oshCellRawData =
            (GridOSHEntity) (new ObjectInputStream(oshCellsRawData.getBinaryStream(1)))
                .readObject();

        // iterate over the history of all OSM objects in the current cell
        AtomicReference<S> accInternal = new AtomicReference<>(result);
        cellIterator.iterateByTimestamps(oshCellRawData, timestamps).forEach(data -> {
          OSMEntitySnapshot snapshot = new OSMEntitySnapshot(data);
          // immediately fold the result
          accInternal.set(accumulator.apply(accInternal.get(), mapper.apply(snapshot)));
        });
        result = accInternal.get();
      }
    }
    return combiner.apply(identitySupplier.get(), result);
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMEntitySnapshotGroupedById(
      SerializableFunction<List<OSMEntitySnapshot>, List<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    CellIterator cellIterator = new CellIterator(
        this._bboxFilter, this._getPolyFilter(),
        this._getTagInterpreter(), this._getPreFilter(), this._getFilter(), false
    );
    SortedSet<OSHDBTimestamp> timestamps = this._tstamps.get();

    S result = identitySupplier.get();
    for (Pair<CellId, CellId> cellIdRange : this._getCellIdRanges()) {
      String sqlQuery = this._typeFilter.stream()
          .map(osmType -> TableNames.forOSMType(osmType)
              .map(tn -> tn.toString(this._oshdb.prefix())))
          .filter(Optional::isPresent).map(Optional::get)
          .map(tn -> "(select data from " + tn + " where level = ?1 and id between ?2 and ?3)")
          .collect(Collectors.joining(" union all "));
      // fetch data from H2 DB
      PreparedStatement pstmt = ((OSHDBJdbc)this._oshdb).getConnection().prepareStatement(sqlQuery);
      pstmt.setInt(1, cellIdRange.getLeft().getZoomLevel());
      pstmt.setLong(2, cellIdRange.getLeft().getId());
      pstmt.setLong(3, cellIdRange.getRight().getId());

      // execute statement
      ResultSet oshCellsRawData = pstmt.executeQuery();

      // iterate over the result
      while (oshCellsRawData.next()) {
        // get one cell from the raw data stream
        GridOSHEntity oshCellRawData =
            (GridOSHEntity) (new ObjectInputStream(oshCellsRawData.getBinaryStream(1)))
                .readObject();

        // iterate over the history of all OSM objects in the current cell
        AtomicReference<S> accInternal = new AtomicReference<>(result);
        List<OSMEntitySnapshot> osmEntitySnapshots = new ArrayList<>();
        cellIterator.iterateByTimestamps(oshCellRawData, timestamps).forEach(data -> {
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
        result = accInternal.get();
      }
    }
    return combiner.apply(identitySupplier.get(), result);
  }
}
