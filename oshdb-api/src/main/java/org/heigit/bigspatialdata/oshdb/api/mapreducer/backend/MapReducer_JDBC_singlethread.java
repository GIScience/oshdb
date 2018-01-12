package org.heigit.bigspatialdata.oshdb.api.mapreducer.backend;

import com.vividsolutions.jts.geom.Geometry;
import java.io.ObjectInputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_Database;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_H2;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBiFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBinaryOperator;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableSupplier;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.object.OSHDB_MapReducible;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.util.celliterator.CellIterator;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.TableNames;
import org.jetbrains.annotations.NotNull;

public class MapReducer_JDBC_singlethread<X> extends MapReducer<X> {
  public MapReducer_JDBC_singlethread(OSHDB_Database oshdb,
      Class<? extends OSHDB_MapReducible> forClass) {
    super(oshdb, forClass);
  }

  // copy constructor
  private MapReducer_JDBC_singlethread(MapReducer_JDBC_singlethread obj) {
    super(obj);
  }

  @NotNull
  @Override
  protected MapReducer<X> copy() {
    return new MapReducer_JDBC_singlethread<X>(this);
  }

  @Override
  protected <R, S> S mapReduceCellsOSMContribution(SerializableFunction<OSMContribution, R> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    S result = identitySupplier.get();
    for (CellId cellId : this._getCellIds()) {
      String sqlQuery = this._typeFilter.stream()
          .map(osmType -> TableNames.forOSMType(osmType)
              .map(tn -> tn.toString(this._oshdb.prefix())))
          .filter(Optional::isPresent).map(Optional::get)
          .map(tn -> "(select data from " + tn + " where level = ?1 and id = ?2)")
          .collect(Collectors.joining(" union all "));
      // fetch data from H2 DB
      PreparedStatement pstmt = ((OSHDB_H2) this._oshdb).getConnection().prepareStatement(sqlQuery);
      pstmt.setInt(1, cellId.getZoomLevel());
      pstmt.setLong(2, cellId.getId());

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
        CellIterator
            .iterateAll(oshCellRawData, this._bboxFilter, this._getPolyFilter(),
                new CellIterator.TimestampInterval(this._tstamps.getTimestamps().get(0),
                    this._tstamps.getTimestamps().get(this._tstamps.getTimestamps().size() - 1)),
                this._getTagInterpreter(), this._getPreFilter(), this._getFilter(), false)
            .forEach(contribution -> {
              OSMContribution osmContribution = new OSMContribution(
                  new OSHDBTimestamp(contribution.timestamp),
                  contribution.nextTimestamp != null
                      ? new OSHDBTimestamp(contribution.nextTimestamp) : null,
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
    S result = identitySupplier.get();
    for (CellId cellId : this._getCellIds()) {
      String sqlQuery = this._typeFilter.stream()
          .map(osmType -> TableNames.forOSMType(osmType)
              .map(tn -> tn.toString(this._oshdb.prefix())))
          .filter(Optional::isPresent).map(Optional::get)
          .map(tn -> "(select data from " + tn + " where level = ?1 and id = ?2)")
          .collect(Collectors.joining(" union all "));
      // fetch data from H2 DB
      PreparedStatement pstmt = ((OSHDB_H2) this._oshdb).getConnection().prepareStatement(sqlQuery);
      pstmt.setInt(1, cellId.getZoomLevel());
      pstmt.setLong(2, cellId.getId());

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
        CellIterator
            .iterateAll(oshCellRawData, this._bboxFilter, this._getPolyFilter(),
                new CellIterator.TimestampInterval(this._tstamps.getTimestamps().get(0),
                    this._tstamps.getTimestamps().get(this._tstamps.getTimestamps().size() - 1)),
                this._getTagInterpreter(), this._getPreFilter(), this._getFilter(), false)
            .forEach(contribution -> {
              OSMContribution thisContribution = new OSMContribution(
                  new OSHDBTimestamp(contribution.timestamp),
                  contribution.nextTimestamp != null
                      ? new OSHDBTimestamp(contribution.nextTimestamp) : null,
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
    S result = identitySupplier.get();
    for (CellId cellId : this._getCellIds()) {
      String sqlQuery = this._typeFilter.stream()
          .map(osmType -> TableNames.forOSMType(osmType)
              .map(tn -> tn.toString(this._oshdb.prefix())))
          .filter(Optional::isPresent).map(Optional::get)
          .map(tn -> "(select data from " + tn + " where level = ?1 and id = ?2)")
          .collect(Collectors.joining(" union all "));
      // fetch data from H2 DB
      PreparedStatement pstmt = ((OSHDB_H2) this._oshdb).getConnection().prepareStatement(sqlQuery);
      pstmt.setInt(1, cellId.getZoomLevel());
      pstmt.setLong(2, cellId.getId());

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
        CellIterator.iterateByTimestamps(oshCellRawData, this._bboxFilter, this._getPolyFilter(),
            this._tstamps.getTimestamps(), this._getTagInterpreter(), this._getPreFilter(),
            this._getFilter(), false).forEach(snapshots -> snapshots.forEach((key, value) -> {
              OSHDBTimestamp tstamp = new OSHDBTimestamp(key);
              Geometry geometry = value.getRight();
              OSMEntity entity = value.getLeft();
              OSMEntitySnapshot snapshot = new OSMEntitySnapshot(tstamp, geometry, entity);
              // immediately fold the result
              accInternal.set(accumulator.apply(accInternal.get(), mapper.apply(snapshot)));
            }));
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
    S result = identitySupplier.get();
    for (CellId cellId : this._getCellIds()) {
      String sqlQuery = this._typeFilter.stream()
          .map(osmType -> TableNames.forOSMType(osmType)
              .map(tn -> tn.toString(this._oshdb.prefix())))
          .filter(Optional::isPresent).map(Optional::get)
          .map(tn -> "(select data from " + tn + " where level = ?1 and id = ?2)")
          .collect(Collectors.joining(" union all "));
      // fetch data from H2 DB
      PreparedStatement pstmt = ((OSHDB_H2) this._oshdb).getConnection().prepareStatement(sqlQuery);
      pstmt.setInt(1, cellId.getZoomLevel());
      pstmt.setLong(2, cellId.getId());

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
        CellIterator.iterateByTimestamps(oshCellRawData, this._bboxFilter, this._getPolyFilter(),
            this._tstamps.getTimestamps(), this._getTagInterpreter(), this._getPreFilter(),
            this._getFilter(), false).forEach(snapshots -> {
              List<OSMEntitySnapshot> osmEntitySnapshots = new ArrayList<>(snapshots.size());
              snapshots.forEach((key, value) -> {
                OSHDBTimestamp tstamp = new OSHDBTimestamp(key);
                Geometry geometry = value.getRight();
                OSMEntity entity = value.getLeft();
                osmEntitySnapshots.add(new OSMEntitySnapshot(tstamp, geometry, entity));
              });
              // immediately fold the results
              for (R r : mapper.apply(osmEntitySnapshots)) {
                accInternal.set(accumulator.apply(accInternal.get(), r));
              }
            });
        result = accInternal.get();
      }
    }
    return combiner.apply(identitySupplier.get(), result);
  }
}
