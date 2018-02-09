package org.heigit.bigspatialdata.oshdb.api.mapreducer.backend;

import com.vividsolutions.jts.geom.Geometry;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
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
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.celliterator.CellIterator.TimestampInterval;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.TableNames;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapReducer_JDBC_multithread<X> extends MapReducer<X> {
  private static final Logger LOG = LoggerFactory.getLogger(MapReducer.class);

  public MapReducer_JDBC_multithread(OSHDB_Database oshdb,
      Class<? extends OSHDB_MapReducible> forClass) {
    super(oshdb, forClass);
  }

  // copy constructor
  private MapReducer_JDBC_multithread(MapReducer_JDBC_multithread obj) {
    super(obj);
  }

  @NotNull
  @Override
  protected MapReducer<X> copy() {
    return new MapReducer_JDBC_multithread<X>(this);
  }

  @Override
  protected <R, S> S mapReduceCellsOSMContribution(SerializableFunction<OSMContribution, R> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    CellIterator cellIterator = new CellIterator(
        this._bboxFilter, this._getPolyFilter(),
        this._getTagInterpreter(), this._getPreFilter(), this._getFilter(), false
    );
    TimestampInterval timestampInterval = new CellIterator.TimestampInterval(
        this._tstamps.get().get(0),
        this._tstamps.get().get(this._tstamps.get().size() - 1)
    );

    final List<Pair<CellId, CellId>> cellIdRanges = new ArrayList<>();
    this._getCellIdRanges().forEach(cellIdRanges::add);

    return cellIdRanges.parallelStream().flatMap(cellIdRange -> {
      try {
        String sqlQuery = this._typeFilter.stream()
            .map(osmType -> TableNames.forOSMType(osmType)
                .map(tn -> tn.toString(this._oshdb.prefix())))
            .filter(Optional::isPresent).map(Optional::get)
            .map(tn -> "(select data from " + tn + " where level = ?1 and id between ?2 and ?3)")
            .collect(Collectors.joining(" union all "));
        // fetch data from H2 DB
        PreparedStatement pstmt =
            ((OSHDB_H2) this._oshdb).getConnection().prepareStatement(sqlQuery);
        pstmt.setInt(1, cellIdRange.getLeft().getZoomLevel());
        pstmt.setLong(2, cellIdRange.getLeft().getId());
        pstmt.setLong(3, cellIdRange.getRight().getId());

        ResultSet oshCellsRawData = pstmt.executeQuery();

        // iterate over the result
        List<GridOSHEntity> cellsData = new ArrayList<>();
        while (oshCellsRawData.next()) {
          // get one cell from the raw data stream
          GridOSHEntity oshCellRawData =
              (GridOSHEntity) (new ObjectInputStream(oshCellsRawData.getBinaryStream(1)))
                  .readObject();
          cellsData.add(oshCellRawData);
        }
        return cellsData.stream();
      } catch (SQLException | IOException | ClassNotFoundException e) {
        e.printStackTrace();
        return Stream.empty();
      }
    }).map(oshCell -> {
      // iterate over the history of all OSM objects in the current cell
      AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
      cellIterator.iterateAll(oshCell, timestampInterval)
          .forEach(contribution -> {
            OSMContribution osmContribution = new OSMContribution(
                contribution.timestamp,
                contribution.nextTimestamp,
                contribution.previousGeometry, contribution.geometry,
                contribution.previousOsmEntity, contribution.osmEntity, contribution.activities);
            accInternal.set(accumulator.apply(accInternal.get(), mapper.apply(osmContribution)));
          });
      return accInternal.get();
    }).reduce(identitySupplier.get(), combiner);
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
    TimestampInterval timestampInterval = new CellIterator.TimestampInterval(
        this._tstamps.get().get(0),
        this._tstamps.get().get(this._tstamps.get().size() - 1)
    );

    final List<Pair<CellId, CellId>> cellIdRanges = new ArrayList<>();
    this._getCellIdRanges().forEach(cellIdRanges::add);

    return cellIdRanges.parallelStream().flatMap(cellIdRange -> {
      try {
        String sqlQuery = this._typeFilter.stream()
            .map(osmType -> TableNames.forOSMType(osmType)
                .map(tn -> tn.toString(this._oshdb.prefix())))
            .filter(Optional::isPresent).map(Optional::get)
            .map(tn -> "(select data from " + tn + " where level = ?1 and id between ?2 and ?3)")
            .collect(Collectors.joining(" union all "));
        // fetch data from H2 DB
        PreparedStatement pstmt =
            ((OSHDB_H2) this._oshdb).getConnection().prepareStatement(sqlQuery);
        pstmt.setInt(1, cellIdRange.getLeft().getZoomLevel());
        pstmt.setLong(2, cellIdRange.getLeft().getId());
        pstmt.setLong(3, cellIdRange.getRight().getId());

        ResultSet oshCellsRawData = pstmt.executeQuery();

        // iterate over the result
        List<GridOSHEntity> cellsData = new ArrayList<>();
        while (oshCellsRawData.next()) {
          // get one cell from the raw data stream
          GridOSHEntity oshCellRawData =
              (GridOSHEntity) (new ObjectInputStream(oshCellsRawData.getBinaryStream(1)))
                  .readObject();
          cellsData.add(oshCellRawData);
        }
        return cellsData.stream();
      } catch (SQLException | IOException | ClassNotFoundException e) {
        e.printStackTrace();
        return Stream.empty();
      }
    }).map(oshCell -> {
      // iterate over the history of all OSM objects in the current cell
      AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
      List<OSMContribution> contributions = new ArrayList<>();
      cellIterator.iterateAll(oshCell, timestampInterval)
          .forEach(contribution -> {
            OSMContribution thisContribution = new OSMContribution(
                contribution.timestamp,
                contribution.nextTimestamp,
                contribution.previousGeometry, contribution.geometry,
                contribution.previousOsmEntity, contribution.osmEntity, contribution.activities);
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
    }).reduce(identitySupplier.get(), combiner);
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
    List<OSHDBTimestamp> timestamps = this._tstamps.get();

    final List<Pair<CellId, CellId>> cellIdRanges = new ArrayList<>();
    this._getCellIdRanges().forEach(cellIdRanges::add);

    return cellIdRanges.parallelStream().flatMap(cellIdRange -> {
      try {
        String sqlQuery = this._typeFilter.stream()
            .map(osmType -> TableNames.forOSMType(osmType)
                .map(tn -> tn.toString(this._oshdb.prefix())))
            .filter(Optional::isPresent).map(Optional::get)
            .map(tn -> "(select data from " + tn + " where level = ?1 and id between ?2 and ?3)")
            .collect(Collectors.joining(" union all "));
        // fetch data from H2 DB
        PreparedStatement pstmt =
            ((OSHDB_H2) this._oshdb).getConnection().prepareStatement(sqlQuery);
        pstmt.setInt(1, cellIdRange.getLeft().getZoomLevel());
        pstmt.setLong(2, cellIdRange.getLeft().getId());
        pstmt.setLong(3, cellIdRange.getRight().getId());
        ResultSet oshCellsRawData = pstmt.executeQuery();

        // iterate over the result
        List<GridOSHEntity> cellsData = new ArrayList<>();
        while (oshCellsRawData.next()) {
          // get one cell from the raw data stream
          GridOSHEntity oshCellRawData =
              (GridOSHEntity) (new ObjectInputStream(oshCellsRawData.getBinaryStream(1)))
                  .readObject();
          cellsData.add(oshCellRawData);
        }
        return cellsData.stream();
      } catch (SQLException | IOException | ClassNotFoundException e) {
        e.printStackTrace();
        return Stream.empty();
      }
    }).map(oshCell -> {
      // iterate over the history of all OSM objects in the current cell
      AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
      cellIterator.iterateByTimestamps(oshCell, timestamps)
          .forEach(result -> result.forEach((timestamp, value) -> {
            Geometry geometry = value.getRight();
            OSMEntity entity = value.getLeft();
            OSMEntitySnapshot snapshot = new OSMEntitySnapshot(timestamp, geometry, entity);
            // immediately fold the result
            accInternal.set(accumulator.apply(accInternal.get(), mapper.apply(snapshot)));
          }));
      return accInternal.get();
    }).reduce(identitySupplier.get(), combiner);
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
    List<OSHDBTimestamp> timestamps = this._tstamps.get();

    final List<Pair<CellId, CellId>> cellIdRanges = new ArrayList<>();
    this._getCellIdRanges().forEach(cellIdRanges::add);

    return cellIdRanges.parallelStream().flatMap(cellIdRange -> {
      try {
        String sqlQuery = this._typeFilter.stream()
            .map(osmType -> TableNames.forOSMType(osmType)
                .map(tn -> tn.toString(this._oshdb.prefix())))
            .filter(Optional::isPresent).map(Optional::get)
            .map(tn -> "(select data from " + tn + " where level = ?1 and id between ?2 and ?3)")
            .collect(Collectors.joining(" union all "));
        // fetch data from H2 DB
        PreparedStatement pstmt =
            ((OSHDB_H2) this._oshdb).getConnection().prepareStatement(sqlQuery);
        pstmt.setInt(1, cellIdRange.getLeft().getZoomLevel());
        pstmt.setLong(2, cellIdRange.getLeft().getId());
        pstmt.setLong(3, cellIdRange.getRight().getId());
        ResultSet oshCellsRawData = pstmt.executeQuery();

        // iterate over the result
        List<GridOSHEntity> cellsData = new ArrayList<>();
        while (oshCellsRawData.next()) {
          // get one cell from the raw data stream
          GridOSHEntity oshCellRawData =
              (GridOSHEntity) (new ObjectInputStream(oshCellsRawData.getBinaryStream(1)))
                  .readObject();
          cellsData.add(oshCellRawData);
        }
        return cellsData.stream();
      } catch (SQLException | IOException | ClassNotFoundException e) {
        e.printStackTrace();
        return Stream.empty();
      }
    }).map(oshCell -> {
      // iterate over the history of all OSM objects in the current cell
      AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
      cellIterator.iterateByTimestamps(oshCell, timestamps).forEach(snapshots -> {
            List<OSMEntitySnapshot> osmEntitySnapshots = new ArrayList<>(snapshots.size());
            snapshots.forEach((timestamp, value) -> {
              Geometry geometry = value.getRight();
              OSMEntity entity = value.getLeft();
              osmEntitySnapshots.add(new OSMEntitySnapshot(timestamp, geometry, entity));
            });
            // immediately fold the results
            for (R r : mapper.apply(osmEntitySnapshots)) {
              accInternal.set(accumulator.apply(accInternal.get(), r));
            }
          });
      return accInternal.get();
    }).reduce(identitySupplier.get(), (cur, acc) -> {
      return combiner.apply(acc, cur);
    });
  }
}
