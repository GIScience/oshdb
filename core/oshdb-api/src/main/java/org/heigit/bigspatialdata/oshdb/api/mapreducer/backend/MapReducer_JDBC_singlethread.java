package org.heigit.bigspatialdata.oshdb.api.mapreducer.backend;

import com.vividsolutions.jts.geom.Geometry;
import java.io.ObjectInputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_H2;
import org.heigit.bigspatialdata.oshdb.api.generic.lambdas.SerializableBiFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.lambdas.SerializableBinaryOperator;
import org.heigit.bigspatialdata.oshdb.api.generic.lambdas.SerializableFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.lambdas.SerializableSupplier;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.api.objects.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.util.CellIterator;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.DefaultTagInterpreter;

public class MapReducer_JDBC_singlethread<X> extends MapReducer<X> {
  public MapReducer_JDBC_singlethread(OSHDB oshdb) {
    super(oshdb);
  }

  // copy constructor
  public MapReducer_JDBC_singlethread(MapReducer_JDBC_singlethread obj) {
    super(obj);
  }

  @Override
  protected <R, S> S mapReduceCellsOSMContribution(SerializableFunction<OSMContribution, R> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    //load tag interpreter helper which is later used for geometry building
    if (this._tagInterpreter == null) this._tagInterpreter = DefaultTagInterpreter.fromJDBC(((OSHDB_H2) this._oshdbForTags).getConnection());

    S result = identitySupplier.get();
    for (CellId cellId : this._getCellIds()) {
      // prepare SQL statement
      PreparedStatement pstmt = ((OSHDB_H2) this._oshdb).getConnection().prepareStatement(
          (this._typeFilter.contains(OSMType.NODE) ? "(select data from grid_node where level = ?1 and id = ?2)" : "(select 0 as data where false)" ) +
              " union all " +
              (this._typeFilter.contains(OSMType.WAY) ? "(select data from grid_way where level = ?1 and id = ?2)" : "(select 0 as data where false)" ) +
              " union all " +
              (this._typeFilter.contains(OSMType.RELATION) ? "(select data from grid_relation where level = ?1 and id = ?2)" : "(select 0 as data where false)" )
      );
      pstmt.setInt(1, cellId.getZoomLevel());
      pstmt.setLong(2, cellId.getId());
      
      // execute statement
      ResultSet oshCellsRawData = pstmt.executeQuery();
      
      // iterate over the result
      while (oshCellsRawData.next()) {
        // get one cell from the raw data stream
        GridOSHEntity oshCellRawData = (GridOSHEntity) (new ObjectInputStream(oshCellsRawData.getBinaryStream(1))).readObject();

        // iterate over the history of all OSM objects in the current cell
        List<R> rs = new ArrayList<>();
        CellIterator.iterateAll(
            oshCellRawData,
            this._bboxFilter,
            this._getPolyFilter(),
            new CellIterator.TimestampInterval(this._tstamps.getTimestamps().get(0), this._tstamps.getTimestamps().get(this._tstamps.getTimestamps().size()-1)),
            this._tagInterpreter,
            this._getPreFilter(),
            this._getFilter(),
            false
        ).forEach(contribution -> rs.add(
            mapper.apply(
                new OSMContribution(
                    new OSHDBTimestamp(contribution.timestamp),
                    contribution.nextTimestamp != null ? new OSHDBTimestamp(contribution.nextTimestamp) : null,
                    contribution.previousGeometry,
                    contribution.geometry,
                    contribution.previousOsmEntity,
                    contribution.osmEntity,
                    contribution.activities
                )
            )
        ));
        
        // fold the results
        for (R r : rs) {
          result = accumulator.apply(result, r);
        }
      }
    }
    return combiner.apply(identitySupplier.get(), result);
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMContributionGroupedById(SerializableFunction<List<OSMContribution>, List<R>> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    //load tag interpreter helper which is later used for geometry building
    if (this._tagInterpreter == null) this._tagInterpreter = DefaultTagInterpreter.fromJDBC(((OSHDB_H2) this._oshdbForTags).getConnection());

    S result = identitySupplier.get();
    for (CellId cellId : this._getCellIds()) {
      // prepare SQL statement
      PreparedStatement pstmt = ((OSHDB_H2) this._oshdb).getConnection().prepareStatement(
          (this._typeFilter.contains(OSMType.NODE) ? "(select data from grid_node where level = ?1 and id = ?2)" : "(select 0 as data where false)" ) +
              " union all " +
              (this._typeFilter.contains(OSMType.WAY) ? "(select data from grid_way where level = ?1 and id = ?2)" : "(select 0 as data where false)" ) +
              " union all " +
              (this._typeFilter.contains(OSMType.RELATION) ? "(select data from grid_relation where level = ?1 and id = ?2)" : "(select 0 as data where false)" )
      );
      pstmt.setInt(1, cellId.getZoomLevel());
      pstmt.setLong(2, cellId.getId());

      // execute statement
      ResultSet oshCellsRawData = pstmt.executeQuery();

      // iterate over the result
      while (oshCellsRawData.next()) {
        // get one cell from the raw data stream
        GridOSHEntity oshCellRawData = (GridOSHEntity) (new ObjectInputStream(oshCellsRawData.getBinaryStream(1))).readObject();

        // iterate over the history of all OSM objects in the current cell
        List<R> rs = new ArrayList<>();
        List<OSMContribution> contributions = new ArrayList<>();
        CellIterator.iterateAll(
            oshCellRawData,
            this._bboxFilter,
            this._getPolyFilter(),
            new CellIterator.TimestampInterval(this._tstamps.getTimestamps().get(0), this._tstamps.getTimestamps().get(this._tstamps.getTimestamps().size()-1)),
            this._tagInterpreter,
            this._getPreFilter(),
            this._getFilter(),
            false
        ).forEach(contribution -> {
          OSMContribution thisContribution = new OSMContribution(
              new OSHDBTimestamp(contribution.timestamp),
              contribution.nextTimestamp != null ? new OSHDBTimestamp(contribution.nextTimestamp) : null,
              contribution.previousGeometry,
              contribution.geometry,
              contribution.previousOsmEntity,
              contribution.osmEntity,
              contribution.activities
          );
          if (contributions.size() > 0 && thisContribution.getEntityAfter().getId() != contributions.get(contributions.size()-1).getEntityAfter().getId()) {
            rs.addAll(mapper.apply(contributions));
            contributions.clear();
          }
          contributions.add(thisContribution);
        });
        // apply mapper one more time for last entity in current cell
        if (contributions.size() > 0)
          rs.addAll(mapper.apply(contributions));

        // fold the results
        for (R r : rs) {
          result = accumulator.apply(result, r);
        }
      }
    }
    return combiner.apply(identitySupplier.get(), result);
  }

  
  @Override
  protected <R, S> S mapReduceCellsOSMEntitySnapshot(SerializableFunction<OSMEntitySnapshot, R> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    //load tag interpreter helper which is later used for geometry building
    if (this._tagInterpreter == null) this._tagInterpreter = DefaultTagInterpreter.fromJDBC(((OSHDB_H2) this._oshdbForTags).getConnection());

    S result = identitySupplier.get();
    for (CellId cellId : this._getCellIds()) {
      // prepare SQL statement
      PreparedStatement pstmt = ((OSHDB_H2) this._oshdb).getConnection().prepareStatement(
          (this._typeFilter.contains(OSMType.NODE) ? "(select data from grid_node where level = ?1 and id = ?2)" : "(select 0 as data where false)" ) +
              " union all " +
              (this._typeFilter.contains(OSMType.WAY) ? "(select data from grid_way where level = ?1 and id = ?2)" : "(select 0 as data where false)" ) +
              " union all " +
              (this._typeFilter.contains(OSMType.RELATION) ? "(select data from grid_relation where level = ?1 and id = ?2)" : "(select 0 as data where false)" )
      );
      pstmt.setInt(1, cellId.getZoomLevel());
      pstmt.setLong(2, cellId.getId());
      
      // execute statement
      ResultSet oshCellsRawData = pstmt.executeQuery();
      
      // iterate over the result
      while (oshCellsRawData.next()) {
        // get one cell from the raw data stream
        GridOSHEntity oshCellRawData = (GridOSHEntity) (new ObjectInputStream(oshCellsRawData.getBinaryStream(1))).readObject();

        // iterate over the history of all OSM objects in the current cell
        List<R> rs = new LinkedList<>();
        CellIterator.iterateByTimestamps(
            oshCellRawData,
            this._bboxFilter,
            this._getPolyFilter(),
            this._tstamps.getTimestamps(),
            this._tagInterpreter,
            this._getPreFilter(),
            this._getFilter(),
            false
        ).forEach(snapshots -> snapshots.entrySet().forEach(entry -> {
          OSHDBTimestamp tstamp = new OSHDBTimestamp(entry.getKey());
          Geometry geometry = entry.getValue().getRight();
          OSMEntity entity = entry.getValue().getLeft();
          rs.add(mapper.apply(new OSMEntitySnapshot(tstamp, geometry, entity)));
        }));
        
        // fold the results
        for (R r : rs) {
          result = accumulator.apply(result, r);
        }
      }
    }
    return combiner.apply(identitySupplier.get(), result);
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMEntitySnapshotGroupedById(SerializableFunction<List<OSMEntitySnapshot>, List<R>> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    //load tag interpreter helper which is later used for geometry building
    if (this._tagInterpreter == null) this._tagInterpreter = DefaultTagInterpreter.fromJDBC(((OSHDB_H2) this._oshdbForTags).getConnection());

    S result = identitySupplier.get();
    for (CellId cellId : this._getCellIds()) {
      // prepare SQL statement
      PreparedStatement pstmt = ((OSHDB_H2) this._oshdb).getConnection().prepareStatement(
          (this._typeFilter.contains(OSMType.NODE) ? "(select data from grid_node where level = ?1 and id = ?2)" : "(select 0 as data where false)" ) +
              " union all " +
              (this._typeFilter.contains(OSMType.WAY) ? "(select data from grid_way where level = ?1 and id = ?2)" : "(select 0 as data where false)" ) +
              " union all " +
              (this._typeFilter.contains(OSMType.RELATION) ? "(select data from grid_relation where level = ?1 and id = ?2)" : "(select 0 as data where false)" )
      );
      pstmt.setInt(1, cellId.getZoomLevel());
      pstmt.setLong(2, cellId.getId());

      // execute statement
      ResultSet oshCellsRawData = pstmt.executeQuery();

      // iterate over the result
      while (oshCellsRawData.next()) {
        // get one cell from the raw data stream
        GridOSHEntity oshCellRawData = (GridOSHEntity) (new ObjectInputStream(oshCellsRawData.getBinaryStream(1))).readObject();

        // iterate over the history of all OSM objects in the current cell
        List<R> rs = new LinkedList<>();
        CellIterator.iterateByTimestamps(
            oshCellRawData,
            this._bboxFilter,
            this._getPolyFilter(),
            this._tstamps.getTimestamps(),
            this._tagInterpreter,
            this._getPreFilter(),
            this._getFilter(),
            false
        ).forEach(snapshots -> {
          List<OSMEntitySnapshot> osmEntitySnapshots = new ArrayList<>(snapshots.size());
          snapshots.entrySet().forEach(entry -> {
            OSHDBTimestamp tstamp = new OSHDBTimestamp(entry.getKey());
            Geometry geometry = entry.getValue().getRight();
            OSMEntity entity = entry.getValue().getLeft();
            osmEntitySnapshots.add(new OSMEntitySnapshot(tstamp, geometry, entity));
          });
          rs.addAll(mapper.apply(osmEntitySnapshots));
        });

        // fold the results
        for (R r : rs) {
          result = accumulator.apply(result, r);
        }
      }
    }
    return combiner.apply(identitySupplier.get(), result);
  }
}
