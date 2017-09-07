package org.heigit.bigspatialdata.oshdb.api.mapper;

import com.vividsolutions.jts.geom.Geometry;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.*;
import java.util.stream.Stream;

import com.vividsolutions.jts.geom.Polygon;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_H2;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.api.objects.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.*;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.DefaultTagInterpreter;

public class Mapper_H2_multithread<T> extends Mapper<T> {
  private TagTranslator _tagTranslator = null;

  protected Mapper_H2_multithread(OSHDB oshdb) {
    super(oshdb);
  }
  
  protected Integer getTagKeyId(String key) throws Exception {
    if (this._tagTranslator == null) this._tagTranslator = new TagTranslator(((OSHDB_H2) this._oshdbForTags).getConnection());
    return this._tagTranslator.key2Int(key);
  }
  
  protected Pair<Integer, Integer> getTagValueId(String key, String value) throws Exception {
    if (this._tagTranslator == null) this._tagTranslator = new TagTranslator(((OSHDB_H2) this._oshdbForTags).getConnection());
    return this._tagTranslator.tag2Int(new ImmutablePair(key,value));
  }
  
  @Override
  protected <R, S> S reduceCellsOSMContribution(Iterable<CellId> cellIds, List<Long> tstamps, BoundingBox bbox, Polygon poly, SerializablePredicate<OSHEntity> preFilter, SerializablePredicate<OSMEntity> filter, SerializableFunction<OSMContribution, R> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    //load tag interpreter helper which is later used for geometry building
    if (this._tagInterpreter == null) this._tagInterpreter = DefaultTagInterpreter.fromH2(((OSHDB_H2) this._oshdbForTags).getConnection());

    final List<CellId> cellIdsList = new ArrayList<>();
    cellIds.forEach(cellIdsList::add);

    return cellIdsList.parallelStream()
    .flatMap(cell -> {
      try {
        // fetch data from H2 DB
        PreparedStatement pstmt = ((OSHDB_H2) this._oshdb).getConnection().prepareStatement(
            (this._typeFilter.contains(OSMType.NODE) ? "(select data from grid_node where level = ?1 and id = ?2)" : "(select 0 as data where false)" ) +
                " union all " +
                (this._typeFilter.contains(OSMType.WAY) ? "(select data from grid_way where level = ?1 and id = ?2)" : "(select 0 as data where false)" ) +
                " union all " +
                (this._typeFilter.contains(OSMType.RELATION) ? "(select data from grid_relation where level = ?1 and id = ?2)" : "(select 0 as data where false)" )
        );
        pstmt.setInt(1, cell.getZoomLevel());
        pstmt.setLong(2, cell.getId());
        ResultSet oshCellsRawData = pstmt.executeQuery();

        // iterate over the result
        List<GridOSHEntity> cellsData = new ArrayList<>();
        while (oshCellsRawData.next()) {
          // get one cell from the raw data stream
          GridOSHEntity oshCellRawData = (GridOSHEntity) (new ObjectInputStream(oshCellsRawData.getBinaryStream(1))).readObject();
          cellsData.add(oshCellRawData);
        }
        return cellsData.stream();
      } catch (SQLException | IOException | ClassNotFoundException e) {
        e.printStackTrace();
        return Stream.empty();
      }
    }).map(oshCell -> {

      // iterate over the history of all OSM objects in the current cell
      List<R> rs = new ArrayList<>();
      CellIterator.iterateAll(
          oshCell,
          bbox,
          poly,
          new CellIterator.TimestampInterval(tstamps.get(0), tstamps.get(tstamps.size()-1)),
          this._tagInterpreter,
          preFilter,
          filter,
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

      // todo: replace this with `rs.stream().reduce(identitySupplier, accumulator, combiner);` (needs accumulator to be non-interfering and stateless, see http://download.java.net/java/jdk9/docs/api/java/util/stream/Stream.html#reduce-U-java.util.function.BiFunction-java.util.function.BinaryOperator-)
      S accInternal = identitySupplier.get();
      // fold the results
      for (R r : rs) {
        accInternal = accumulator.apply(accInternal, r);
      }
      return accInternal;
    }).reduce(identitySupplier.get(), (cur, acc) -> {
      return combiner.apply(acc, cur);
    });
  }

  
  @Override
  protected <R, S> S reduceCellsOSMEntitySnapshot(Iterable<CellId> cellIds, List<Long> tstamps, BoundingBox bbox, Polygon poly, SerializablePredicate<OSHEntity> preFilter, SerializablePredicate<OSMEntity> filter, SerializableFunction<OSMEntitySnapshot, R> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    //load tag interpreter helper which is later used for geometry building
    if (this._tagInterpreter == null)
      this._tagInterpreter = DefaultTagInterpreter.fromH2(((OSHDB_H2) this._oshdbForTags).getConnection());

    final List<CellId> cellIdsList = new ArrayList<>();
    cellIds.forEach(cellIdsList::add);

    return cellIdsList.parallelStream()
    .flatMap(cell -> {
      try {
        // fetch data from H2 DB
        PreparedStatement pstmt = ((OSHDB_H2) this._oshdb).getConnection().prepareStatement(
            (this._typeFilter.contains(OSMType.NODE) ? "(select data from grid_node where level = ?1 and id = ?2)" : "(select 0 as data where false)" ) +
                " union all " +
                (this._typeFilter.contains(OSMType.WAY) ? "(select data from grid_way where level = ?1 and id = ?2)" : "(select 0 as data where false)" ) +
                " union all " +
                (this._typeFilter.contains(OSMType.RELATION) ? "(select data from grid_relation where level = ?1 and id = ?2)" : "(select 0 as data where false)" )
        );
        pstmt.setInt(1, cell.getZoomLevel());
        pstmt.setLong(2, cell.getId());
        ResultSet oshCellsRawData = pstmt.executeQuery();

        // iterate over the result
        List<GridOSHEntity> cellsData = new ArrayList<>();
        while (oshCellsRawData.next()) {
          // get one cell from the raw data stream
          GridOSHEntity oshCellRawData = (GridOSHEntity) (new ObjectInputStream(oshCellsRawData.getBinaryStream(1))).readObject();
          cellsData.add(oshCellRawData);
        }
        return cellsData.stream();
      } catch (SQLException | IOException | ClassNotFoundException e) {
        e.printStackTrace();
        return Stream.empty();
      }
    }).map(oshCell -> {
      // iterate over the history of all OSM objects in the current cell
      List<R> rs = new ArrayList<>();
      CellIterator.iterateByTimestamps(
          oshCell,
          bbox,
          poly,
          tstamps,
          this._tagInterpreter,
          preFilter,
          filter,
          false
      ).forEach(result -> result.entrySet().forEach(entry -> {
        OSHDBTimestamp tstamp = new OSHDBTimestamp(entry.getKey());
        Geometry geometry = entry.getValue().getRight();
        OSMEntity entity = entry.getValue().getLeft();
        rs.add(mapper.apply(new OSMEntitySnapshot(tstamp, geometry, entity)));
      }));

      // todo: replace this with `rs.stream().reduce(identitySupplier, accumulator, combiner);` (needs accumulator to be non-interfering and stateless, see http://download.java.net/java/jdk9/docs/api/java/util/stream/Stream.html#reduce-U-java.util.function.BiFunction-java.util.function.BinaryOperator-)
      S accInternal = identitySupplier.get();
      // fold the results
      for (R r : rs) {
        accInternal = accumulator.apply(accInternal, r);
      }
      return accInternal;
    }).reduce(identitySupplier.get(), (cur, acc) -> {
      return combiner.apply(acc, cur);
    });
  }
}
