package org.heigit.bigspatialdata.oshdb.api.mapreducer.backend;

import com.google.common.collect.Streams;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBiFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBinaryOperator;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableSupplier;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.backend.Kernels.CellProcessor;
import org.heigit.bigspatialdata.oshdb.api.object.OSHDBMapReducible;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHNodes;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHRelations;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHWays;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osh.OSHRelation;
import org.heigit.bigspatialdata.oshdb.osh.OSHWay;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.util.celliterator.CellIterator;
import org.heigit.bigspatialdata.oshdb.util.update.UpdateDatabaseHandler;
import org.jetbrains.annotations.NotNull;
import org.json.simple.parser.ParseException;
import org.roaringbitmap.longlong.LongBitmapDataProvider;

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

  private <S> S reduce(
      CellProcessor<S> cellProcessor,
      SerializableSupplier<S> identitySupplier,
      SerializableBinaryOperator<S> combiner
  ) throws ParseException, SQLException, IOException, ClassNotFoundException {
    CellIterator cellIterator = new CellIterator(
        this.tstamps.get(),
        this.bboxFilter, this.getPolyFilter(),
        this.getTagInterpreter(), this.getPreFilter(), this.getFilter(), false
    );

    Map<OSMType, LongBitmapDataProvider> bitMapIndex = null;
    if (this.update != null) {
      bitMapIndex = UpdateDatabaseHandler.getBitMap(
          this.update.getBitArrayDb()
      );
      cellIterator.excludeIDs(bitMapIndex);
    }
    S result = identitySupplier.get();
    for (Pair<CellId, CellId> cellIdRange : this.getCellIdRanges()) {
      ResultSet oshCellsRawData = getOshCellsRawDataFromDb(cellIdRange);

      while (oshCellsRawData.next()) {
        GridOSHEntity oshCellRawData = readOshCellRawData(oshCellsRawData);
        result = combiner.apply(
            result,
            cellProcessor.apply(oshCellRawData, cellIterator)
        );
      }
    }

    
    if (this.update != null) {
      cellIterator.includeIDsOnly(bitMapIndex);

      ResultSet updateEntites = this.getUpdates();
      while (updateEntites.next()) {
        byte[] data = updateEntites.getBytes("data");
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        ObjectInputStream ois = new ObjectInputStream(bais);
        OSHEntity entity = (OSHEntity) ois.readObject();

        GridOSHEntity updateCell = null;
        switch (entity.getType()) {
          case NODE:
            ArrayList<OSHNode> nodes = new ArrayList<>(1);
            nodes.add((OSHNode) entity);
            updateCell = GridOSHNodes.rebase(0, 0, 0, 0, 0, 0, nodes);
            break;
          case WAY:
            ArrayList<OSHWay> ways = new ArrayList<>(1);
            ways.add((OSHWay) entity);
            updateCell = GridOSHWays.compact(0, 0, 0, 0, 0, 0, ways);
            break;
          case RELATION:
            ArrayList<OSHRelation> relations = new ArrayList<>(1);
            relations.add((OSHRelation) entity);
            updateCell = GridOSHRelations.compact(0, 0, 0, 0, 0, 0, relations);
            break;
          default:
            throw new AssertionError(entity.getType().name());
        }

        result = combiner.apply(
            result,
            cellProcessor.apply(updateCell, cellIterator)
        );
      }
    }
    return result;
  }

  private Stream<X> stream(
      CellProcessor<Collection<X>> cellProcessor
  ) throws ParseException, SQLException, IOException, ClassNotFoundException {
    CellIterator cellIterator = new CellIterator(
        this.tstamps.get(),
        this.bboxFilter, this.getPolyFilter(),
        this.getTagInterpreter(), this.getPreFilter(), this.getFilter(), false
    );

    return Streams.stream(this.getCellIdRanges())
        .flatMap(this::getOshCellsStream)
        .map(oshCellRawData -> cellProcessor.apply(oshCellRawData, cellIterator))
        .flatMap(Collection::stream);
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
        Kernels.getOSMContributionCellReducer(mapper, identitySupplier, accumulator),
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
        Kernels.getOSMContributionGroupingCellReducer(mapper, identitySupplier, accumulator),
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
    return this.reduce(
        Kernels.getOSMEntitySnapshotCellReducer(mapper, identitySupplier, accumulator),
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
        Kernels.getOSMEntitySnapshotGroupingCellReducer(mapper, identitySupplier, accumulator),
        identitySupplier,
        combiner
    );
  }

  // === stream operations ===
  @Override
  protected Stream<X> mapStreamCellsOSMContribution(
      SerializableFunction<OSMContribution, X> mapper) throws Exception {
    return this.stream(Kernels.getOSMContributionCellStreamer(mapper));
  }

  @Override
  protected Stream<X> flatMapStreamCellsOSMContributionGroupedById(
      SerializableFunction<List<OSMContribution>, Iterable<X>> mapper
  ) throws Exception {
    return this.stream(Kernels.getOSMContributionGroupingCellStreamer(mapper));
  }

  @Override
  protected Stream<X> mapStreamCellsOSMEntitySnapshot(
      SerializableFunction<OSMEntitySnapshot, X> mapper) throws Exception {
    return this.stream(Kernels.getOSMEntitySnapshotCellStreamer(mapper));
  }

  @Override
  protected Stream<X> flatMapStreamCellsOSMEntitySnapshotGroupedById(
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<X>> mapper) throws Exception {
    return this.stream(Kernels.getOSMEntitySnapshotGroupingCellStreamer(mapper));
  }

}
