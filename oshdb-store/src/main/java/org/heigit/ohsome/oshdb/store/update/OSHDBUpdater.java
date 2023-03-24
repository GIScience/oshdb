package org.heigit.ohsome.oshdb.store.update;


import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.heigit.ohsome.oshdb.osm.OSMType.NODE;
import static org.heigit.ohsome.oshdb.osm.OSMType.RELATION;
import static org.heigit.ohsome.oshdb.osm.OSMType.WAY;
import static reactor.core.publisher.Flux.concat;
import static reactor.core.publisher.Flux.defer;

import com.google.common.collect.Maps;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.heigit.ohsome.oshdb.grid.GridOSHEntity;
import org.heigit.ohsome.oshdb.grid.GridOSHNodes;
import org.heigit.ohsome.oshdb.grid.GridOSHRelations;
import org.heigit.ohsome.oshdb.grid.GridOSHWays;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.osm.OSMWay;
import org.heigit.ohsome.oshdb.store.OSHDBStore;
import org.heigit.ohsome.oshdb.store.OSHData;
import org.heigit.ohsome.oshdb.util.CellId;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.function.TupleUtils;
import reactor.util.function.Tuple2;

public class OSHDBUpdater {

  private static final Logger log = LoggerFactory.getLogger(OSHDBUpdater.class);

  private final OSHDBStore store;
  private final GridUpdater gridUpdater;
  private final boolean optimize;


  private final Map<OSMType, Set<Long>> minorUpdates = new EnumMap<>(OSMType.class);
  private final Map<OSMType, Set<Long>> updatedEntities = new EnumMap<>(OSMType.class);

  public OSHDBUpdater(OSHDBStore store, GridUpdater gridUpdater, boolean optimize) {
    this.store = store;
    this.gridUpdater = gridUpdater;
    this.optimize = optimize;
  }

  public Flux<OSHEntity> updateEntities(Flux<Tuple2<OSMType, Flux<OSMEntity>>> entities) {
    return concat(
        entities.concatMap(TupleUtils.function(this::entities)),
        defer(this::minorWays),
        defer(this::minorRelations));
  }

  private Flux<OSHEntity> minorWays() {
    var ids = minorUpdates.getOrDefault(WAY, emptySet());
    return ways(ids.stream().collect(toMap(identity(), x -> emptyList())));
  }

  private Flux<OSHEntity> minorRelations() {
    var ids = minorUpdates.getOrDefault(RELATION, emptySet());
    return relations(ids.stream().collect(toMap(identity(), x -> emptyList())));
  }

  public Flux<OSHEntity> entities(OSMType type, Flux<OSMEntity> entities) {
    return switch (type) {
      case NODE -> nodes(entities.cast(OSMNode.class));
      case WAY -> ways(entities.cast(OSMWay.class));
      case RELATION -> relations(entities.cast(OSMRelation.class));
    };
  }

  private Flux<OSHEntity> nodes(Flux<OSMNode> entities) {
    return entities.collectMultimap(OSMEntity::getId)
        .flatMapMany(this::nodes);
  }

  private Flux<OSHEntity> nodes(Map<Long, Collection<OSMNode>> entities) {
    return Flux.using(() -> new Updates(store, minorUpdates, updatedEntities),
        updates -> updates.nodes(entities),
        updates -> updateGrid(NODE));
  }

  private Flux<OSHEntity> ways(Flux<OSMWay> entities) {
    return entities.collectMultimap(OSMEntity::getId)
        .flatMapMany(this::ways);
  }

  private Flux<OSHEntity> ways(Map<Long, Collection<OSMWay>> entities) {
    return Flux.using(() -> new Updates(store, minorUpdates, updatedEntities),
          updates -> updates.ways(mergeWithMinorUpdates(WAY, entities)),
          updates -> updateGrid(WAY));
  }

  private Flux<OSHEntity> relations(Flux<OSMRelation> entities) {
    return concat(
        defer(this::minorWays), // minor ways could trigger minor relations
        entities.collectMultimap(OSMEntity::getId).flatMapMany(this::relations));
  }

  private Flux<OSHEntity> relations(Map<Long, Collection<OSMRelation>> entities) {
    return Flux.using(() -> new Updates(store, minorUpdates, updatedEntities),
        updates -> updates.relations(mergeWithMinorUpdates(RELATION, entities)),
        updates -> updateGrid(RELATION));
  }

  private <T extends OSMEntity> Map<Long, Collection<T>> mergeWithMinorUpdates(OSMType type, Map<Long, Collection<T>> entities) {
    var minorIds = minorUpdates.getOrDefault(type, emptySet());
    var result = Maps.<Long, Collection<T>>newHashMapWithExpectedSize(entities.size() + minorIds.size());
    result.putAll(entities);
    minorIds.forEach(id -> result.computeIfAbsent(id, x -> emptyList()));
    return result;
  }

  private void updateGrid(OSMType type) {
    var cellIds = store.dirtyGrids(type);

    //TODO optimize grid!!!

    log.debug("updateGrid {} cells:{}", type, cellIds.size());
    for (var id : cellIds) {
      var cellId = CellId.fromLevelId(id);
      var entities = store.grid(type, id);
      var grid = buildGrid(type, cellId, entities);
      gridUpdater.update(type, cellId, grid);
    }
    store.resetDirtyGrids(type);
  }

  private GridOSHEntity buildGrid(OSMType type, CellId cellId, List<OSHData> entities) {
    if (entities.isEmpty()) {
      return null;
    }  
    var index = new int[entities.size()];
    var offset = 0;
    var i = 0;
    for (var data : entities) {
      index[i++] = offset;
      offset += data.getData().length;
    }
    i = 0;
    var data = new byte[offset];
    for (var oshData : entities) {
      var len = oshData.getData().length;
      System.arraycopy(oshData.getData(),0, data, index[i++], len);
    }
    return switch (type) {
      case NODE -> grid(GridOSHNodes.class, cellId.getId(), cellId.getZoomLevel(), index, data);
      case WAY -> grid(GridOSHWays.class, cellId.getId(), cellId.getZoomLevel(), index, data);
      case RELATION -> grid(GridOSHRelations.class, cellId.getId(), cellId.getZoomLevel(), index, data);
    };
  }

  @SuppressWarnings("unchecked")
  private static <T extends GridOSHEntity> T grid(Class<T> clazz, long id, int zoom, int[] index, byte[] data) {
    var constructor = clazz.getDeclaredConstructors()[0];
        constructor.setAccessible(true);
    try {
      return (T) constructor.newInstance(id, zoom, 0L, 0L, 0, 0, index, data);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new OSHDBException(e);
    }
  }

}
