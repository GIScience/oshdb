package org.heigit.ohsome.oshdb.tool.importer.load;

import it.unimi.dsi.fastutil.longs.Long2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.heigit.ohsome.oshdb.tool.importer.transform.oshdb.TransformOSHNode;
import org.heigit.ohsome.oshdb.tool.importer.transform.reader.TransfromNodeReaders;
import org.heigit.ohsome.oshdb.tool.importer.util.ZGrid;

public class LoaderNode extends Loader {

  public static interface Handler {
    void handleNodeGrid(long cellId, Collection<TransformOSHNode> nodes);
  }

  private static class Grid {
    long cellId = -1;
    List<TransformOSHNode> entities;

    public void clear() {
      entities = null;
    }
  }

  final List<Grid> zoomLevel = new ArrayList<>(20);
  int maxZoom = -1;
  final int maxZoomLevel;

  private final TransfromNodeReaders reader;
  private final Handler handler;
  private final boolean onlyNodesWithTags;

  public LoaderNode(Path workDirectory, Handler handler, int minEntitiesPerCell,
      boolean onlyNodesWithTags, int maxZoomLevel) throws IOException {
    super(minEntitiesPerCell);
    Path[] files;
    try (DirectoryStream<Path> stream =
        Files.newDirectoryStream(workDirectory, "transform_node_*")) {
      files = StreamSupport.stream(stream.spliterator(), false).collect(Collectors.toList())
          .toArray(new Path[0]);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    reader = new TransfromNodeReaders(files);
    this.handler = handler;
    this.onlyNodesWithTags = onlyNodesWithTags;
    this.maxZoomLevel = Math.max(1, maxZoomLevel);
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  public final Long2ObjectMap<TransformOSHNode> invalidNodes = new Long2ObjectAVLTreeMap<>();


  @Override
  public void load(long cellId2, boolean all) {
    if (!reader.hasNext()) {
      return;
    }

    if (reader.getCellId() == -1) {
      Set<TransformOSHNode> set = reader.next();
      set.stream().forEach(node -> invalidNodes.put(node.getId(), node));
      handler.handleNodeGrid(-1, invalidNodes.values());
    }

    while (reader.hasNext()
        && (all || (ZGrid.ORDER_DFS_TOP_DOWN.compare(reader.getCellId(), cellId2) <= 0))) {
      final long cellId = reader.getCellId();
      final int zoom = ZGrid.getZoom(cellId);
      final Set<TransformOSHNode> nodes = reader.next();

      nodes.forEach(node -> {
        for (Loader bla : loaders) {
          bla.visitNode(node);
        }
      });

      initZoomLevel(zoom);

      store(zoom);

      Grid grid = zoomLevel.get(zoom);
      grid.cellId = cellId;

      if (onlyNodesWithTags) {
        grid.entities = nodes.stream().filter(osh -> {
          return osh.stream().anyMatch(osm -> osm.getRawTags().length > 0);
        }).collect(Collectors.toList());
      } else {
        grid.entities = new ArrayList<>(nodes);
      }

    }
    if (!reader.hasNext()) {
      store(0);
    }
  }

  private void store(int zoom) {
    for (int i = maxZoom; i >= zoom; i--) {
      Grid grid = zoomLevel.get(i);
      if ((grid == null) || (grid.entities == null)) {
        continue;
      }

      if ((i > maxZoomLevel) || ((grid.entities.size() < minEntitiesPerCell) && (i > 0))) {
        Grid parent = zoomLevel.get(i - 1);
        if (parent.entities == null) {
          parent.cellId = ZGrid.getParent(grid.cellId);
          parent.entities = grid.entities;
        } else {
          parent.entities.addAll(grid.entities);
        }
        grid.clear();
        continue;
      }
      handler.handleNodeGrid(grid.cellId, grid.entities);
      grid.clear();
    }
  }

  protected Grid getParentInZoomHierachie(int zoom) {
    if (zoom > 0) {
      return zoomLevel.get(zoom - 1);
    }
    return null;
  }

  protected void initZoomLevel(int zoom) {
    if (maxZoom < zoom) {
      for (int i = maxZoom; i < zoom; i++) {
        zoomLevel.add(new Grid());
      }
      maxZoom = zoom;
    }
  }
}