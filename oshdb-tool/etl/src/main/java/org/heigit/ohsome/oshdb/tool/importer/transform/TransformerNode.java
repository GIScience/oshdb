package org.heigit.ohsome.oshdb.tool.importer.transform;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.LongFunction;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.tool.importer.transform.oshdb.TransformOSHNode;
import org.heigit.ohsome.oshdb.tool.importer.util.TagToIdMapper;
import org.heigit.ohsome.oshdb.util.bytearray.ByteArrayOutputWrapper;
import org.heigit.ohsome.oshpbf.parser.osm.v06.Entity;
import org.heigit.ohsome.oshpbf.parser.osm.v06.Node;

public class TransformerNode extends Transformer {

  private final ByteArrayOutputWrapper baData = new ByteArrayOutputWrapper(1024);
  private final ByteArrayOutputWrapper baRecord = new ByteArrayOutputWrapper(1024);
  private final ByteArrayOutputWrapper baAux = new ByteArrayOutputWrapper(1024);

  public TransformerNode(long maxMemory, int maxZoom, Path workDirectory,
      TagToIdMapper tagToIdMapper, int workerId) throws IOException {
    super(maxMemory, maxZoom, workDirectory, tagToIdMapper, workerId);
  }

  @Override
  public OSMType type() {
    return OSMType.NODE;
  }

  @Override
  public void transform(long id, List<Entity> versions) {

    final List<OSMNode> nodes = new ArrayList<>(versions.size());
    final Set<Long> cellIds = new TreeSet<>();
    for (Entity version : versions) {
      final Node node = (Node) version;
      if (version.isVisible()) {
        final long zId = getCell(node.getLongitude(), node.getLatitude());
        if (zId >= 0) {
          cellIds.add(zId);
        }
      }
      nodes.add(getNode(node));
    }
    final long cellId = cellIds.size() > 0 ? findBestFittingCellId(cellIds) : -1;

    try {

      final OSHDBBoundingBox bbox = getCellBounce(cellId);

      final long baseLongitude = bbox.getMinLonLong();
      final long baseLatitude = bbox.getMinLatLong();

      final LongFunction<byte[]> toByteArray = baseId -> {
        try {
          TransformOSHNode.build(baData, baRecord, baAux, nodes,
              baseId, 0L, baseLongitude, baseLatitude);

          final byte[] record = new byte[baRecord.length()];
          System.arraycopy(baRecord.array(), 0, record, 0, record.length);

          return record;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      };

      store(cellId, id, toByteArray);
      addIdToCell(id, cellId);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private OSMNode getNode(Node entity) {
    return new OSMNode(entity.getId(), //
        modifiedVersion(entity), //
        entity.getTimestamp(), //
        entity.getChangeset(), //
        entity.getUserId(), //
        getKeyValue(entity.getTags()), //
        entity.getLongitude(), entity.getLatitude());
  }
}
