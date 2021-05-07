package org.heigit.ohsome.oshdb.tool.importer.load.handle;

import java.util.Collection;
import java.util.List;
import org.heigit.ohsome.oshdb.tool.importer.load.LoaderKeyTables;
import org.heigit.ohsome.oshdb.tool.importer.load.LoaderNode;
import org.heigit.ohsome.oshdb.tool.importer.load.LoaderRelation;
import org.heigit.ohsome.oshdb.tool.importer.load.LoaderWay;
import org.heigit.ohsome.oshdb.tool.importer.transform.oshdb.TransfomRelation;
import org.heigit.ohsome.oshdb.tool.importer.transform.oshdb.TransformOSHNode;
import org.heigit.ohsome.oshdb.tool.importer.transform.oshdb.TransformOSHWay;

public class LoaderHandler implements LoaderKeyTables.Handler, LoaderNode.Handler,
    LoaderWay.Handler, LoaderRelation.Handler {

  @Override
  public void handleNodeGrid(long cellId, Collection<TransformOSHNode> nodes) {
  }

  @Override
  public void handleWayGrid(long cellId, Collection<TransformOSHWay> ways,
      Collection<TransformOSHNode> nodes) {
  }

  @Override
  public void handleRelationGrid(long cellId, Collection<TransfomRelation> entities,
      Collection<TransformOSHNode> nodes, Collection<TransformOSHWay> ways) {
  }

  @Override
  public void loadKeyValues(int id, String key, List<String> values) {
  }

  @Override
  public void loadRole(int id, String role) {
  }

}
