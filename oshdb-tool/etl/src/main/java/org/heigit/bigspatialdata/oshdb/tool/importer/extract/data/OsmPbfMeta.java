package org.heigit.bigspatialdata.oshdb.tool.importer.extract.data;

import java.nio.file.Path;

public class OsmPbfMeta {

  public Path pbf;
  
  public long nodeStart;
  public long nodeEnd;

  public long wayStart;
  public long wayEnd;
  
  public long relationStart;
  public long relationEnd;

}
