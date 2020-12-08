package org.heigit.bigspatialdata.oshdb.tool.importer.load.cli;

import java.nio.file.Path;

import org.heigit.bigspatialdata.oshdb.tool.importer.cli.CommonArgs;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.beust.jcommander.validators.PositiveInteger;

public class DBH2Arg {
  @ParametersDelegate
  public CommonArgs common = new CommonArgs();
  
  @Parameter(names = {"-mn","--min-nodes"}, description = "minimum of nodes per grid cell", validateWith = PositiveInteger.class)
  public int minNodesPerGrid = 1000;
  
  @Parameter(names = {"-mw","--min-ways"}, description = "minimum of ways per grid cell", validateWith = PositiveInteger.class)
  public int minWaysPerGrid = 100;
  
  @Parameter(names = {"-mr","--min-relations"}, description = "minimum of relations per grid cell", validateWith = PositiveInteger.class)
  public int minRelationPerGrid = 10;

  @Parameter(names={"--nodesWithTagsOnly"}, description ="only nodes with tags in the nodes grid")
  public boolean onlyNodesWithTags = true;
  
  @Parameter(names={"--withOutKeyTables"}, description ="load also keytables in to h2 db")
  public boolean withOutKeyTables;
  
  @Parameter(names={"--out"}, description="output path", required = true)
  public Path h2db;
  
  @Parameter(names = {"-z", "--maxZoom" }, description = "maximal zoom level", validateWith = PositiveInteger.class,  order = 2)
  public int maxZoom = 15;
    
  @Parameter(names = {"--attribution"}, required = true)
  public String attribution = "Copyright Right";
  
  @Parameter(names = {"--attribution-url"}, required = true)
  public String attributionUrl;
     
}
