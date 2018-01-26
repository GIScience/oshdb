package org.heigit.bigspatialdata.oshdb.tool.importer.load.cli;

import org.heigit.bigspatialdata.oshdb.tool.importer.cli.CommonArgs;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.beust.jcommander.validators.PositiveInteger;

public class DBH2Arg {
  @ParametersDelegate
  public CommonArgs common = new CommonArgs();
  
  @Parameter(names = {"-z", "--maxZoom" }, description = "maximal zoom level", validateWith = PositiveInteger.class,  order = 2)
  public int maxZoom = 15;
}
