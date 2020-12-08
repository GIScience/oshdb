package org.heigit.bigspatialdata.oshdb.tool.importer.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.validators.PositiveInteger;

public class DistributableArgs {
  @Parameter(names = { "--worker" }, description = "number of this worker (beginning with 0).", validateWith =PositiveInteger.class, required = false, order = 20)
  public int worker = 0;

  @Parameter(names = { "--totalWorker" }, description = "total number of workers.", validateWith =PositiveInteger.class, required = false, order = 21)
  public int totalWorkers = 1;
  
  @Parameter(names = {"--merge"}, description = "merge output of workers together", required = false, order = 22)
  public boolean merge;
}
