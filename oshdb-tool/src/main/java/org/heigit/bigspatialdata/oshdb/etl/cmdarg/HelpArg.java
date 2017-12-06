package org.heigit.bigspatialdata.oshdb.etl.cmdarg;

import com.beust.jcommander.Parameter;

public class HelpArg {

  @Parameter(names = {"-help", "--help", "-h", "--h"}, help = true, order = 0)
  public boolean help = false;
}
