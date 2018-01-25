package org.heigit.bigspatialdata.oshdb.tool.etl.cmdarg;

import com.beust.jcommander.ParametersDelegate;

public class TransformArgs {

  @ParametersDelegate
  public ExtractArgs baseArgs = new ExtractArgs();

  @ParametersDelegate
  public OSHDBArg oshdbarg = new OSHDBArg();
}
