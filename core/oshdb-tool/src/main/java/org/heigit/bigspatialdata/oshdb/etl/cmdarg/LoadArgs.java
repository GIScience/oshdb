package org.heigit.bigspatialdata.oshdb.etl.cmdarg;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import java.io.File;

public class LoadArgs {

  @ParametersDelegate
  public OSHDBArg oshdbarg = new OSHDBArg();

  @Parameter(names = {"-ignite", "-igniteConfig", "-icfg"}, description = "Path ot ignite-config.xml", required = true)
  public File ignitexml;
}
