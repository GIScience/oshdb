package org.heigit.bigspatialdata.oshdb.etl.cmdarg;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import java.io.File;

public class LoadArgs {

  @ParametersDelegate
  public OSHDBArg oshdbarg = new OSHDBArg();

  @ParametersDelegate
  public HelpArg help = new HelpArg();

  @Parameter(names = {"-ignite", "-igniteConfig", "-icfg"}, description = "Path ot ignite-config.xml", required = true, order = 1)
  public File ignitexml;

  @Parameter(names = {"--prefix"}, description = "cache table prefix", required = false)
  public String prefix;
}
