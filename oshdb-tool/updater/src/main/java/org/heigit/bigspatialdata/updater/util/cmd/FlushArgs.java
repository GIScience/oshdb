package org.heigit.bigspatialdata.updater.util.cmd;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

public class FlushArgs {

  @Parameter(names = {"-dbConfig", "-dbcfg"},
      description = "connection of production database. JDBC definition for H2, Igntie file path otherwise",
      required = true,
      order = 1)
  public String dbconfig;

  @ParametersDelegate
  public BaseArgs baseArgs = new BaseArgs();

}
