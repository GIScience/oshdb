package org.heigit.bigspatialdata.updater.util.cmd;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

public class FlushArgs {

  @Parameter(names = {"-dbConfig", "-dbcfg"},
      description = "connection of production database. JDBC definition for H2, Igntie file path otherwise",
      required = true,
      order = 1)
  public String dbconfig;
  
  @Parameter(names = {"-updateMeta"},
      description="Set wheather the matadata of the OSHDB should be updated. Make sure metadatatable is present and properly configured.",
      required=false)
  public boolean updateMeta=false;

  @ParametersDelegate
  public BaseArgs baseArgs = new BaseArgs();

}
