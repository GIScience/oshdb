package org.heigit.bigspatialdata.updater.util.cmd;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

/**
 * Special arguments for Flushing.
 */
public class FlushArgs {

  /**
   * Common base arguments.
   */
  @ParametersDelegate
  public BaseArgs baseArgs = new BaseArgs();

  /**
   * The configuration for the oshdb. Content of string may vary according to type (H2 -> jdbc,
   * Ignite -> file-path).
   */
  @Parameter(names = {"-dbConfig", "-dbcfg"},
      description = "connection of production database. "
      + "JDBC definition for H2, Igntie file path otherwise",
      required = true,
      order = 1)
  public String dbconfig;

  /**
   * True if metadate of oshdb should be updated.
   */
  @Parameter(names = {"-updateMeta"},
      description = "Set wheather the matadata of the OSHDB should be updated. "
      + "Make sure metadatatable is present and properly configured.",
      required = false)
  public boolean updateMeta = false;

}
