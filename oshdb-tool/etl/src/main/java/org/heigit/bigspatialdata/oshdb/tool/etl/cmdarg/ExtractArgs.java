package org.heigit.bigspatialdata.oshdb.tool.etl.cmdarg;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtractArgs {

  private static final Logger LOG = LoggerFactory.getLogger(ExtractArgs.class);
  @ParametersDelegate
  public HelpArg help = new HelpArg();

  @Parameter(names = {"-pbf"}, description = "Path to pbf-File to import", required = true, order = 0)
  public File pbfFile;

  @Parameter(names = {"-key", "-keytables", "-keyDb"}, description = "Path to output keytables. Please note that this should not contain the file extension (.mv.db) e.g. /home/user/keytables", required = false, order = 1)
  public File keytables = new File("./keytables");

  @Parameter(names = {"-tmp", "-temporyDirectory"}, description = "path to store temporary files", required = false, order = 2)
  public Path tempDir = Paths.get("./");

}
