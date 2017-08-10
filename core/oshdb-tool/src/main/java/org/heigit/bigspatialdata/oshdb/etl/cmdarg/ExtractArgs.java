package org.heigit.bigspatialdata.oshdb.etl.cmdarg;

import com.beust.jcommander.Parameter;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Logger;

public class ExtractArgs {

  private static final Logger LOG = Logger.getLogger(ExtractArgs.class.getName());

  @Parameter(names = {"-pbf"}, description = "Path to pbf-File to import", required = true)
  public File pbfFile;

  @Parameter(names = {"-tmp", "-temporyDirectory"}, description = "path to store temporary files", required = false)
  public Path tempDir = Paths.get("./");

  @Parameter(names = {"-db", "-oshdb", "-outputDb"}, description = "Path to output H2. please note that this should not contain .mv.db e.g. /home/user/osh", required = false)
  public File oshdb = new File("./oshdb");
}
