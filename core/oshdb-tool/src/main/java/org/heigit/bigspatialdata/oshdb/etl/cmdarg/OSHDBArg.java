package org.heigit.bigspatialdata.oshdb.etl.cmdarg;

import com.beust.jcommander.Parameter;
import java.io.File;

public class OSHDBArg {

  @Parameter(names = {"-db", "-oshdb", "-outputDb"}, description = "Path to output H2. please note that this should not contain .mv.db e.g. /home/user/osh", required = false)
  public File oshdb = new File("./oshdb");
}
