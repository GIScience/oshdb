package org.heigit.bigspatialdata.oshdb.etl.cmdarg;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import java.io.File;

public class TransformArgs {

  @ParametersDelegate
  public ExtractArgs baseArgs = new ExtractArgs();

  @Parameter(names = {"-db", "-oshdb", "-outputDb"}, description = "Path to output H2. please note that this should not contain .mv.db e.g. /home/user/osh", required = false)
  public File oshdb = new File("./oshdb");
}
