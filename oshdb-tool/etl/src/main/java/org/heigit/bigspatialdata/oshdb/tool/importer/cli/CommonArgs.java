package org.heigit.bigspatialdata.oshdb.tool.importer.cli;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.heigit.bigspatialdata.oshdb.tool.importer.cli.validator.DirExistValidator;

import com.beust.jcommander.Parameter;

public class CommonArgs {
  @Parameter(names = { "-workDir", "--workingDir" }, description = "path to store the result files.", validateWith =DirExistValidator.class, required = false, order = 10)
  public Path workDir = Paths.get(".");
  
  @Parameter(names = {"-tmpDir", "--temporayDirectory"}, description = "path to store temporary files", validateWith =DirExistValidator.class, required = false, order = 11)
  public Path tempDir;
  
  @Parameter(names = {"-help", "--help", "-h", "--h"},description = "prints this help", help = true, order = 99)
  public boolean help = false;
}
