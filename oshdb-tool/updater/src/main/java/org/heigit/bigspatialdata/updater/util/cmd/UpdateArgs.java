package org.heigit.bigspatialdata.updater.util.cmd;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import java.io.File;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.heigit.bigspatialdata.oshdb.tool.importer.cli.validator.DirExistValidator;
import org.heigit.bigspatialdata.updater.util.URLValidator;

public class UpdateArgs {

  @Parameter(names = {"-url"},
      description = "URL to take replication-files from e.g. https://planet.openstreetmap.org/replication/minute/",
      validateWith = URLValidator.class,
      required = true,
      order = 1)
  public URL baseURL;

  @Parameter(names = {"-etl"},
      description = "Path to etlFiles",
      validateWith = DirExistValidator.class,
      required = true,
      order = 2)
  public Path etl;

  @Parameter(names = {"-kafka"},
      description = "Path to kafka Config",
      required = false,
      order = 4)
  public File kafka;

  @Parameter(names = {"-keytables", "-k"},
      description = "Configuration of Keytables JDBC (parallel to jdbc)",
      required = true,
      order = 3)
  public String keytables;

  @Parameter(names = {"-wd", "-workDir", "-workingDir"},
      description = "path to store the intermediate files.",
      validateWith = DirExistValidator.class,
      required = false,
      order = 5)
  public Path workDir = Paths.get("target/updaterWD/");

  @ParametersDelegate
  public BaseArgs baseArgs = new BaseArgs();
}
