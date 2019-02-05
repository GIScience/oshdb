package org.heigit.bigspatialdata.updater.util;

import com.beust.jcommander.Parameter;
import java.io.File;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.heigit.bigspatialdata.oshdb.tool.importer.cli.validator.DirExistValidator;

public class UpdateArgs {
  @Parameter(names = {"-wd", "-workDir", "-workingDir"}, description = "path to store the intermediate files.", validateWith = DirExistValidator.class, required = false, order = 1)
  public Path workDir = Paths.get("target/updaterWD/");

  @Parameter(names = {"-jdbc"}, description = "Connection details for jdbc-storage of updates: jdbc:dbms://host:port/database?user=UserName&password=Password", required = true, order = 2)
  public String jdbc;

  @Parameter(names = {"-url"}, description = "URL to take replication-files from e.g. https://planet.openstreetmap.org/replication/minute/", validateWith = URLValidator.class, order = 3)
  public URL baseURL;

  @Parameter(names = {"-i", "-ignite"}, description = "set ignite backend (default is H2)", required = false, order = 4)
  public boolean ignite = false;

  @Parameter(names = {"-flush"}, description = "flush updates from jdbc to ignite", required = false, order = 5)
  public boolean flush = false;

  @Parameter(names = {"-dbConfig", "-dbcfg"}, description = "Configuration of Database-Backend (either Path to ignite.xml or jdbc for h2 (parallel to jdbc))", required = false, order = 6)
  public String dbconfig;

  @Parameter(names = {"-kafka"}, description = "Path to kafka Config", required = false, order = 7)
  public File kafka;

  @Parameter(names = {"-keytables", "-k"}, description = "Configuration of Keytables JDBC (parallel to jdbc)", required = true, order = 8)
  public String keytables;

  @Parameter(names = {"-etlN"}, description = "Configuration of Keytables JDBC (parallel to jdbc)", required = false, order = 8)
  public File nodeEtl;

  @Parameter(names = {"-etlW"}, description = "Configuration of Keytables JDBC (parallel to jdbc)", required = false, order = 8)
  public File wayEtl;

  @Parameter(names = {"-etlR"}, description = "Configuration of Keytables JDBC (parallel to jdbc)", required = false, order = 8)
  public File relationEtl;

  @Parameter(names = {"-help", "--help", "-h", "--h"}, description = "prints this help", help = true, order = 99)
  public boolean help = false;
}
