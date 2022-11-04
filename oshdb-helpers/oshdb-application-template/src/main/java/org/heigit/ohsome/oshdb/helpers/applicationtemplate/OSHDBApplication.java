package org.heigit.ohsome.oshdb.helpers.applicationtemplate;

import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.heigit.ohsome.oshdb.helpers.db.OSHDBConnection;
import org.heigit.ohsome.oshdb.helpers.db.OSHDBDriver;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Class that can be used to bootstrap and launch a OSHDB application from a Java main method. By
 * default will perform the following steps to bootstrap your application:
 * <ul>
 * <li>parse command line parameters</li>
 * <li>connect to specified oshdb</li>
 * </ul>
 * In most circumstances the static run(Class, String[]) method can be called directly from your
 * main method to bootstrap your application:
 *
 * <pre>
 * {@code
 * public class MyApplication extends OSHDBApplication {
 *
 *    public static void main(String[] args){
 *      OSHDBApplication.run(MyApplication.class, args);
 *    }
 *
 *    protected int run(OSHDBConnection oshdb){
 *     oshdb.getSnapshotView()
 *         .areaOfInterest((Geometry & Polygonal) areaOfInterest)
 *         .timestamps(tstamps)
 *         .osmTag(key)
 *         ...
 *     return 0; // exitCode
 *    }
 * }}
 * </pre>
 */
@Command(mixinStandardHelpOptions = true, sortOptions = false)
public abstract class OSHDBApplication implements Callable<Integer> {

  @ArgGroup(exclusive = false, multiplicity = "1")
  ConfigOrUrl configOrUrl;

  static class ConfigOrUrl {
    @Option(names = {"--props"}, description = ".properties file path")
    protected Path config;

    @Option(names = {"--oshdb"}, description = "oshdbUrl (ignite:...|h2:...)")
    protected String oshdbUrl;
  }

  @Option(names = {"--keytables"}, description = "keytablesUrl jdbc:...")
  protected String keytableUrl;

  @Option(names = {"--prefix"}, description = "prefix to use")
  protected String prefix;

  @Option(names = {
      "--multithreading"}, description = "for jdbc based connections", negatable = true)
  protected Boolean multithreading = null;

  protected Properties props;

  @SuppressWarnings("java:S112")
  protected abstract int run(OSHDBConnection oshdb) throws Exception;

  /**
   * Method to be called from the implemented application.
   *
   * @param clazz Class that will be started.
   * @param args main args
   * @throws Exception from application
   */
  @SuppressWarnings("java:S112")
  public static void run(Class<? extends OSHDBApplication> clazz, String[] args)
      throws Exception {
    var app = clazz.getDeclaredConstructor().newInstance();
    int exit = new CommandLine(app).execute(args);
    System.exit(exit);
  }

  @Override
  public Integer call() throws Exception {
    props = new Properties();
    PropsUtil.load(props, configOrUrl.config);
    PropsUtil.set(props, OSHDBDriver.OSHDB_PROPERTY_NAME, configOrUrl.oshdbUrl);
    PropsUtil.set(props, OSHDBDriver.KEYTABLES_PROPERTY_NAME, keytableUrl);
    PropsUtil.set(props, OSHDBDriver.PREFIX_PROPERTY_NAME, prefix);
    PropsUtil.set(props, OSHDBDriver.MULTITHREADING_PROPERTY_NAME, multithreading);
    return OSHDBDriver.connect(props, this::setAndRun);
  }

  private int setAndRun(OSHDBConnection connection) throws Exception {
    configOrUrl.oshdbUrl = PropsUtil.get(props, OSHDBDriver.OSHDB_PROPERTY_NAME).orElseThrow();
    keytableUrl = PropsUtil.get(props, OSHDBDriver.KEYTABLES_PROPERTY_NAME).orElse(null);
    prefix = PropsUtil.get(props, OSHDBDriver.PREFIX_PROPERTY_NAME).orElse("");
    multithreading = Boolean.valueOf(
        PropsUtil.get(props, OSHDBDriver.MULTITHREADING_PROPERTY_NAME).orElse("false"));
    return run(connection);
  }
}
