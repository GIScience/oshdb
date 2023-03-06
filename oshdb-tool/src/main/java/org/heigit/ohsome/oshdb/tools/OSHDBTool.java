package org.heigit.ohsome.oshdb.tools;

import java.util.ServiceLoader;
import java.util.concurrent.Callable;
import org.heigit.ohsome.oshdb.tools.create.OSHDBToolCreate;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ScopeType;

@Command(name = "oshdb-tool", description = "oshdb-tool",
    mixinStandardHelpOptions = true, scope = ScopeType.INHERIT,
    footerHeading = "Copyright%n", footer = "(c) Copyright by the authors",
    subcommands = {
    OSHDBToolCreate.class
})
public class OSHDBTool implements Callable<Integer> {

  @Override
  public Integer call() throws Exception {
    return 0;
  }

  public static void main(String[] args) {
    var main = new OSHDBTool();
    var cl = new CommandLine(main);
    args = new String[]{"create", "-h"};
    var exit = cl.execute(args);
    System.exit(exit);
  }
}
