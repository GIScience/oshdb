package org.heigit.ohsome.oshdb.tools;

import org.heigit.ohsome.oshdb.tools.update.UpdateCommand;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ScopeType;

@Command(name = "oshdb-tool", description = "oshdb-tool",
    mixinStandardHelpOptions = true, scope = ScopeType.INHERIT,
    subcommands = {
      UpdateCommand.class
    },
    footerHeading = "Copyright%n", footer = "(c) Copyright by the authors")
public class OSHDBTool  {

  @Option(names = {"-v"}, scope = ScopeType.INHERIT)
  boolean[] verbose;

  public static void main(String[] args) {
    var main = new OSHDBTool();
    var cli = new CommandLine(main);
    var exit = cli.execute(args);
    System.exit(exit);
  }
}
