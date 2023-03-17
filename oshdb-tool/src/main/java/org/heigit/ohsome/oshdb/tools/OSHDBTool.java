package org.heigit.ohsome.oshdb.tools;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.ServiceLoader;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ScopeType;

@Command(name = "oshdb-tool", description = "oshdb-tool",
    mixinStandardHelpOptions = true, scope = ScopeType.INHERIT,
    footerHeading = "Copyright%n", footer = "(c) Copyright by the authors")
public class OSHDBTool  {

  @Option(names = {"-v"}, scope = ScopeType.INHERIT)
  boolean[] verbose;

  public static void main(String[] args) {
    var main = new OSHDBTool();
    var cl = new CommandLine(main);


    var commands = new HashMap<String, CommandLine>();
    commands.put("tool", cl) ;

    var providers = new ArrayList<OSHDBToolCommandProvider>();
    for (var provider : ServiceLoader.load(OSHDBToolCommandProvider.class)){
      CommandLine command = provider.getCommand();
      commands.put(command.getCommandName(), command);
      providers.add(provider);
    }
    providers.forEach(provider -> provider.dependency(commands));

    args = new String[]{"-h"};
    var exit = cl.execute(args);
    System.exit(exit);
  }
}
