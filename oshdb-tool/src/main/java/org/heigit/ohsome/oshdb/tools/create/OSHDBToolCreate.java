package org.heigit.ohsome.oshdb.tools.create;

import java.nio.file.Path;
import java.util.concurrent.Callable;
import org.heigit.ohsome.oshdb.tools.OSHDBTool;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

/**
 *  oshdb-tool create --db rocksdb --memory 10M --directory /tmp/oshdb-rocksdb --pbf /data/osm.pbf
 */
@Command(name="create")
public class OSHDBToolCreate implements Callable<Integer> {

  @ParentCommand
  OSHDBTool parent;

  @Option(names = {"pbf"})
  Path pbf;

  @Override
  public Integer call() throws Exception {
    return 0;
  }
}
