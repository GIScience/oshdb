package org.heigit.ohsome.oshdb.tool.importer.load;

import com.google.common.base.Functions;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.heigit.ohsome.oshdb.tool.importer.extract.Extract.KeyValuePointer;
import org.heigit.ohsome.oshdb.tool.importer.extract.data.Role;
import org.heigit.ohsome.oshdb.tool.importer.extract.data.ValueFrequency;

public class LoaderKeyTables {

  public interface Handler {
    void loadKeyValues(int id, String key, List<String> values);

    void loadRole(int id, String role);
  }

  Path workDirectory;
  private Handler handler;

  public LoaderKeyTables(Path workdirectory, Handler handler) {
    this.workDirectory = workdirectory;
    this.handler = handler;
  }

  public void load() {
    loadTags();
  }

  public void loadTags() {
    final Function<InputStream, InputStream> input = Functions.identity();

    try (
        DataInputStream keyIn = new DataInputStream(input.apply(new BufferedInputStream(
            new FileInputStream(workDirectory.resolve("extract_keys").toFile()))));
        final RandomAccessFile raf =
            new RandomAccessFile(workDirectory.resolve("extract_keyvalues").toFile(), "r");
        final FileChannel valuesChannel = raf.getChannel();) {

      final int length = keyIn.readInt();
      for (int i = 0; i < length; i++) {
        final KeyValuePointer kvp = KeyValuePointer.read(keyIn);

        final String key = kvp.key;
        List<String> values = Collections.emptyList();

        values = new ArrayList<>(kvp.valuesNumber);

        valuesChannel.position(kvp.valuesOffset);
        try (DataInputStream valueStream =
            new DataInputStream(Channels.newInputStream(valuesChannel));) {
          for (int j = 0; j < kvp.valuesNumber; j++) {
            final ValueFrequency vf = ValueFrequency.read(valueStream);
            values.add(vf.value);
          }
        }

        handler.loadKeyValues(i, key, values);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void loadRoles() {
    final Function<InputStream, InputStream> input = Functions.identity();
    try (DataInputStream roleIn = new DataInputStream(input.apply(new BufferedInputStream(
        new FileInputStream(workDirectory.resolve("extract_roles").toFile()))))) {
      for (int id = 0; true; id++) {
        final Role role = Role.read(roleIn);
        handler.loadRole(id, role.role);
      }
    } catch (IOException e) {
      if (!(e instanceof EOFException)) {
        e.printStackTrace();
      }
    }
  }
}
