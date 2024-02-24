package org.heigit.ohsome.oshdb.ignite.progress;

import java.time.Duration;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;

public class ProgressUtil {

  private ProgressUtil(){
    // utility class
  }

  public static ProgressBar progressBar(String name, int size, Duration updateInterval) {
    return new ProgressBarBuilder().setTaskName(name)
        .setUpdateIntervalMillis((int)updateInterval.toMillis())
        .setInitialMax(size)
        .setConsumer(new SysOutProgressConsumer(60))
        .build();
  }
}
