package org.heigit.ohsome.oshdb.ignite.progress;

import me.tongfei.progressbar.ProgressBarConsumer;

public class SysOutProgressConsumer implements ProgressBarConsumer {
  private final int maxLength;

  public SysOutProgressConsumer(int maxLength) {
    this.maxLength = maxLength;
  }

  @Override
  public int getMaxRenderedLength() {
    return maxLength;
  }

  @Override
  public void accept(String rendered) {
    System.out.println(rendered);
  }

  @Override
  public void close() {
    // no/op
  }
}
