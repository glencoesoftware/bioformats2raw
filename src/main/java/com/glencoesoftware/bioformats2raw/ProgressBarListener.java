/**
 * Copyright (c) 2022 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.bioformats2raw;

import me.tongfei.progressbar.DelegatingProgressBarConsumer;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProgressBarListener implements IProgressListener {

  // not a typo - the progress bar consumes Converter's logging output
  private static final Logger LOGGER = LoggerFactory.getLogger(Converter.class);

  private String logLevel;
  private ProgressBar pb;
  private int currentSeries = -1;

  /**
   * Create a new progress listener that displays a progress bar.
   *
   * @param level logging level
   */
  public ProgressBarListener(String level) {
    logLevel = level;
  }


  @Override
  public void notifySeriesStart(int series) {
    currentSeries = series;
  }

  @Override
  public void notifySeriesEnd(int series) {
  }

  @Override
  public void notifyResolutionStart(int resolution, int tileCount) {
    ProgressBarBuilder builder = new ProgressBarBuilder()
      .setInitialMax(tileCount)
      .setTaskName(String.format("[%d/%d]", currentSeries, resolution));

    if (!(logLevel.equals("OFF") ||
      logLevel.equals("ERROR") ||
      logLevel.equals("WARN")))
    {
      builder.setConsumer(new DelegatingProgressBarConsumer(LOGGER::trace));
    }
    pb = builder.build();
  }

  @Override
  public void notifyChunkStart(int plane, int xx, int yy, int zz) {
    // intentional no-op
  }

  @Override
  public void notifyChunkEnd(int plane, int xx, int yy, int zz) {
    if (pb != null) {
      pb.step();
    }
  }

  @Override
  public void notifyResolutionEnd(int resolution) {
    if (pb != null) {
      pb.close();
      pb = null;
    }
  }

}
