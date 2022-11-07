/**
 * Copyright (c) 2022 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.bioformats2raw.test;

import com.glencoesoftware.bioformats2raw.IProgressListener;

import java.util.ArrayList;
import java.util.List;

public class TestProgressListener implements IProgressListener {

  private List<Integer> finishedResolutions = new ArrayList<Integer>();
  private int startedTiles = 0;
  private int completedTiles = 0;
  private int expectedTileCount = 0;

  @Override
  public void notifySeriesStart(int series) {
  }

  @Override
  public void notifySeriesEnd(int series) {
  }

  @Override
  public void notifyResolutionStart(int resolution, int tileCount) {
    expectedTileCount = tileCount;
  }

  @Override
  public void notifyChunkStart(int plane, int xx, int yy, int zz) {
    synchronized (this) {
      startedTiles++;
    }
  }

  @Override
  public void notifyChunkEnd(int plane, int xx, int yy, int zz) {
    synchronized (this) {
      completedTiles++;
    }
  }

  @Override
  public void notifyResolutionEnd(int resolution) {
    if (startedTiles == completedTiles && completedTiles == expectedTileCount) {
      finishedResolutions.add(completedTiles);
    }
    completedTiles = 0;
    startedTiles = 0;
  }

  /**
   * Get the recorded tile counts for each completed resolution.
   *
   * @return an array with one element per resolution
   */
  public Integer[] getTileCounts() {
    return finishedResolutions.toArray(new Integer[finishedResolutions.size()]);
  }

}
