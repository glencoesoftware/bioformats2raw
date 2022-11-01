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
  private int completedTiles = 0;
  private int expectedTileCount = 0;

  @Override
  public void notifyResolution(int series, int resolution, int tileCount) {
    expectedTileCount = tileCount;
  }

  @Override
  public void notifyChunk(int plane, int xx, int yy, int zz) {
    completedTiles++;
  }

  @Override
  public void notifyDone(int series, int resolution) {
    if (completedTiles == expectedTileCount) {
      finishedResolutions.add(completedTiles);
    }
    completedTiles = 0;
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
