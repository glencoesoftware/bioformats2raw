/**
 * Copyright (c) 2022 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.bioformats2raw;

public class NoOpProgressListener implements IProgressListener {

  @Override
  public void notifySeriesStart(int series) {
  }

  @Override
  public void notifySeriesEnd(int series) {
  }

  @Override
  public void notifyResolutionStart(int resolution, int tileCount) {
  }

  @Override
  public void notifyResolutionEnd(int resolution) {
  }

  @Override
  public void notifyChunkStart(int plane, int xx, int yy, int zz) {
  }

  @Override
  public void notifyChunkEnd(int plane, int xx, int yy, int zz) {
  }


}
