/**
 * Copyright (c) 2022 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.bioformats2raw;

import java.util.EventListener;

public interface IProgressListener extends EventListener {

  /**
   * Indicates the beginning of processing a particular resolution.
   *
   * @param series the series index being processed
   * @param resolution the resolution index being processed
   * @param tileCount the total number of tiles in this resolution
   */
  void notifyResolution(int series, int resolution, int tileCount);

  /**
   * Indicates that the given chunk has been processed.
   *
   * @param plane plane index
   * @param xx X coordinate
   * @param yy Y coordinate
   * @param zz Z coordinate
   */
  void notifyChunk(int plane, int xx, int yy, int zz);

  /**
   * Indicates the end of processing a particular resolution.
   *
   * @param series the series index being processed
   * @param resolution the resolution index being processed
   */
  void notifyDone(int series, int resolution);

}
