/**
 * Copyright (c) 2020 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */

package com.glencoesoftware.bioformats2raw;

import org.opencv.imgproc.Imgproc;

/**
 */
public enum Downsampling {
  SIMPLE(-1, "simple"),
  GAUSSIAN(-1, "gaussian"),
  AREA(Imgproc.INTER_AREA, "area"),
  LINEAR(Imgproc.INTER_LINEAR, "linear"),
  CUBIC(Imgproc.INTER_CUBIC, "cubic"),
  LANCZOS(Imgproc.INTER_LANCZOS4, "lanczos");

  private final int code;
  private final String name;

  private Downsampling(int newCode, String newName) {
    this.code = newCode;
    this.name = newName;
  }

  /**
   * @return OpenCV interpolation code, or -1 if not defined
   */
  public int getCode() {
    return code;
  }

  /**
   * @return name to be written in metadata
   */
  public String getName() {
    return name;
  }
}
