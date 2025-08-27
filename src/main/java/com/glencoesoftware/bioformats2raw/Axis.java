/**
 * Copyright (c) 2025 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.bioformats2raw;

/**
 * Describe an axis, including type and dimensions.
 */
public class Axis {

  private char type;
  private int length;
  private int chunkSize;

  /**
   * Create a new Axis.
   *
   * @param t axis type (e.g. 'X')
   * @param len axis length
   * @param chunk chunk length (expected to be in range [1, len])
   */
  public Axis(char t, int len, int chunk) {
    type = t;
    length = len;
    chunkSize = chunk;
  }

  /**
   * @return axis type (e.g. 'X')
   */
  public char getType() {
    return type;
  }

  /**
   * @return axis length
   */
  public int getLength() {
    return length;
  }

  /**
   * @return chunk length
   */
  public int getChunkSize() {
    return chunkSize;
  }

}
