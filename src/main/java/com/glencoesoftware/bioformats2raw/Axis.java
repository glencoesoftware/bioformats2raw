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

  private String type;
  private int length;
  private int chunkSize;
  private String dimensionType;

  /**
   * Create a new Axis.
   *
   * @param t axis type (e.g. 'X')
   * @param len axis length
   * @param chunk chunk length (expected to be in range [1, len])
   * @param dimType Zarr dimension type e.g. 'space'
   */
  public Axis(char t, int len, int chunk, String dimType) {
    this(String.valueOf(t), len, chunk, dimType);
  }

  /**
   * Create a new Axis.
   *
   * @param t axis type (e.g. 'X')
   * @param len axis length
   * @param chunk chunk length (expected to be in range [1, len])
   * @param dimType Zarr dimension type e.g. 'space'
   */
  public Axis(String t, int len, int chunk, String dimType) {
    type = t;
    length = len;
    chunkSize = chunk;
    dimensionType = dimType;
  }

  /**
   * @return axis type (e.g. 'X')
   */
  public String getType() {
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

  /**
   * @return dimension type e.g. 'space'
   */
  public String getDimensionType() {
    return dimensionType;
  }

}
