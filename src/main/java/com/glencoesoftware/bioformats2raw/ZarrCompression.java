/**
 * Copyright (c) 2020 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.bioformats2raw;


public enum ZarrCompression {
  raw("null"),
  zlib("zlib"),
  blosc("blosc");

  private final String value;

  private ZarrCompression(final String value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return value;
  }
}
