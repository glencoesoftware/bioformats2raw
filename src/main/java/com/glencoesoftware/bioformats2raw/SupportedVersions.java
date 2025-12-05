/**
 * Copyright (c) 2025 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.bioformats2raw;

public enum SupportedVersions {
  NGFF_01("0.1"),
  NGFF_04("0.4"),
  NGFF_05("0.5");

  private final String value;

  private SupportedVersions(final String value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return value;
  }
}
