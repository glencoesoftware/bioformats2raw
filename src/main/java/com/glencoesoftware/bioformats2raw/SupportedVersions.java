/**
 * Copyright (c) 2025 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.bioformats2raw;

import java.util.Arrays;
import java.util.List;

public enum SupportedVersions {
  NGFF_01("0.1", 2, null),
  NGFF_04("0.4", 2, null),
  NGFF_05("0.5", 3, new Integer[] {2}),
  NGFF_DEV("0.9.dev1", 3, new Integer[] {2, 3, 5});

  private final String value;
  private final int zarrVersion;
  private final List<Integer> supportedRFCs;

  private SupportedVersions(
    final String value, int zarrVersion, Integer[] rfcs)
  {
    this.value = value;
    this.zarrVersion = zarrVersion;
    if (rfcs != null) {
      this.supportedRFCs = Arrays.asList(rfcs);
    }
    else {
      this.supportedRFCs = null;
    }
  }

  /**
   * @return the version of the Zarr format used by this OME-Zarr version
   */
  public int getZarrVersion() {
    return zarrVersion;
  }

  /**
   * @return true if extra dimensions (RFC-3) are supported by this version
   */
  public boolean supportsExtraDimensions() {
    return supportedRFCs != null && supportedRFCs.contains(3);
  }

  /**
   * @return true if coordinate systems/transformations (RFC-5)
   *              are supported by this version
   */
  public boolean supportsCoordinateSystems() {
    return supportedRFCs != null && supportedRFCs.contains(5);
  }

  @Override
  public String toString() {
    return value;
  }
}
