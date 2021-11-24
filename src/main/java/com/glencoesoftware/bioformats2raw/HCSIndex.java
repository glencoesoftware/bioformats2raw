/**
 * Copyright (c) 2020 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.bioformats2raw;

import loci.formats.meta.IMetadata;

public class HCSIndex {

  private final int plate;
  private final Integer plateAcquisition;
  private final int wellRow;
  private final int wellColumn;
  private final int field;

  /**
   * Construct an HCSIndex object representing an Image/series,
   * using the supplied IMetadata to calculate indexes.
   *
   * @param meta IMetadata object containing HCS metadata
   * @param series OME Image/Bio-Formats series index
   */
  public HCSIndex(IMetadata meta, int series) {
    Integer thePlateAcquisition = null;
    for (int p=0; p<meta.getPlateCount(); p++) {
      for (int w=0; w<meta.getWellCount(p); w++) {
        for (int ws=0; ws<meta.getWellSampleCount(p, w); ws++) {
          if (meta.getWellSampleIndex(p, w, ws).getValue() == series) {
            plate = p;
            field = ws;
            wellRow = meta.getWellRow(p, w).getValue();
            wellColumn = meta.getWellColumn(p, w).getValue();
            String id = meta.getWellSampleID(p, w, ws);

            for (int pa=0; pa<meta.getPlateAcquisitionCount(p); pa++) {
              for (int s=0; s<meta.getWellSampleRefCount(p, pa); s++) {
                String refID = meta.getPlateAcquisitionWellSampleRef(p, pa, s);
                if (id.equals(refID)) {
                  thePlateAcquisition = pa;
                  break;
                }
              }
              if (thePlateAcquisition >= 0) {
                break;
              }
            }
            plateAcquisition = thePlateAcquisition;
            return;
          }
        }
      }
    }
    throw new IllegalArgumentException(
        "Series " + series + " not present in metadata!");
  }

  /**
   * @return plate index
   */
  public int getPlateIndex() {
    return plate;
  }

  /**
   * @return plate acquisition index or <code>null</code> if no related plate
   * acquisition can be found
   */
  public Integer getPlateAcquisitionIndex() {
    return plateAcquisition;
  }

  /**
   * @return well row index
   */
  public int getWellRowIndex() {
    return wellRow;
  }

  /**
   * @return well column index
   */
  public int getWellColumnIndex() {
    return wellColumn;
  }

  /**
   * @return field index
   */
  public int getFieldIndex() {
    return field;
  }

  /**
   * @return row path relative to the plate group
   */
  public String getRowPath() {
    return String.format("%d", getWellRowIndex());
  }

  /**
   * @return column path relative to the row group
   */
  public String getColumnPath() {
    return String.format("%d", getWellColumnIndex());
  }

  /**
   * @return well path relative to the plate group
   */
  public String getWellPath() {
    return String.format("%s/%s", getRowPath(), getColumnPath());
  }

  @Override
  public String toString() {
    return String.format("plate=%d, plateAcq=%d, row=%d, col=%d, field=%d",
      getPlateIndex(),
      getPlateAcquisitionIndex(),
      getWellRowIndex(),
      getWellColumnIndex(),
      getFieldIndex());
  }

}
