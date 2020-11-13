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

  private int plate = -1;
  private int plateAcquisition = -1;
  private int wellRow = -1;
  private int wellColumn = -1;
  private int field = -1;

  /**
   * Construct an HCSIndex object representing an Image/series,
   * using the supplied IMetadata to calculate indexes.
   *
   * @param meta IMetadata object containing HCS metadata
   * @param series OME Image/Bio-Formats series index
   */
  public HCSIndex(IMetadata meta, int series) {
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
                  plateAcquisition = pa;
                  break;
                }
              }
              if (plateAcquisition >= 0) {
                break;
              }
            }
            return;
          }
        }
      }
    }
  }

  /**
   * @return plate index
   */
  public int getPlateIndex() {
    return plate;
  }

  /**
   * Set plate index.
   *
   * @param plateIndex plate index
   */
  public void setPlateIndex(int plateIndex) {
    this.plate = plateIndex;
  }

  /**
   * @return plate acquisition index
   */
  public int getPlateAcquisitionIndex() {
    return plateAcquisition;
  }

  /**
   * Set plate acquisition index.
   *
   * @param plateAcquisitionIndex plate acquisition index
   */
  public void setPlateAcquisitionIndex(int plateAcquisitionIndex) {
    this.plateAcquisition = plateAcquisitionIndex;
  }

  /**
   * @return well row index
   */
  public int getWellRowIndex() {
    return wellRow;
  }

  /**
   * Set well row index.
   *
   * @param wellRowIndex well row index
   */
  public void setWellRowIndex(int wellRowIndex) {
    this.wellRow = wellRowIndex;
  }

  /**
   * @return well column index
   */
  public int getWellColumnIndex() {
    return wellColumn;
  }

  /**
   * Set well column index.
   *
   * @param wellColumnIndex well column index
   */
  public void setWellColumnIndex(int wellColumnIndex) {
    this.wellColumn = wellColumnIndex;
  }

  /**
   * @return field index
   */
  public int getFieldIndex() {
    return field;
  }

  /**
   * Set field (well sample) index.
   *
   * @param fieldIndex field index
   */
  public void setFieldIndex(int fieldIndex) {
    this.field = fieldIndex;
  }

  /**
   * @return well path relative to the plate group
   */
  public String getWellPath() {
    return String.format("%d/%d/%d",
      getPlateAcquisitionIndex(),
      getWellRowIndex(),
      getWellColumnIndex());
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
