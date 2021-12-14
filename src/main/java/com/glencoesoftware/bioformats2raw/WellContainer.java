/**
 * Copyright (c) 2021 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.bioformats2raw;

import java.util.ArrayList;
import java.util.List;

import loci.formats.MetadataTools;
import loci.formats.meta.MetadataStore;
import ome.xml.model.primitives.NonNegativeInteger;

/**
 * Represents a well in a plate, including the files that make up the well.
 */
public abstract class WellContainer implements Comparable<WellContainer> {

  private int plateAcquisitionIndex;
  private int rowIndex;
  private int columnIndex;
  private transient int wellIndex;
  private int fieldCount;

  private List<String> files = new ArrayList<String>();

  /**
   * Construct an empty well.
   */
  public WellContainer() {
  }

  /**
   * Construct a well in particular location on the plate.
   *
   * @param pa plate acquisition index
   * @param row row index
   * @param col column index
   * @param fields number of fields in this well
   */
  public WellContainer(int pa, int row, int col, int fields) {
    plateAcquisitionIndex = pa;
    rowIndex = row;
    columnIndex = col;
    fieldCount = fields;
  }

  /**
   * Get the appropriate file for the given field and plane.
   *
   * @param fieldIndex field index
   * @param planeIndex plane index
   * @return corresponding pixels file, or null if indexes are invalid
   */
  public String getFile(int fieldIndex, int planeIndex) {
    String[] fieldFiles = getFiles(fieldIndex);
    if (fieldFiles != null && planeIndex < fieldFiles.length) {
      return fieldFiles[planeIndex];
    }
    return null;
  }

  /**
   * Get an array of files for the given field.
   *
   * @param fieldIndex field index
   * @return any files used for the given field
   */
  public abstract String[] getFiles(int fieldIndex);

  /**
   * Get a list of all files used by this well.
   *
   * @return all used files
   */
  public List<String> getAllFiles() {
    return files;
  }

  /**
   * Add a file to the list of files used by this well.
   * Does not perform any field/plane to file mapping.
   *
   * @param file file path
   */
  public void addFile(String file) {
    files.add(file);
  }

  /**
   * @return this well's plate acquisition index
   */
  public int getPlateAcquisition() {
    return plateAcquisitionIndex;
  }

  /**
   * @param pa new plate acquisition index for this well
   */
  public void setPlateAcquisition(int pa) {
    plateAcquisitionIndex = pa;
  }

  /**
   * @return this well's row index
   */
  public int getRowIndex() {
    return rowIndex;
  }

  /**
   * @param newRow new row index for this well
   */
  public void setRowIndex(int newRow) {
    rowIndex = newRow;
  }

  /**
   * @return this well's column index
   */
  public int getColumnIndex() {
    return columnIndex;
  }

  /**
   * @param newColumn new column index for this well
   */
  public void setColumnIndex(int newColumn) {
    columnIndex = newColumn;
  }

  /**
   * @return the number of fields in this well
   */
  public int getFieldCount() {
    return fieldCount;
  }

  /**
   * @param fields new field count for this well
   */
  public void setFieldCount(int fields) {
    fieldCount = fields;
  }

  /**
   * @return the well index (independent of row/column)
   */
  public int getWellIndex() {
    return wellIndex;
  }

  /**
   * @param index the new index for this well (independent of row/column)
   */
  public void setWellIndex(int index) {
    wellIndex = index;
  }

  /**
   * Populate a MetadataStore using this well.
   *
   * @param store target MetadataStore
   * @param plate plate index
   * @param plateAcq plate acquisition index
   * @param wellSampleStart index of first field in this well
   * @param nextImage index of first image linked to this well
   */
  public void fillMetadataStore(MetadataStore store, int plate, int plateAcq,
    int wellSampleStart, int nextImage)
  {
    fillMetadataStore(store, plate, plateAcq,
      wellIndex, wellSampleStart, nextImage);
  }

  /**
   * Populate a MetadataStore using this well.
   *
   * @param store target MetadataStore
   * @param plate plate index
   * @param plateAcq plate acquisition index
   * @param well index of this well
   * @param wellSampleStart index of first field in this well
   * @param nextImage index of first image linked to this well
   */
  public void fillMetadataStore(MetadataStore store, int plate, int plateAcq,
    int well, int wellSampleStart, int nextImage)
  {
    store.setWellID(MetadataTools.createLSID("Well", plate, well), plate, well);
    store.setWellRow(new NonNegativeInteger(getRowIndex()), plate, well);
    store.setWellColumn(new NonNegativeInteger(getColumnIndex()), plate, well);

    for (int field=0; field<getFieldCount(); field++) {
      int wellSampleIndex = field + wellSampleStart;
      String wellSample =
        MetadataTools.createLSID("WellSample", plate, well, wellSampleIndex);
      store.setWellSampleID(wellSample, plate, well, wellSampleIndex);

      int imageIndex = nextImage + field;
      store.setWellSampleIndex(
        new NonNegativeInteger(imageIndex), plate, well, wellSampleIndex);
      String imageID = MetadataTools.createLSID("Image", imageIndex);
      store.setImageID(imageID, imageIndex);
      store.setWellSampleImageRef(imageID, plate, well, wellSampleIndex);
      store.setPlateAcquisitionWellSampleRef(
        wellSample, plate, plateAcq, imageIndex);
    }
  }

  @Override
  public int compareTo(WellContainer w) {
    int paDiff = getPlateAcquisition() - w.getPlateAcquisition();
    if (paDiff != 0) {
      return paDiff;
    }
    int rowDiff = getRowIndex() - w.getRowIndex();
    if (rowDiff != 0) {
      return rowDiff;
    }
    return getColumnIndex() - w.getColumnIndex();
  }

}
