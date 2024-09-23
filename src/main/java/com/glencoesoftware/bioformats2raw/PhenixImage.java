/**
 * Copyright (c) 2021 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.bioformats2raw;

import loci.common.DataTools;
import loci.common.IniTable;

/**
 * Represents a single image (one 2D plane) in an Opera Phenix database plate.
 */
public class PhenixImage {

  /**
   * Unique index of this plane.
   * This is for sorting purposes only, and may not correspond to the
   * well, field, series, plane, etc.
   */
  public int index;

  /**
   * Non-unique Image ID.
   * All images in the plate will have the same ID, so this should not be
   * used for sorting or lookups.
   */
  public String id;

  /** Well row, indexed from 1. */
  public int row;

  /** Well column, indexed from 1. */
  public int col;

  /** Field, indexed from 1. */
  public int field;

  /** Calculated series index, from 0. */
  public int series;

  /** Plane, indexed from 1. */
  public int plane;

  /** Channel, indexed from 1. */
  public int channel;

  /** Channel name. */
  public String channelName;

  /** Exposure time in seconds. */
  public Double exposureTime;

  /** Filter wavelength in nm. */
  public Double filterWavelength;

  /** Laser wavelength in nm. */
  public Double wavelength;

  /** Image acquisition date. */
  public String acquisitionDate;

  /** Absolute Z position. */
  public Double zPosition;

  /** Temperature at acquisition. */
  public Double temperature;

  /** Absolute file path. */
  public String filename;

  /**
   * Construct an empty PhenixImage.
   */
  public PhenixImage() {
  }

  /**
   * Construct a PhenixImage representing a single plane,
   * using the given INI data.
   *
   * @param table INI table containing image metadata
   */
  public PhenixImage(IniTable table) {
    id = table.get("id");
    index = Integer.parseInt(table.get("index"));
    row = Integer.parseInt(table.get("row"));
    col = Integer.parseInt(table.get("col"));
    field = Integer.parseInt(table.get("field"));
    series = Integer.parseInt(table.get("series"));
    plane = Integer.parseInt(table.get("plane"));
    channel = Integer.parseInt(table.get("channel"));
    acquisitionDate = table.get("acquisitionDate");
    zPosition = DataTools.parseDouble(table.get("zPosition"));
    temperature = DataTools.parseDouble(table.get("temperature"));
    filename = table.get("filename");
    channelName = table.get("channelName");
    exposureTime = DataTools.parseDouble(table.get("exposureTime"));
    wavelength = DataTools.parseDouble(table.get("laserWavelength"));
    filterWavelength = DataTools.parseDouble(table.get("filterWavelength"));
  }

  /**
   * @return populated INI table representing this image
   */
  public IniTable getIniTable() {
    IniTable table = new IniTable();
    table.put(IniTable.HEADER_KEY, "Image " + index);
    table.put("id", id);
    table.put("index", String.valueOf(index));
    table.put("row", String.valueOf(row));
    table.put("col", String.valueOf(col));
    table.put("field", String.valueOf(field));
    table.put("series", String.valueOf(series));
    table.put("plane", String.valueOf(plane));
    table.put("channel", String.valueOf(channel));
    table.put("acquisitionDate", acquisitionDate);
    table.put("zPosition", String.valueOf(zPosition));
    table.put("temperature", String.valueOf(temperature));
    table.put("filename", filename);
    table.put("channelName", channelName);
    table.put("exposureTime", String.valueOf(exposureTime));
    table.put("laserWavelength", String.valueOf(wavelength));
    table.put("filterWavelength", String.valueOf(filterWavelength));
    return table;
  }

  @Override
  public String toString() {
    return String.format("ID %d: %s", id, filename);
  }

}
