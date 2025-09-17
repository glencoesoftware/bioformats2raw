/**
 * Copyright (c) 2025 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.bioformats2raw;

import com.bc.zarr.DataType;
import loci.formats.FormatTools;

public final class ZarrTypes {

  /**
   * Convert Bio-Formats pixel type to UCAR array type.
   *
   * @param type Bio-Formats pixel type
   * @return UCAR array type
   */
  public static ucar.ma2.DataType getDataType(int type) {
    switch (type) {
      case FormatTools.INT8:
        return ucar.ma2.DataType.BYTE;
      case FormatTools.UINT8:
        return ucar.ma2.DataType.BYTE;
      case FormatTools.INT16:
        return ucar.ma2.DataType.SHORT;
      case FormatTools.UINT16:
        return ucar.ma2.DataType.SHORT;
      case FormatTools.INT32:
        return ucar.ma2.DataType.INT;
      case FormatTools.UINT32:
        return ucar.ma2.DataType.INT;
      case FormatTools.FLOAT:
        return ucar.ma2.DataType.FLOAT;
      case FormatTools.DOUBLE:
        return ucar.ma2.DataType.DOUBLE;
      default:
        throw new IllegalArgumentException("Unsupported pixel type: "
            + FormatTools.getPixelTypeString(type));
    }
  }

  /**
   * Convert Bio-Formats pixel type to Zarr v3 data type.
   *
   * @param type Bio-Formats pixel type
   * @return corresponding Zarr v3 data type
   */
  public static dev.zarr.zarrjava.v3.DataType getV3ZarrType(int type) {
    switch (type) {
      case FormatTools.INT8:
        return dev.zarr.zarrjava.v3.DataType.INT8;
      case FormatTools.UINT8:
        return dev.zarr.zarrjava.v3.DataType.UINT8;
      case FormatTools.INT16:
        return dev.zarr.zarrjava.v3.DataType.INT16;
      case FormatTools.UINT16:
        return dev.zarr.zarrjava.v3.DataType.UINT16;
      case FormatTools.INT32:
        return dev.zarr.zarrjava.v3.DataType.INT32;
      case FormatTools.UINT32:
        return dev.zarr.zarrjava.v3.DataType.UINT32;
      case FormatTools.FLOAT:
        return dev.zarr.zarrjava.v3.DataType.FLOAT32;
      case FormatTools.DOUBLE:
        return dev.zarr.zarrjava.v3.DataType.FLOAT64;
      default:
        throw new IllegalArgumentException("Unsupported pixel type: "
            + FormatTools.getPixelTypeString(type));
    }
  }

  /**
   * Convert Bio-Formats pixel type to Zarr data type.
   *
   * @param type Bio-Formats pixel type
   * @return corresponding Zarr data type
   */
  public static DataType getZarrType(int type) {
    switch (type) {
      case FormatTools.INT8:
        return DataType.i1;
      case FormatTools.UINT8:
        return DataType.u1;
      case FormatTools.INT16:
        return DataType.i2;
      case FormatTools.UINT16:
        return DataType.u2;
      case FormatTools.INT32:
        return DataType.i4;
      case FormatTools.UINT32:
        return DataType.u4;
      case FormatTools.FLOAT:
        return DataType.f4;
      case FormatTools.DOUBLE:
        return DataType.f8;
      default:
        throw new IllegalArgumentException("Unsupported pixel type: "
            + FormatTools.getPixelTypeString(type));
    }
  }

  /**
   * Return the number of bytes per pixel for a JZarr data type.
   * @param dataType type to return number of bytes per pixel for
   * @return See above.
   */
  public static int bytesPerPixel(DataType dataType) {
    switch (dataType) {
      case i1:
      case u1:
        return 1;
      case i2:
      case u2:
        return 2;
      case i4:
      case u4:
      case f4:
        return 4;
      case f8:
        return 8;
      default:
        throw new IllegalArgumentException(
            "Unsupported data type: " + dataType);
    }
  }

  /**
   * Get the minimum and maximum pixel values for the given pixel type.
   *
   * @param bfPixelType pixel type as defined in FormatTools
   * @return array of length 2 representing the minimum and maximum
   *         pixel values, or null if converting to the given type is
   *         not supported
   */
  public static double[] getRange(int bfPixelType) {
    double[] range = new double[2];
    switch (bfPixelType) {
      case FormatTools.INT8:
        range[0] = -128.0;
        range[1] = 127.0;
        break;
      case FormatTools.UINT8:
        range[0] = 0.0;
        range[1] = 255.0;
        break;
      case FormatTools.INT16:
        range[0] = -32768.0;
        range[1] = 32767.0;
        break;
      case FormatTools.UINT16:
        range[0] = 0.0;
        range[1] = 65535.0;
        break;
      default:
        return null;
    }

    return range;
  }

}
