/**
 * Copyright (c) 2019 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.bioformats2raw;

import loci.common.DataTools;
import loci.formats.FormatTools;

import org.opencv.core.Core;
import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.core.Size;
import org.opencv.imgproc.Imgproc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenCVTools {

  private static final Logger LOGGER =
    LoggerFactory.getLogger(OpenCVTools.class);

  /**
   * Attempt to load native libraries associated with OpenCV.
   * If library loading fails, any exceptions are caught and
   * logged so that simple downsampling can still be used.
   */
  public static void loadOpenCV() {
    try {
      nu.pattern.OpenCV.loadLocally();
      return;
    }
    catch (Throwable e) {
      LOGGER.warn("Could not load OpenCV libraries", e);
    }
    try {
      System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
    }
    catch (Throwable e) {
      LOGGER.warn(
        "Could not load native library " + Core.NATIVE_LIBRARY_NAME, e);
    }
  }

  /**
   * Convert Bio-Formats pixel type to OpenCV pixel type.
   *
   * @param bfPixelType Bio-Formats pixels type
   * @return corresponding OpenCV pixel type
   */
  public static int getCvType(int bfPixelType) {
    switch (bfPixelType) {
      case FormatTools.UINT8:
        return CvType.CV_8U;
      case FormatTools.INT8:
        return CvType.CV_8S;
      case FormatTools.UINT16:
        return CvType.CV_16U;
      case FormatTools.INT16:
        return CvType.CV_16S;
      case FormatTools.INT32:
        return CvType.CV_32S;
      case FormatTools.FLOAT:
        return CvType.CV_32F;
      case FormatTools.DOUBLE:
        return CvType.CV_64F;
      default:
        throw new IllegalArgumentException(
          "Unsupported pixel type: " +
           FormatTools.getPixelTypeString(bfPixelType));
    }
  }

  /**
   * @return OpenCV version
   */
  public static String getVersion() {
    return Core.VERSION;
  }

  /**
   * Downsample the given tile.
   *
   * @param tile input pixel bytes
   * @param pixelType Bio-Formats pixel type
   * @param width input tile width
   * @param height input tile height
   * @param scale downsampling scale factor
   * @param downsampling downsampling algorithm
   * @return downsampled pixel bytes
   */
  public static byte[] downsample(
    byte[] tile, int pixelType, int width, int height, int scale,
    Downsampling downsampling)
  {
    boolean floatingPoint = FormatTools.isFloatingPoint(pixelType);
    int bytesPerPixel = FormatTools.getBytesPerPixel(pixelType);
    Object pixels =
      DataTools.makeDataArray(tile, bytesPerPixel, floatingPoint, false);
    int scaleWidth = width / scale;
    int scaleHeight = height / scale;

    int cvType = OpenCVTools.getCvType(pixelType);
    Mat sourceMat = new Mat(height, width, cvType);
    Mat destMat = new Mat(scaleHeight, scaleWidth, cvType);
    Size destSize = new Size(scaleWidth, scaleHeight);

    try {
      if (pixels instanceof byte[]) {
        sourceMat.put(0, 0, (byte[]) pixels);
        opencvDownsample(sourceMat, destMat, destSize, downsampling);
        byte[] dest = new byte[scaleWidth * scaleHeight];
        destMat.get(0, 0, dest);
        return dest;
      }
      else if (pixels instanceof short[]) {
        sourceMat.put(0, 0, (short[]) pixels);
        opencvDownsample(sourceMat, destMat, destSize, downsampling);
        short[] dest = new short[scaleWidth * scaleHeight];
        destMat.get(0, 0, dest);
        return DataTools.shortsToBytes(dest, false);
      }
      else if (pixels instanceof int[]) {
        sourceMat.put(0, 0, (int[]) pixels);
        opencvDownsample(sourceMat, destMat, destSize, downsampling);
        int[] dest = new int[scaleWidth * scaleHeight];
        destMat.get(0, 0, dest);
        return DataTools.intsToBytes(dest, false);
      }
      else if (pixels instanceof float[]) {
        sourceMat.put(0, 0, (float[]) pixels);
        opencvDownsample(sourceMat, destMat, destSize, downsampling);
        float[] dest = new float[scaleWidth * scaleHeight];
        destMat.get(0, 0, dest);
        return DataTools.floatsToBytes(dest, false);
      }
      else if (pixels instanceof double[]) {
        sourceMat.put(0, 0, (double[]) pixels);
        opencvDownsample(sourceMat, destMat, destSize, downsampling);
        double[] dest = new double[scaleWidth * scaleHeight];
        destMat.get(0, 0, dest);
        return DataTools.doublesToBytes(dest, false);
      }
    }
    finally {
      sourceMat.release();
      destMat.release();
    }
    throw new IllegalArgumentException(
      "Unsupported array type: " + pixels.getClass());
  }

  /**
   * Downsample the given source tile using OpenCV.
   *
   * @param source source matrix
   * @param dest destination matrix
   * @param destSize destination matrix size
   * @param downsampling downsampling algorithm
   */
  public static void opencvDownsample(Mat source, Mat dest, Size destSize,
    Downsampling downsampling)
  {
    if (downsampling == Downsampling.GAUSSIAN) {
      Imgproc.pyrDown(source, dest, destSize);
    }
    else {
      Imgproc.resize(source, dest, destSize, 0, 0, downsampling.getCode());
    }
  }

}
