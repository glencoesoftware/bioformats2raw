/**
 * Copyright (c) 2020 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */

package com.glencoesoftware.bioformats2raw;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import loci.common.RandomAccessInputStream;
import loci.common.services.DependencyException;
import loci.common.services.ServiceException;
import loci.common.services.ServiceFactory;
import loci.formats.CoreMetadata;
import loci.formats.FormatException;
import loci.formats.FormatTools;
import loci.formats.MissingLibraryException;
import loci.formats.in.BaseTiffReader;
import loci.formats.meta.IMetadata;
import loci.formats.meta.MetadataConverter;
import loci.formats.meta.MetadataStore;
import loci.formats.services.OMEXMLService;
import loci.formats.services.OMEXMLServiceImpl;
import loci.formats.tiff.IFD;
import loci.formats.tiff.PhotoInterp;
import loci.formats.tiff.TiffCompression;
import loci.formats.tiff.TiffParser;

import ome.xml.meta.OMEXMLMetadataRoot;
import ome.xml.model.Image;
import ome.xml.model.Pixels;
import ome.xml.model.TiffData;

/**
 * PyramidTiffReader is the file format reader for pyramid TIFFs.
 */
public class PyramidTiffReader extends BaseTiffReader {

  // -- Constants --

  /** Logger for this class. */
  private static final Logger LOGGER =
    LoggerFactory.getLogger(PyramidTiffReader.class);

  // -- Fields --

  private IMetadata omexml = null;;

  // -- Constructor --

  /** Constructs a new pyramid TIFF reader. */
  public PyramidTiffReader() {
    super("Pyramid TIFF", new String[] {"tif", "tiff"});
    domains = new String[] {FormatTools.EM_DOMAIN};
    suffixSufficient = false;
    suffixNecessary = false;
    equalStrips = true;
    noSubresolutions = true;
    canSeparateSeries = false;
  }

  // -- IFormatReader API methods --

  /* @see loci.formats.IFormatReader#isThisType(RandomAccessInputStream) */
  @Override
  public boolean isThisType(RandomAccessInputStream stream) throws IOException {
    TiffParser parser = new TiffParser(stream);
    parser.setAssumeEqualStrips(equalStrips);
    IFD ifd = parser.getFirstIFD();
    if (ifd == null) {
      return false;
    }
    String software = ifd.getIFDTextValue(IFD.SOFTWARE);
    if (software != null && software.indexOf("Faas") >= 0) {
      return true;
    }

    // compare width and height of first and last IFD
    // all IFDs could be checked, but that may affect performance
    try {
      long[] offsets = parser.getIFDOffsets();
      if (offsets.length == 1) {
        return false;
      }
      IFD lastIFD = parser.getIFD(offsets[offsets.length - 1]);

      if (lastIFD.getImageWidth() >= ifd.getImageWidth() ||
        lastIFD.getImageLength() >= ifd.getImageLength())
      {
        return false;
      }
      if (lastIFD.getSamplesPerPixel() != ifd.getSamplesPerPixel()) {
        return false;
      }
      if (lastIFD.getPixelType() != ifd.getPixelType()) {
        return false;
      }

      int powerOfTwo = 0;
      long width = ifd.getImageWidth();
      while (width > lastIFD.getImageWidth()) {
        width /= 2;
        powerOfTwo++;
      }
      // don't accept the file if the number of IFDs is not
      // a multiple of the detected resolution count
      if (offsets.length % (powerOfTwo + 1) != 0) {
        return false;
      }

      long height = ifd.getImageLength() / (int) Math.pow(2, powerOfTwo);
      return height == lastIFD.getImageLength();
    }
    catch (FormatException e) {
      LOGGER.trace("Could not finish type checking", e);
    }
    return false;
  }

  /**
   * @see loci.formats.IFormatReader#close(boolean)
   */
  @Override
  public void close(boolean fileOnly) throws IOException {
    super.close(fileOnly);
    if (!fileOnly) {
      omexml = null;
    }
  }

  /**
   * @see loci.formats.IFormatReader#openBytes(int, byte[], int, int, int, int)
   */
  @Override
  public byte[] openBytes(int no, byte[] buf, int x, int y, int w, int h)
    throws FormatException, IOException
  {
    FormatTools.checkPlaneParameters(this, no, buf.length, x, y, w, h);
    int index = getCoreIndex() * getImageCount() + no;
    IFD ifd = ifds.get(index);
    // strip or tile sizes will vary if any kind of compression was used
    // so not safe to assume that they have equal length
    TiffCompression compression = ifd.getCompression();
    tiffParser.setAssumeEqualStrips(
      compression == TiffCompression.UNCOMPRESSED ||
      compression == TiffCompression.DEFAULT_UNCOMPRESSED);
    tiffParser.getSamples(ifds.get(index), buf, x, y, w, h);
    return buf;
  }

  /* @see loci.formats.IFormatReader#getOptimalTileWidth() */
  @Override
  public int getOptimalTileWidth() {
    FormatTools.assertId(currentId, true, 1);
    try {
      return (int) ifds.get(getCoreIndex() * getImageCount()).getTileWidth();
    }
    catch (FormatException e) {
      LOGGER.debug("", e);
    }
    return super.getOptimalTileWidth();
  }

  /* @see loci.formats.IFormatReader#getOptimalTileHeight() */
  @Override
  public int getOptimalTileHeight() {
    FormatTools.assertId(currentId, true, 1);
    try {
      return (int) ifds.get(getCoreIndex() * getImageCount()).getTileLength();
    }
    catch (FormatException e) {
      LOGGER.debug("", e);
    }
    return super.getOptimalTileHeight();
  }

  // -- Internal BaseTiffReader API methods --

  /* @see loci.formats.in.BaseTiffReader#initStandardMetadata() */
  @Override
  protected void initStandardMetadata() throws FormatException, IOException {
    // count number of consecutive planes with the same XY dimensions

    ifds.addAll(thumbnailIFDs);

    int nPlanes = 1;
    long baseWidth = ifds.get(0).getImageWidth();
    long baseHeight = ifds.get(0).getImageLength();
    for (int i=1; i<ifds.size(); i++) {
      long width = ifds.get(i).getImageWidth();
      long height = ifds.get(i).getImageLength();
      if (width == baseWidth && height == baseHeight) {
        nPlanes++;
      }
      else {
        break;
      }
    }

    int seriesCount = ifds.size() / nPlanes;

    String comment = ifds.get(0).getComment();
    if (comment != null && comment.length() > 0) {
      try {
        ServiceFactory factory = new ServiceFactory();
        OMEXMLService service = factory.getInstance(OMEXMLService.class);
        omexml = service.createOMEXMLMetadata(comment);
      }
      catch (DependencyException de) {
        throw new MissingLibraryException(OMEXMLServiceImpl.NO_OME_XML_MSG, de);
      }
      catch (ServiceException e) {
        LOGGER.debug("Could not parse comment as OME-XML", e);
      }
    }

    // repopulate core metadata
    core.clear();
    core.add();
    for (int s=0; s<seriesCount; s++) {
      CoreMetadata ms = new CoreMetadata();
      core.add(0, ms);

      if (s == 0) {
        ms.resolutionCount = seriesCount;
      }

      IFD ifd = ifds.get(s * nPlanes);

      PhotoInterp p = ifd.getPhotometricInterpretation();
      int samples = ifd.getSamplesPerPixel();
      ms.rgb = samples > 1 || p == PhotoInterp.RGB;

      long numTileRows = ifd.getTilesPerColumn() - 1;
      long numTileCols = ifd.getTilesPerRow() - 1;

      ms.sizeX = (int) ifd.getImageWidth();
      ms.sizeY = (int) ifd.getImageLength();
      if (omexml != null) {
        ms.sizeZ = omexml.getPixelsSizeZ(0).getValue();
        ms.sizeT = omexml.getPixelsSizeT(0).getValue();
        ms.sizeC = omexml.getPixelsSizeC(0).getValue();
        ms.dimensionOrder = omexml.getPixelsDimensionOrder(0).getValue();
      }
      else {
        ms.sizeZ = 1;
        ms.sizeT = 1;
        ms.sizeC = ms.rgb ? samples : 1;
        // assuming all planes are channels
        ms.sizeC *= nPlanes;
        ms.dimensionOrder = "XYCZT";
      }
      ms.littleEndian = ifd.isLittleEndian();
      ms.indexed = p == PhotoInterp.RGB_PALETTE &&
        (get8BitLookupTable() != null || get16BitLookupTable() != null);
      ms.imageCount = nPlanes;
      ms.pixelType = ifd.getPixelType();
      ms.metadataComplete = true;
      ms.interleaved = false;
      ms.falseColor = false;
      ms.thumbnail = s > 0;
    }
  }

  /* @see loci.formats.BaseTiffReader#initMetadataStore() */
  @Override
  protected void initMetadataStore() throws FormatException {
    boolean setImageNames = true;
    if (omexml == null) {
      super.initMetadataStore();
    }
    else {
      OMEXMLMetadataRoot root = (OMEXMLMetadataRoot) omexml.getRoot();
      List<Image> images = root.copyImageList();
      for (int i=0; i<images.size(); i++) {
        Image img = images.get(i);
        if (i > 0 && !hasFlattenedResolutions()) {
          root.removeImage(img);
          continue;
        }
        Pixels pix = img.getPixels();
        List<TiffData> tiffData = pix.copyTiffDataList();
        for (TiffData t : tiffData) {
          pix.removeTiffData(t);
        }
        if (img.getName() != null) {
          setImageNames = false;
        }
      }
      omexml.setRoot(root);

      MetadataConverter.convertMetadata(omexml, makeFilterMetadata());
    }

    if (setImageNames) {
      MetadataStore store = makeFilterMetadata();

      for (int i=0; i<getSeriesCount(); i++) {
        store.setImageName("Series " + (i + 1), i);
      }
    }
  }

}
