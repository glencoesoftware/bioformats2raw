/**
 * Copyright (c) 2019 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.bioformats2raw;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import loci.common.ByteArrayHandle;
import loci.common.DataTools;
import loci.common.DateTools;
import loci.common.IniList;
import loci.common.IniParser;
import loci.common.IniTable;
import loci.common.Location;
import loci.common.RandomAccessInputStream;
import loci.common.Region;
import loci.formats.ChannelSeparator;
import loci.formats.CoreMetadata;
import loci.formats.FormatException;
import loci.formats.FormatReader;
import loci.formats.FormatTools;
import loci.formats.MetadataTools;
import loci.formats.codec.CodecOptions;
import loci.formats.codec.JPEGCodec;
import loci.formats.codec.JPEG2000Codec;
import loci.formats.codec.JPEG2000CodecOptions;
import loci.formats.codec.JPEGXRCodec;
import loci.formats.codec.ZlibCodec;
import loci.formats.in.APNGReader;
import loci.formats.in.DynamicMetadataOptions;
import loci.formats.in.MetadataLevel;
import loci.formats.in.MetadataOptions;
import loci.formats.meta.IMinMaxStore;
import loci.formats.meta.MetadataStore;
import ome.units.quantity.Length;
import ome.units.quantity.Time;
import ome.units.UNITS;
import ome.xml.model.primitives.Color;
import ome.xml.model.primitives.Timestamp;

/**
 * MiraxReader is the file format reader for 3D Histech/Zeiss Mirax datasets.
 */
public class MiraxReader extends FormatReader {

  // -- Constants --

  private static final Logger LOGGER =
    LoggerFactory.getLogger(MiraxReader.class);

  private static final String[] DATE_FORMATS = new String[] {
    "dd/MM/yyyy HH:mm:ss", "yyyy.MM.dd HH:mm:ss.SSS"
  };

  private static final int MAX_TILE_SIZE = 256;

  private static final String SLIDE_DATA = "Slidedat.ini";

  /**
   * Maximum number of channels in a tile.  The assembled slide can
   * contain more channels, but then uses SizeC / MAX_CHANNELS tiles to do so.
   */
  private static final int MAX_CHANNELS = 3;

  /**
   * Linear scaling factor for each progressively smaller resolution.
   * This meant to be as close to 2.0 as possible, with a little breathing room
   * for overlap calculations
   */
  private static final double SCALE_FACTOR = 2.0;

  public static final String DIMENSIONS_KEY = "mirax.use_metadata_dimensions";
  public static final boolean DIMENSIONS_DEFAULT = true;

  // -- Fields --

  private int channelFillColor;
  private int pyramidDepth;
  private List<String> files = new ArrayList<String>();
  private String iniPath;
  private int[] tileWidth;
  private int[] tileHeight;
  private double[] overlapX;
  private double[] overlapY;
  private double[] tileRowCount;
  private double[] tileColCount;
  private List<String> format = new ArrayList<String>();

  /**
   * Offsets and other metadata for tiles indexed firstly by resolution,
   * secondly by tile counter and finally ordered by channel.
   */
  private SortedMap<TilePointer, List<TilePointer>> offsets = null;

  private int xTiles;
  private int yTiles;
  private int divPerSide;

  private int[] minColIndex;
  private int[] maxColIndex;
  private int[] minRowIndex;
  private int[] maxRowIndex;
  private int[][] tilePositions;

  private transient String tileFile;
  private transient RandomAccessInputStream tileStream;

  private ChannelSeparator pngReader = new ChannelSeparator(new APNGReader());
  private JPEGCodec jpegCodec = new JPEGCodec();
  private CodecOptions jpegOptions = new CodecOptions();

  private JPEG2000Codec jp2kCodec = new JPEG2000Codec();
  private JPEG2000CodecOptions jp2kOptions =
    JPEG2000CodecOptions.getDefaultOptions();

  private boolean fluorescence = false;

  private transient JPEGXRCodec jpegxrCodec = new JPEGXRCodec();

  private transient Cache<TilePointer, byte[]> tileCache;

  // -- Constructor --

  /** Constructs a new Mirax reader. */
  public MiraxReader() {
    super("Mirax", new String[] {"mrxs", "ini"});
    domains = new String[] {FormatTools.HISTOLOGY_DOMAIN};
    datasetDescription =
      "A directory of .ini and .dat files, and an optional .mrxs file";
    suffixNecessary = true;
    suffixSufficient = false;
  }

  // -- IFormatReader API methods --

  /* @see loci.formats.IFormatReader#isThisType(String, boolean) */
  @Override
  public boolean isThisType(String name, boolean open) {
    if (checkSuffix(name, "mrxs")) {
      return true;
    }
    return new Location(name).getName().equals(SLIDE_DATA);
  }

  /* @see loci.formats.IFormatReader#getOptimalTileWidth() */
  @Override
  public int getOptimalTileWidth() {
    FormatTools.assertId(currentId, true, 1);
    if (getCoreIndex() < pyramidDepth) {
      return MAX_TILE_SIZE;
    }
    return getSizeX();
  }

  /* @see loci.formats.IFormatReader#getOptimalTileHeight() */
  @Override
  public int getOptimalTileHeight() {
    FormatTools.assertId(currentId, true, 1);
    if (getCoreIndex() < pyramidDepth) {
      return MAX_TILE_SIZE;
    }
    return getSizeY();
  }

  /* @see loci.formats.IFormatReader#getSeriesUsedFiles(boolean) */
  @Override
  public String[] getSeriesUsedFiles(boolean noPixels) {
    FormatTools.assertId(currentId, true, 1);

    ArrayList<String> f = new ArrayList<String>();
    f.add(currentId);
    if (!iniPath.equals(currentId)) {
      f.add(iniPath);
    }
    for (String file : files) {
      if (!checkSuffix(file, "dat") || !noPixels) {
        f.add(file);
      }
    }

    return f.toArray(new String[f.size()]);
  }

  /* @see loci.formats.IFormatReader#setCoreIndex(int) */
  @Override
  public void setCoreIndex(int index) {
    super.setCoreIndex(index);
    closeTileStream();
  }

  /**
   * @see loci.formats.IFormatReader#openBytes(int, byte[], int, int, int, int)
   */
  @Override
  public byte[] openBytes(int no, byte[] buf, int x, int y, int w, int h)
    throws FormatException, IOException
  {
    FormatTools.checkPlaneParameters(this, no, buf.length, x, y, w, h);

    // set background color to black instead of the stored fill color
    // this is to match the default behavior of Pannoramic Viewer
    Arrays.fill(buf, getFillColor());

    if (tileCache == null) {
      tileCache = CacheBuilder.newBuilder()
        .maximumSize(0)  // Disabled
        .build();
    }

    int index = getCoreIndex();

    Region image = new Region(x, y, w, h);
    int pixel = FormatTools.getBytesPerPixel(getPixelType());
    int outputRowLen = w * pixel;

    boolean inPyramid = index < pyramidDepth;
    if (!inPyramid) {
      index = tileRowCount.length;
    }

    double div = getResolutionDivisions(index);

    int rowCount = inPyramid ? (int) Math.ceil(tileRowCount[index]) : 1;
    int colCount = inPyramid ? (int) Math.ceil(tileColCount[index]) : 1;

    int width = inPyramid ? tileWidth[index] : getSizeX();
    int height = inPyramid ? tileHeight[index] : getSizeY();
    int rowLen = pixel * width;
    int endX = x + w;
    int endY = y + h;

    double scale = divPerSide / div;
    for (int row=0; row<rowCount; row++) {
      for (int col=0; col<colCount; col++) {
        Region tileRegion = new Region(0, 0, width, height);
        if (tilePositions != null && index == 0) {
          int colIndex = (int) (minColIndex[index] + col * scale);
          int rowIndex = (int) (minRowIndex[index] + row * scale);
          int xp = (int) (colIndex / divPerSide);
          int yp = (int) (rowIndex / divPerSide);
          int tile = yp * (xTiles / divPerSide) + xp;
          int correctX = colIndex % (int) div;
          int correctY = rowIndex % (int) div;
          tileRegion.x =
            (int) (((tilePositions[tile][0] / scale) + (correctX * width)));
          tileRegion.y =
            (int) (((tilePositions[tile][1] / scale) + (correctY * height)));
        }

        Region intersection = tileRegion.intersection(image);
        if (!tileRegion.intersects(image)) {
          continue;
        }

        TilePointer thisOffset = lookupTile(index, col, row, no / MAX_CHANNELS);
        if (thisOffset != null) {
          int channel = no % MAX_CHANNELS;
          // 2 channel JPEG data needs to have the channel index inverted
          // 2 channel JPEG-2000 data should not have the channel index inverted
          if (fluorescence &&
            (getSizeC() != 2 || format.get(index).equals("JPEG")))
          {
            channel = MAX_CHANNELS - channel - 1;
          }

          String file = files.get(thisOffset.fileIndex + 1);
          byte[] tileBuf = tileCache.getIfPresent(thisOffset);
          if (tileBuf == null) {
            long nextOffset = thisOffset.offset + thisOffset.length;
            try {
              tileBuf = readTile(file, thisOffset.offset, nextOffset,
                format.get(index), channel);
            }
            catch (NullPointerException e) {
              LOGGER.warn("Could not read tile (file=" + file +
                ", offset="  + thisOffset.offset +
                ", nextOffset=" + nextOffset + ")", e);
            }
          }
          if (tileBuf != null) {
            int channelIndex = channel * (tileBuf.length / MAX_CHANNELS);

            // channels were separated during readTile
            if (tileBuf.length == width * height * pixel) {
              channelIndex = 0;
            }

            // overlap is confined to the edges of the tile,
            // so we can copy directly
            for (int trow=0; trow<intersection.height; trow++) {
              int src = channelIndex + pixel *
                (width * (intersection.y - tileRegion.y + trow) +
                intersection.x - tileRegion.x);
              int dest = pixel * (intersection.x - x) +
                outputRowLen * (trow + intersection.y - y);
              int copy =
                (int) Math.min(intersection.width * pixel, buf.length - dest);
              if (copy > 0) {
                System.arraycopy(tileBuf, src, buf, dest, copy);
              }
            }
            tileCache.put(thisOffset, tileBuf);
          }
        }
      }
    }

    return buf;
  }

  /** @see loci.formats.IFormatReader#close(boolean) */
  @Override
  public void close(boolean fileOnly) throws IOException {
    super.close(fileOnly);
    if (!fileOnly) {
      iniPath = null;
      files.clear();
      tileWidth = null;
      tileHeight = null;
      overlapX = null;
      overlapY = null;
      format.clear();
      offsets = null;
      pyramidDepth = 0;
      tileRowCount = null;
      tileColCount = null;
      xTiles = 0;
      yTiles = 0;
      divPerSide = 0;
      channelFillColor = 0;
      minColIndex = null;
      maxColIndex = null;
      minRowIndex = null;
      maxRowIndex = null;
      closeTileStream();
      tilePositions = null;
      pngReader.close();
      fluorescence = false;
    }
  }

  /* @see loci.formats.IFormatReader#reopenFile() */
  @Override
  public void reopenFile() throws IOException {
    super.reopenFile();
    // make sure we have usable tile offsets
    // see https://github.com/glencoesoftware/bioformats-private/pull/84 and
    // https://github.com/glencoesoftware/bioformats-private/pull/135
    //
    // PR 84 changed the type of 'offsets' from ArrayList to SortedMap
    // memo files generated prior to PR 84 can still be loaded with the
    // changes in PR 84 (since no variables were added or removed), but the
    // resulting 'offsets' is not usable due to the type change.
    // Calling lookupTile forces 'offsets' to be referenced, which results in
    // an IncompatibleClassChangeError.  Wrapping ICCE in an IOException forces
    // the Memoizer to automatically invalidate and regenerate the memo file.
    // Throwing IncompatibleClassChangeError here would require Memoizer to
    // catch Throwable, which is potentially dangerous.
    //
    // If a Memoizer is not in the reader stack, IncompatibleClassChangeError
    // should never be thrown, and the impact of a single
    // lookupTile call is minimal.
    try {
      lookupTile(0, 0, 0, 0);
    }
    catch (IncompatibleClassChangeError e) {
      throw new IOException(e);
    }
  }

  // -- Internal FormatReader API methods --

  /* @see loci.formats.FormatReader#initFile(String) */
  protected void initFile(String id) throws FormatException, IOException {
    super.initFile(id);

    Location slideDirectory = null;

    if (checkSuffix(id, "mrxs")) {
      slideDirectory = new Location(id.substring(0, id.lastIndexOf(".")));
      Location slideData = new Location(slideDirectory, SLIDE_DATA);
      iniPath = slideData.getAbsolutePath();
    }
    else {
      slideDirectory = new Location(id).getParentFile();
      iniPath = new Location(id).getAbsolutePath();
    }

    offsets = new TreeMap<TilePointer, List<TilePointer>>();

    String ini = DataTools.readFile(iniPath);
    ini = ini.substring(ini.indexOf("["));
    IniParser parser = new IniParser();
    IniList data = parser.parseINI(new BufferedReader(new StringReader(ini)));

    IniTable general = data.getTable("GENERAL");
    IniTable hierarchy = data.getTable("HIERARCHICAL");

    fluorescence = "SLIDE_TYPE_FLUORESCENCE".equals(general.get("SLIDE_TYPE"));

    xTiles = Integer.parseInt(general.get("IMAGENUMBER_X"));
    yTiles = Integer.parseInt(general.get("IMAGENUMBER_Y"));

    String divisions = general.get("CameraImageDivisionsPerSide");
    if (divisions != null) {
      divPerSide = Integer.parseInt(divisions);
    }
    else {
      divPerSide = 1;
    }

    String indexFile = hierarchy.get("INDEXFILE");
    Location index = new Location(slideDirectory, indexFile);
    if (!index.exists()) {
      throw new IOException(
        "Index file " + index.getAbsolutePath() + " missing");
    }
    indexFile = index.getAbsolutePath();
    files.add(indexFile);

    int nHierarchies = Integer.parseInt(hierarchy.get("HIER_COUNT"));

    pyramidDepth = Integer.parseInt(hierarchy.get("HIER_0_COUNT"));

    core.clear();
    for (int i=0; i<pyramidDepth; i++) {
      core.add(new CoreMetadata());
    }

    tileWidth = new int[pyramidDepth];
    tileHeight = new int[pyramidDepth];
    overlapX = new double[pyramidDepth];
    overlapY = new double[pyramidDepth];
    double[] pixelSizeX = new double[pyramidDepth];
    double[] pixelSizeY = new double[pyramidDepth];

    // find offsets to each tile

    IniTable fileTable = data.getTable("DATAFILE");
    int nFiles = Integer.parseInt(fileTable.get("FILE_COUNT"));

    for (int i=0; i<nFiles; i++) {
      Location f = new Location(slideDirectory, fileTable.get("FILE_" + i));
      if (!f.exists()) {
        throw new IOException("Data file " + f.getAbsolutePath() + " missing");
      }
      files.add(f.getAbsolutePath());
    }

    int pageSize = Integer.parseInt(hierarchy.get("PAGEELEMENTCOUNT"));

    RandomAccessInputStream indexData = new RandomAccessInputStream(indexFile);
    indexData.order(true);
    indexData.seek(37);
    long hierarchicalRoot = indexData.readInt();
    long nonHierarchicalRoot = indexData.readInt();
    indexData.seek(hierarchicalRoot);
    long[] listOffsets = new long[nHierarchies * pyramidDepth];
    for (int i=0; i<listOffsets.length; i++) {
      listOffsets[i] = indexData.readInt();
    }

    // read offsets to pyramid pixel data tiles
    for (int h=0; h<nHierarchies; h++) {
      for (int i=0; i<pyramidDepth; i++) {
        if (i == format.size()) {
          format.add("");
        }
        int levelIndex = h * pyramidDepth + i;
        if (listOffsets[levelIndex] == 0) {
          continue;
        }
        indexData.seek(listOffsets[levelIndex]);
        int nItems = indexData.readInt();
        if (nItems != 0) {
          LOGGER.warn("First page size should be 0, was " + nItems +
            "; skipping level " + i + " in hierarchy " + h);
          continue;
        }
        listOffsets[levelIndex] = indexData.readInt();

        if (listOffsets[levelIndex] == 0) {
          continue;
        }

        indexData.seek(listOffsets[levelIndex]);
        nItems = indexData.readInt();
        int nextPointer = indexData.readInt();
        int nextCounter = indexData.readInt();
        int itemCounter = 0;
        while (indexData.getFilePointer() <= indexData.length() - 16) {
          if (itemCounter == nItems) {
            if (nextPointer == 0) {
              break;
            }
            indexData.seek(nextPointer);
            nItems = indexData.readInt();
            nextPointer = indexData.readInt();
            nextCounter = indexData.readInt();
            itemCounter = 0;
          }
          long nextOffset = indexData.readInt();
          int length = indexData.readInt();
          int fileNumber = indexData.readInt();

          if (i == 0) {
            TilePointer key = new TilePointer(i, nextCounter);
            List<TilePointer> resolutionOffsets =
              getOrCreateResolutionOffsets(key);
            resolutionOffsets.add(
              new TilePointer(i, fileNumber, nextOffset, nextCounter, length));
          }
          nextCounter = indexData.readInt();
          itemCounter++;
        }
      }
    }

    // read offset to barcode image

    int nonHierCount = Integer.parseInt(hierarchy.get("NONHIER_COUNT"));
    int totalCount = 0;
    int originX = 0;
    int originY = 0;
    int metadataWidth = 0;
    int metadataHeight = 0;
    for (int i=0; i<nonHierCount; i++) {
      String name = hierarchy.get("NONHIER_" + i + "_NAME");
      int count = Integer.parseInt(hierarchy.get("NONHIER_" + i + "_COUNT"));
      if (name.equals("Scan data layer")) {
        for (int q=0; q<count; q++) {
          name = hierarchy.get("NONHIER_" + i + "_VAL_" + q);
          if (name.equals("ScanDataLayer_SlideBarcode")) {
            indexData.seek(nonHierarchicalRoot + (totalCount + q) * 4);
            int offset = indexData.readInt();
            indexData.seek(offset);
            int nItems = indexData.readInt();
            offset = indexData.readInt();
            indexData.seek(offset);
            nItems = indexData.readInt();
            if (nItems == 1) {
              int nextPointer = indexData.readInt();
              indexData.skipBytes(8);
              int nextOffset = indexData.readInt();
              int length = indexData.readInt();
              int fileNumber = indexData.readInt();

              TilePointer key = new TilePointer(pyramidDepth, 0);
              List<TilePointer> resolutionOffsets =
                  getOrCreateResolutionOffsets(key);
              resolutionOffsets.add(new TilePointer(
                pyramidDepth, fileNumber, nextOffset, 0, length));

              String section =
                hierarchy.get("NONHIER_" + i + "_VAL_" + q + "_SECTION");
              IniTable barcode = data.getTable(section);

              CoreMetadata m = new CoreMetadata();
              m.sizeX = Integer.parseInt(barcode.get("BARCODE_IMAGE_WIDTH"));
              m.sizeY = Integer.parseInt(barcode.get("BARCODE_IMAGE_HEIGHT"));
              m.sizeZ = 1;
              m.sizeT = 1;
              m.pixelType = FormatTools.UINT8;
              m.sizeC = 3;
              m.imageCount = m.sizeZ * m.sizeC * m.sizeT;
              m.rgb = false;
              m.dimensionOrder = "XYCZT";

              format.add(barcode.get("BARCODE_IMAGE_TYPE"));
              core.add(m);
            }
          }
        }
      }
      else if (name.equals("StitchingIntensityLayer")) {
        for (int q=0; q<count; q++) {
          name = hierarchy.get("NONHIER_" + i + "_VAL_" + q);
          if (name.equals("StitchingIntensityLevel")) {
            indexData.seek(nonHierarchicalRoot + (totalCount + q) * 4);
            int offset = indexData.readInt();
            indexData.seek(offset);
            int nItems = indexData.readInt();
            offset = indexData.readInt();
            indexData.seek(offset);
            nItems = indexData.readInt();
            int nextPointer = indexData.readInt();
            indexData.skipBytes(8);
            int nextOffset = indexData.readInt();
            int length = indexData.readInt();
            int fileNumber = indexData.readInt();

            String positionFile = files.get(fileNumber + 1);
            RandomAccessInputStream stream =
              new RandomAccessInputStream(positionFile);
            stream.seek(nextOffset);

            ZlibCodec codec = new ZlibCodec();
            byte[] positionData = codec.decompress(stream, null);

            int nTiles = positionData.length / 9;
            tilePositions = new int[nTiles][2];
            int minX = Integer.MAX_VALUE;
            int minY = Integer.MAX_VALUE;

            for (int t=0; t<nTiles; t++) {
              tilePositions[t][0] =
                DataTools.bytesToInt(positionData, t * 9 + 1, 4, true);
              tilePositions[t][1] =
                DataTools.bytesToInt(positionData, t * 9 + 5, 4, true);

              if (tilePositions[t][0] > 0 && tilePositions[t][0] < minX) {
                minX = tilePositions[t][0];
              }
              if (tilePositions[t][1] > 0 && tilePositions[t][1] < minY) {
                minY = tilePositions[t][1];
              }
            }

            originX = minX - (minX % 256);
            originY = minY - (minY % 256);

            for (int t=0; t<nTiles; t++) {
              tilePositions[t][0] -= originX;
              tilePositions[t][1] -= originY;
            }

            stream.close();

            String stitchingTable =
              hierarchy.get("NONHIER_" + i + "_VAL_" + q + "_SECTION");
            IniTable stitching = data.getTable(stitchingTable);
            String prefix =
              "COMPRESSED_STITCHING_ORIG_SLIDE_SCANNED_AREA_IN_PIXELS__";

            String left = stitching.get(prefix + "LEFT");
            String top = stitching.get(prefix + "TOP");
            String right = stitching.get(prefix + "RIGHT");
            String bottom = stitching.get(prefix + "BOTTOM");

            if (left != null && top != null &&
              right != null && bottom != null)
            {
              int tableMinX = Integer.parseInt(left);
              int tableMinY = Integer.parseInt(top);
              int tableMaxX = Integer.parseInt(right);
              int tableMaxY = Integer.parseInt(bottom);
              metadataWidth = (tableMaxX - tableMinX) + 1;
              metadataHeight = (tableMaxY - tableMinY) + 1;
            }
          }
        }

      }
      else if (name.equals("VIMSLIDE_POSITION_BUFFER")) {
        for (int q=0; q<count; q++) {
          indexData.seek(nonHierarchicalRoot + (totalCount + q) * 4);
          int offset = indexData.readInt();
          indexData.seek(offset);
          int nItems = indexData.readInt();
          offset = indexData.readInt();
          indexData.seek(offset);
          nItems = indexData.readInt();
          int nextPointer = indexData.readInt();
          indexData.skipBytes(8);
          int nextOffset = indexData.readInt();
          int length = indexData.readInt();
          int fileNumber = indexData.readInt();

          String positionFile = files.get(fileNumber + 1);
          RandomAccessInputStream stream =
            new RandomAccessInputStream(positionFile);
          stream.order(true);
          stream.seek(nextOffset);

          int nTiles = length / 9;
          int minX = Integer.MAX_VALUE;
          int minY = Integer.MAX_VALUE;
          tilePositions = new int[nTiles][2];
          for (int t=0; t<nTiles; t++) {
            stream.skipBytes(1);
            tilePositions[t][0] = stream.readInt();
            tilePositions[t][1] = stream.readInt();

            if (tilePositions[t][0] > 0 && tilePositions[t][0] < minX) {
              minX = tilePositions[t][0];
            }
            if (tilePositions[t][1] > 0 && tilePositions[t][1] < minY) {
              minY = tilePositions[t][1];
            }
          }
          minX -= (minX % 256);
          minY -= (minY % 256);
          for (int t=0; t<nTiles; t++) {
            tilePositions[t][0] -= minX;
            tilePositions[t][1] -= minY;
          }
          stream.close();
        }
      }
      totalCount += count;
    }
    indexData.close();

    tileRowCount = new double[pyramidDepth];
    tileColCount = new double[pyramidDepth];
    minRowIndex = new int[pyramidDepth];
    minColIndex = new int[pyramidDepth];
    maxRowIndex = new int[pyramidDepth];
    maxColIndex = new int[pyramidDepth];

    String bitDepth = general.get("VIMSLIDE_SLIDE_BITDEPTH");
    int bits = 8;
    if (bitDepth != null) {
      try {
        bits = Integer.parseInt(bitDepth);
      }
      catch (NumberFormatException e) {
        LOGGER.debug("Could not parse bit depth {}", bitDepth);
      }
    }

    // parse metadata for each resolution level
    for (int i=0; i<pyramidDepth; i++) {
      String section = hierarchy.get("HIER_0_VAL_" + i + "_SECTION");
      IniTable zoomLevel = data.getTable(section);

      tileWidth[i] = Integer.parseInt(zoomLevel.get("DIGITIZER_WIDTH"));
      tileHeight[i] = Integer.parseInt(zoomLevel.get("DIGITIZER_HEIGHT"));
      overlapX[i] = Double.parseDouble(zoomLevel.get("OVERLAP_X"));
      overlapY[i] = Double.parseDouble(zoomLevel.get("OVERLAP_Y"));
      pixelSizeX[i] =
        Double.parseDouble(zoomLevel.get("MICROMETER_PER_PIXEL_X"));
      pixelSizeY[i] =
        Double.parseDouble(zoomLevel.get("MICROMETER_PER_PIXEL_Y"));
      format.set(i, zoomLevel.get("IMAGE_FORMAT"));

      channelFillColor =
        Integer.parseInt(zoomLevel.get("IMAGE_FILL_COLOR_BGR"));

      CoreMetadata m = core.get(i);
      m.sizeC = Integer.parseInt(hierarchy.get("HIER_1_COUNT"));
      m.rgb = false;
      m.sizeZ = 1;
      m.sizeT = 1;
      m.imageCount = getSizeZ() * getSizeT() * getSizeC();
      m.dimensionOrder = "XYCZT";

      minRowIndex[i] = Integer.MAX_VALUE;
      minColIndex[i] = Integer.MAX_VALUE;

      // make sure we only check offsets for the first resolution
      for (TilePointer key : offsets.keySet()) {
        if (key.resolution != i) {
          continue;
        }
        for (TilePointer channelOffset : offsets.get(key)) {
          int normalizedCounter = channelOffset.counter;
          int colIndex = normalizedCounter % xTiles;
          int rowIndex = normalizedCounter / xTiles;
          if (colIndex < minColIndex[i]) {
            minColIndex[i] = colIndex;
          }
          if (colIndex > maxColIndex[i]) {
            maxColIndex[i] = colIndex;
          }
          if (rowIndex < minRowIndex[i]) {
            minRowIndex[i] = rowIndex;
          }
          if (rowIndex > maxRowIndex[i]) {
            maxRowIndex[i] = rowIndex;
          }
        }
      }

      double resScale = Math.pow(2, i);
      if (i > 0) {
        double targetMinRow = minRowIndex[0] / resScale;
        double targetMinCol = minColIndex[0] / resScale;

        if (targetMinRow > minRowIndex[i] / resScale) {
          minRowIndex[i] = (int) ((int) targetMinRow * resScale);
        }
        if (targetMinCol > minColIndex[i] / resScale) {
          minColIndex[i] = (int) ((int) targetMinCol * resScale);
        }
      }

      tileColCount[i] = (maxColIndex[i] - minColIndex[i] + 1) / resScale;
      tileRowCount[i] = (maxRowIndex[i] - minRowIndex[i] + 1) / resScale;

      overlapX[i] = Math.round(overlapX[i]);
      overlapY[i] = Math.round(overlapY[i]);

      double div = getResolutionDivisions(0);

      int tilesPerTile = (int) Math.ceil(1 / getResolutionDivisions(i));

      if (i == 0) {
        double totalWidth = tileWidth[i] * tileColCount[i];
        double totalHeight = tileHeight[i] * tileRowCount[i];

        m.sizeX = (int) totalWidth;
        m.sizeY = (int) totalHeight;

        if (useMetadataDimensions() &&
          metadataWidth > 0 && metadataHeight > 0)
        {
          m.sizeX = (int) Math.min(metadataWidth + tileWidth[i], m.sizeX);
          m.sizeY = (int) Math.min(metadataHeight + tileHeight[i], m.sizeY);
        }
      }
      else {
        m.sizeX = (int) (core.get(i - 1).sizeX / SCALE_FACTOR);
        m.sizeY = (int) (core.get(i - 1).sizeY / SCALE_FACTOR);

        tileWidth[i] -= (tilesPerTile - 1) * overlapX[i];
        tileHeight[i] -= (tilesPerTile - 1) * overlapY[i];
      }

      m.pixelType = FormatTools.pixelTypeFromBytes(bits / 8, false, false);

      if (i == 0) {
        m.resolutionCount = pyramidDepth;
      }
      else {
        m.thumbnail = true;
      }
    }

    int start = core.size() == pyramidDepth ? core.size() - 1 : core.size() - 2;
    for (int i=start; i>=0; i--) {
      if (core.get(i).sizeX == 0 || core.get(i).sizeY == 0) {
        core.remove(i);
        core.get(0).resolutionCount--;
        pyramidDepth--;
      }
    }

    MetadataStore store = makeFilterMetadata();
    MetadataTools.populatePixels(store, this, true);

    if (store instanceof IMinMaxStore) {
      // set default minimum and maximum values for pyramid resolutions
      IMinMaxStore minMax = (IMinMaxStore) store;
      for (int series=0; series<core.get(0).resolutionCount; series++) {
        for (int c=0; c<getEffectiveSizeC(); c++) {
          int color = (channelFillColor >> (8 * (c % MAX_CHANNELS))) & 0xff;
          double percentColor = color / 255.0;
          percentColor *=
            (Math.pow(2, FormatTools.getBytesPerPixel(getPixelType()) * 8) - 1);
          minMax.setChannelGlobalMinMax(c, 0, percentColor, series);
        }
      }
    }

    if (getMetadataOptions().getMetadataLevel() != MetadataLevel.MINIMUM) {
      String slideName = general.get("SLIDE_NAME");
      String projectName = general.get("PROJECT_NAME");
      String slideID = general.get("SLIDE_ID");
      String slideDescription = general.get("SLIDE_CONTENT");
      String creationDate = general.get("SLIDE_CREATIONDATETIME");
      String objectiveMag = general.get("OBJECTIVE_MAGNIFICATION");
      String objectiveName = general.get("OBJECTIVE_NAME");
      String cameraType = general.get("CAMERA_TYPE");

      for (String key : general.keySet()) {
        if (!key.equals(IniTable.HEADER_KEY)) {
          addGlobalMeta(key, general.get(key));
        }
      }

      String instrument = MetadataTools.createLSID("Instrument", 0);
      store.setInstrumentID(instrument, 0);

      String objective = MetadataTools.createLSID("Objective", 0, 0);
      store.setObjectiveID(objective, 0, 0);
      store.setObjectiveModel(objectiveName, 0, 0);
      if (objectiveMag != null) {
        try {
          store.setObjectiveNominalMagnification(
            new Double(objectiveMag), 0, 0);
        }
        catch (NumberFormatException e) {
          LOGGER.debug(
            "Could not parse objective magnification {}", objectiveMag);
        }
      }

      String detector = MetadataTools.createLSID("Detector", 0, 0);
      store.setDetectorID(detector, 0, 0);
      store.setDetectorModel(cameraType, 0, 0);

      if ((slideName == null || slideName.equals("None")) &&
        objectiveMag != null)
      {
        slideName = objectiveMag + "x";
      }

      store.setImageName(slideName, 0);

      if (core.size() > pyramidDepth) {
        store.setImageName("label image", getSeriesCount() - 1);
      }

      store.setImageDescription(slideID, 0);
      store.setImageInstrumentRef(instrument, 0);
      store.setObjectiveSettingsID(objective, 0);

      if (creationDate != null) {
        String formattedDate = null;
        for (String dateFormat : DATE_FORMATS) {
          formattedDate = DateTools.formatDate(creationDate, dateFormat);
          if (formattedDate != null) {
            break;
          }
        }
        if (formattedDate != null) {
          store.setImageAcquisitionDate(new Timestamp(formattedDate), 0);
        }
        else {
          LOGGER.warn("Could not parse acquisition date: {}", creationDate);
        }
      }

      String section = hierarchy.get("HIER_0_VAL_0_SECTION");
      IniTable resTable = data.getTable(section);

      String sizeX = resTable.get("MICROMETER_PER_PIXEL_X");
      String sizeY = resTable.get("MICROMETER_PER_PIXEL_Y");

      // store positions in pixels, but override with value in physical units
      // if the physical pixel sizes are present and valid
      store.setPlanePositionX(new Length(originX, UNITS.PIXEL), 0, 0);
      store.setPlanePositionY(new Length(originY, UNITS.PIXEL), 0, 0);

      if (sizeX != null) {
        try {
          double x = Double.parseDouble(sizeX);
          if (x > 0) {
            store.setPixelsPhysicalSizeX(new Length(x, UNITS.MICROM), 0);
            store.setPlanePositionX(
              new Length(originX * x, UNITS.MICROM), 0, 0);
          }
        }
        catch (NumberFormatException e) {
          LOGGER.debug("Could not parse physical pixel size X {}", sizeX);
        }
      }
      if (sizeY != null) {
        try {
          double y = Double.parseDouble(sizeY);
          if (y > 0) {
            store.setPixelsPhysicalSizeY(new Length(y, UNITS.MICROM), 0);
            store.setPlanePositionY(
              new Length(originY * y, UNITS.MICROM), 0, 0);
          }
        }
        catch (NumberFormatException e) {
          LOGGER.debug("Could not parse physical pixel size Y {}", sizeY);
        }
      }

      // parse channel data

      for (int c=0; c<getSizeC(); c++) {
        section = hierarchy.get("HIER_1_VAL_" + c + "_SECTION");
        IniTable channelTable = data.getTable(section);

        if (channelTable == null) {
          continue;
        }

        String name = channelTable.get("FILTER_NAME");
        String red = channelTable.get("COLOR_R");
        String green = channelTable.get("COLOR_G");
        String blue = channelTable.get("COLOR_B");
        String exposure = channelTable.get("EXPOSURE_TIME");
        String gain = channelTable.get("DIGITALGAIN");
        boolean useRed =
          Boolean.valueOf(channelTable.get("USE_RED_CHANNEL").toLowerCase());
        boolean useGreen =
          Boolean.valueOf(channelTable.get("USE_GREEN_CHANNEL").toLowerCase());
        boolean useBlue =
          Boolean.valueOf(channelTable.get("USE_BLUE_CHANNEL").toLowerCase());

        if (!useRed && !useGreen && !useBlue) {
          // name and color information won't be usable
          // exposure and gain information will be missing
          continue;
        }

        if (name != null && !name.equals("Default")) {
          store.setChannelName(name, 0, c);
        }
        if (red != null && green != null && blue != null) {
          int r = 0;
          try {
            r = Integer.parseInt(red);
          }
          catch (NumberFormatException e) {
            LOGGER.debug("Could not parse red value {}", red);
          }
          int g = 0;
          try {
            g = Integer.parseInt(green);
          }
          catch (NumberFormatException e) {
            LOGGER.debug("Could not parse green value {}", green);
          }
          int b = 0;
          try {
            b = Integer.parseInt(blue);
          }
          catch (NumberFormatException e) {
            LOGGER.debug("Could not parse blue value {}", blue);
          }
          // don't set the color to black or white
          if ((r < 255 || g < 255 || b < 255) && (r > 0 || g > 0 || b > 0)) {
            store.setChannelColor(new Color(r, g, b, 255), 0, c);
          }
        }
        if (exposure != null) {
          try {
            double exp = Double.parseDouble(exposure);
            store.setPlaneExposureTime(new Time(exp, UNITS.MICROS), 0, c);
          }
          catch (NumberFormatException e) {
            LOGGER.debug("Could not parse exposure time {}", exposure);
          }
        }

        store.setDetectorSettingsID(detector, 0, c);
        if (gain != null) {
          try {
            store.setDetectorSettingsGain(new Double(gain), 0, c);
          }
          catch (NumberFormatException e) {
            LOGGER.debug("Could not parse channel gain {}", gain);
          }
        }
      }
    }
  }

  /**
   * Set the fill value for missing tiles.
   *
   * @param fill the fill value, or null to use the reader's default value
   * @deprecated used setFillColor(Byte)
   */
  public void setFillValue(Byte fill) {
    setFillColor(fill);
  }

  /**
   * Get the fill value for missing tiles.
   * If {@link setFillValue(Byte)} was not called or the fill value was
   * set to null, assumes 0 (black) for fluorescence data and 255 (white)
   * for brightfield.
   *
   * @return fill value for missing tiles
   * @deprecated use getFillColor()
   */
  public byte getFillValue() {
    return getFillColor();
  }

  @Override
  public Byte getFillColor() {
    // don't check super.getFillColor() here, as that is expected to
    // return 0 instead of null if a fill color was not set
    if (fillColor != null) {
      return super.getFillColor();
    }
    return fluorescence ? (byte) 0 : (byte) 255;
  }

  /**
   * Sets the tile cache that will be used to try and avoid repeated
   * decompression.
   * @param newTileCache the tile cache to use
   */
  public void setTileCache(Cache<TilePointer, byte[]> newTileCache) {
    tileCache = newTileCache;
  }

  /**
   * @return true if XY dimensions calculated from Slidedat.ini should be used
   */
  public boolean useMetadataDimensions() {
    MetadataOptions options = getMetadataOptions();
    if (options instanceof DynamicMetadataOptions) {
      return ((DynamicMetadataOptions) options).getBoolean(
        DIMENSIONS_KEY, DIMENSIONS_DEFAULT);
    }
    return DIMENSIONS_DEFAULT;
  }

  /**
   * Retrieves the current set of offsets for a given resolution.  Creates the
   * resolution offsets map if it has not been already.
   * @param resolution resolution to retrieve offsets for
   * @return a map of channel offsets for the resolution
   * <code>resolution</code> indexed by tile counter
   * @see #getChannelOffsets()
   */
  private List<TilePointer> getOrCreateResolutionOffsets(
      TilePointer resolution)
  {
    List<TilePointer> resolutionOffsets = offsets.get(resolution);
    if (resolutionOffsets == null) {
      resolutionOffsets = new ArrayList<TilePointer>();
      offsets.put(resolution, resolutionOffsets);
    }
    return resolutionOffsets;
  }

  private byte[] readTile(String file, long offset, long nextOffset,
    String tileFormat, int channel)
    throws FormatException, IOException
  {
    if (tileStream == null || !file.equals(tileFile)) {
      closeTileStream();
      tileStream = new RandomAccessInputStream(file, 8192);
      tileFile = file;
    }
    tileStream.seek(offset);

    byte[] tile = null;
    int compressedByteCount = (int) (nextOffset - offset);
    if (nextOffset < 0 || compressedByteCount <= 0) {
      compressedByteCount = (int) (tileStream.length() - offset);
    }
    if (tileFormat.equals("PNG")) {
      byte[] fileBuffer = new byte[compressedByteCount];
      tileStream.readFully(fileBuffer);

      String filename = "tile." + (tileFormat.equals("PNG") ? "png" : "jpg");
      ByteArrayHandle byteFile = new ByteArrayHandle(fileBuffer);
      Location.mapFile(filename, byteFile);

      pngReader.setId(filename);
      tile = pngReader.openBytes(channel);
      pngReader.close();
      byteFile.close();
      Location.mapFile(filename, null);
    }
    else if (tileFormat.equals("JPEG")) {
      // check the JPEG marker before decompressing
      // missing tiles will sometimes present as an offset somewhere
      // in the middle of a file that does not contain pixel data
      //
      // checking the marker first is faster and prevents
      // IllegalArgumentException and/or NullPointerException from
      // being thrown
      if (tileStream.read() == (byte) 0xff &&
        tileStream.read() == (byte) 0xd8)
      {
        tileStream.seek(offset);
        jpegOptions.interleaved = false;
        tile = jpegCodec.decompress(tileStream, jpegOptions);
      }
      else {
        LOGGER.trace("Invalid marker: reading from {} at {}", file, offset);
      }
    }
    else if (tileFormat.equals("JPEG2000")) {
      jp2kOptions.interleaved = false;
      LOGGER.debug("jp2k offset = {}", offset);
      LOGGER.debug("jp2k next offset = {}", nextOffset);
      if (nextOffset < Integer.MAX_VALUE && nextOffset > offset) {
        jp2kOptions.maxBytes = (int) nextOffset;
      }
      else {
        jp2kOptions.maxBytes = 0;
      }
      jp2kOptions.littleEndian = isLittleEndian();
      jp2kOptions.bitsPerSample =
        FormatTools.getBytesPerPixel(getPixelType()) * 8;
      long t0 = System.currentTimeMillis();
      tile = jp2kCodec.decompress(tileStream, jp2kOptions);
      LOGGER.debug(
          "jp2kCodec.decompress() time {}ms", System.currentTimeMillis() - t0
      );
    }
    else if (tileFormat.equals("JPEGXR")) {
      byte[] fileBuffer = new byte[compressedByteCount];
      tileStream.readFully(fileBuffer);
      jpegOptions.interleaved = false;
      jpegOptions.littleEndian = isLittleEndian();
      jpegOptions.bitsPerSample =
        FormatTools.getBytesPerPixel(getPixelType()) * 8;
      jpegOptions.width = tileWidth[getCoreIndex()];
      jpegOptions.height = tileHeight[getCoreIndex()];
      if (jpegxrCodec == null) {
        jpegxrCodec = new JPEGXRCodec();
      }
      long t0 = System.currentTimeMillis();
      tile = jpegxrCodec.decompress(fileBuffer, jpegOptions);
      LOGGER.debug(
        "jpegXrCodec.decompress() time {}ms", System.currentTimeMillis() - t0
      );
    }
    else {
      throw new FormatException("Unsupported tile format: " + tileFormat);
    }
    return tile;
  }

  private TilePointer lookupTile(int resolution, int x, int y, int c) {
    int counter = 0;
    if (resolution < pyramidDepth) {
      int resScale = (int) Math.pow(2, resolution);
      int firstTile =
        minRowIndex[resolution] * xTiles + minColIndex[resolution];
      counter = firstTile + resScale * (y * xTiles + x);
    }
    TilePointer key = new TilePointer(resolution, counter);
    List<TilePointer> channelOffsets = offsets.get(key);
    if (channelOffsets == null || c >= channelOffsets.size()) {
      return null;
    }
    return channelOffsets.get(c);
  }

  private void closeTileStream() {
    if (tileStream != null) {
      try {
        tileStream.close();
      }
      catch (IOException e) {
        LOGGER.trace("Could not close tile stream", e);
      }
    }
    tileStream = null;
    tileFile = null;
  }

  private double getResolutionDivisions(int res) {
    double div = divPerSide / Math.pow(2, res);
    if (div <= 0) {
      div = 1;
    }
    return div;
  }

  public class TilePointer implements Comparable<TilePointer> {
    public final int resolution;
    public final int fileIndex;
    public final long offset;
    public final int counter;
    public final int length;

    /**
     * Tile pointer to a tile at a given logical offset at a given resolution.
     * @param res resolution the file is at
     * @param tileCounter logical offset of the tile
     */
    public TilePointer(int res, int tileCounter) {
      this(res, 0, 0, tileCounter, 0);
    }

    /**
     * Tile pointer to a tile at a given logical offset at a given resolution.
     * with a physical offset and index within the file
     * @param res resolution the file is at
     * @param index physical index within the file
     * @param tileOffset physical offset within the file
     * @param tileCounter logical offset of the tile
     * @param length number of valid bytes starting at tileOffset
     */
    public TilePointer(int res, int index, long tileOffset, int tileCounter,
      int length)
    {
      this.resolution = res;
      this.fileIndex = index;
      this.offset = tileOffset;
      this.counter = tileCounter;
      this.length = length;
    }

    @Override
    public int compareTo(TilePointer o) {
      if (equals(o)) {
        return 0;
      }
      if (o.resolution != this.resolution) {
        return this.resolution - o.resolution;
      }
      if (o.counter != this.counter) {
        return this.counter - o.counter;
      }
      if (o.fileIndex != this.fileIndex) {
        return this.fileIndex - o.fileIndex;
      }
      if (this.offset != o.offset) {
        return this.offset < o.offset ? -1 : 1;
      }
      if (this.length < o.length) {
        return -1;
      }
      return 1;
    }

    @Override
    public String toString() {
      return "resolution=" + resolution + ", fileIndex=" + fileIndex +
        ", offset=" + offset + ", counter=" + counter;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof TilePointer) {
        TilePointer tilePointer = (TilePointer) obj;
        return tilePointer.resolution == this.resolution
            && tilePointer.fileIndex == this.fileIndex
            && tilePointer.offset == this.offset
            && tilePointer.counter == this.counter
            && tilePointer.length == this.length;
      }
      return super.equals(obj);
    }

    @Override
    public int hashCode() {
      return counter;
    }
  }

}
