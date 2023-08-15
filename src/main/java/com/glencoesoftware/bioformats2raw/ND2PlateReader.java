/**
 * Copyright (c) 2022 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */

package com.glencoesoftware.bioformats2raw;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import loci.common.Location;

import loci.formats.CoreMetadata;
import loci.formats.FormatException;
import loci.formats.FormatReader;
import loci.formats.FormatTools;
import loci.formats.MetadataTools;

import loci.formats.in.ND2Reader;

import loci.formats.meta.MetadataStore;
import loci.formats.ome.OMEXMLMetadata;

import ome.xml.meta.MetadataConverter;
import ome.xml.meta.OMEXMLMetadataRoot;
import ome.xml.model.Image;
import ome.xml.model.primitives.NonNegativeInteger;
import ome.xml.model.primitives.PositiveInteger;

/**
 *
 * @see ND2Reader
 */
public class ND2PlateReader extends FormatReader {

  private static final String PLATE_REGEX =
    "(.*_?)Well([A-Z])(\\d{2})_Channel(.*)_Seq(\\d{4}).nd2";

  private transient ND2Reader reader = new ND2Reader();
  private transient Pattern platePattern;
  private String[] files;
  private Integer[] fieldCount;

  // -- Constructor --

  /** Constructs a new ND2 reader. */
  public ND2PlateReader() {
    super("Nikon ND2 Plate", "nd2");
    suffixSufficient = false;
    domains = new String[] {FormatTools.HCS_DOMAIN};
  }

  @Override
  public boolean isThisType(String name, boolean open) {
    if (!isGroupFiles() || !reader.isThisType(name, open)) {
      return false;
    }
    Pattern p = Pattern.compile(PLATE_REGEX);
    return p.matcher(name).matches();
  }

  @Override
  public void close(boolean fileOnly) throws IOException {
    super.close(fileOnly);
    if (reader != null) {
      reader.close(fileOnly);
      files = null;
      fieldCount = null;
      platePattern = null;
    }
  }

  @Override
  public String[] getSeriesUsedFiles(boolean noPixels) {
    if (noPixels) {
      return super.getSeriesUsedFiles(noPixels);
    }
    return new String[] {files[getFileIndex(getSeries())]};
  }

  @Override
  public byte[] openBytes(int no, byte[] buf, int x, int y, int w, int h)
    throws FormatException, IOException
  {
    Arrays.fill(buf, (byte) 0);

    int fileIndex = getFileIndex(getSeries());
    if (fileIndex >= 0 && fileIndex < files.length) {
      if (files[fileIndex] != null) {
        try {
          reader.setId(files[fileIndex]);
          reader.setSeries(getFieldIndex(getSeries()));
          return reader.openBytes(no, buf, x, y, w, h);
        }
        catch (Exception e) {
          LOGGER.error(
            files[fileIndex] + " could not be read; returning blank planes", e);
        }
      }
    }
    return buf;
  }

  @Override
  protected void initFile(String id) throws FormatException, IOException {
    super.initFile(id);

    // use an OMEXMLMetadataStore independent of this reader's MetadataStore
    // so that Images can for sure be copied
    reader.setMetadataStore(MetadataTools.createOMEXMLMetadata());

    // look for all files in the directory that match the regex
    // and have the same plate name

    platePattern = Pattern.compile(PLATE_REGEX);
    Location currentFile = new Location(id).getAbsoluteFile();
    Location parentDir = currentFile.getParentFile();
    Matcher currentMatcher = platePattern.matcher(currentFile.getName());
    String plateName = null;
    if (currentMatcher.matches()) {
      plateName = currentMatcher.group(1);
    }

    List<String> allFiles = new ArrayList<String>();
    for (String s : parentDir.list(true)) {
      Matcher m = platePattern.matcher(s);
      if (m.matches() && m.group(1).equals(plateName)) {
        allFiles.add(new Location(parentDir, s).getAbsolutePath());
      }
    }

    // sort list of files by well row and column

    allFiles.sort(new Comparator<String>() {
      @Override
      public int compare(String s1, String s2) {
        int[] well1 = getWellCoordinates(s1);
        int[] well2 = getWellCoordinates(s2);

        if (well1[0] != well2[0]) {
          return well1[0] - well2[0];
        }
        return well1[1] - well2[1];
      }
    });

    // populate MetadataStore Plate data based on min/max well row and column

    // copy each well file's metadata to core metadata and MetadataStore

    core.clear();

    OMEXMLMetadataRoot tmpRoot = new OMEXMLMetadataRoot();

    int minRow = Integer.MAX_VALUE;
    int maxRow = 0;
    int minCol = Integer.MAX_VALUE;
    int maxCol = 0;
    List<int[]> wells = new ArrayList<int[]>();
    List<Integer> fieldCounts = new ArrayList<Integer>();
    for (int f=0; f<allFiles.size(); f++) {
      try {
        reader.setId(allFiles.get(f));
      }
      catch (Exception e) {
        LOGGER.error(allFiles.get(f) + " could not be read; ignoring", e);
        allFiles.remove(f);
        f--;
        continue;
      }
      fieldCounts.add(reader.getSeriesCount());

      List<CoreMetadata> wellCores = reader.getCoreMetadataList();
      for (CoreMetadata c : wellCores) {
        core.add(new CoreMetadata(c));
      }

      OMEXMLMetadata omeMeta = (OMEXMLMetadata) reader.getMetadataStore();
      OMEXMLMetadataRoot wellRoot = (OMEXMLMetadataRoot) omeMeta.getRoot();
      if (f == 0) {
        // only need one copy of the Instrument data
        tmpRoot.addInstrument(wellRoot.copyInstrumentList().get(0));
      }

      List<Image> images = wellRoot.copyImageList();
      for (Image img : images) {
        tmpRoot.addImage(img);
      }

      int[] rowColumn = getWellCoordinates(allFiles.get(f));
      minRow = (int) Math.min(minRow, rowColumn[0]);
      maxRow = (int) Math.max(maxRow, rowColumn[0]);
      minCol = (int) Math.min(minCol, rowColumn[1]);
      maxCol = (int) Math.max(maxCol, rowColumn[1]);
      wells.add(rowColumn);
    }
    files = allFiles.toArray(new String[allFiles.size()]);
    fieldCount = fieldCounts.toArray(new Integer[fieldCounts.size()]);

    OMEXMLMetadata tmpMeta =
      (OMEXMLMetadata) MetadataTools.createOMEXMLMetadata();
    tmpMeta.setRoot(tmpRoot);
    MetadataStore store = makeFilterMetadata();
    MetadataConverter.convertMetadata(tmpMeta, store);
    MetadataTools.populatePixels(store, this);

    store.setPlateID(MetadataTools.createLSID("Plate", 0), 0);

    // remove trailing underscore
    if (plateName != null && plateName.endsWith("_")) {
      plateName = plateName.substring(0, plateName.length() - 1);
    }
    store.setPlateName(plateName, 0);

    store.setPlateRows(new PositiveInteger(maxRow + 1), 0);
    store.setPlateColumns(new PositiveInteger(maxCol + 1), 0);

    int imageIndex = 0;
    for (int f=0; f<fieldCount.length; f++) {
      int[] rowColumn = wells.get(f);
      store.setWellID(MetadataTools.createLSID("Well", 0, f), 0, f);
      store.setWellRow(new NonNegativeInteger(rowColumn[0]), 0, f);
      store.setWellColumn(new NonNegativeInteger(rowColumn[1]), 0, f);

      for (int ws=0; ws<fieldCount[f]; ws++, imageIndex++) {
        String wsID = MetadataTools.createLSID("WellSample", 0, f, ws);
        String imageID = MetadataTools.createLSID("Image", imageIndex);
        store.setWellSampleID(wsID, 0, f, ws);
        store.setWellSampleIndex(new NonNegativeInteger(imageIndex), 0, f, ws);
        store.setWellSampleImageRef(imageID, 0, f, ws);
      }
    }
  }

  private int getFileIndex(int seriesIndex) {
    int index = 0;
    int file = 0;
    while (index <= seriesIndex && file < fieldCount.length) {
      index += fieldCount[file];
      file++;
    }
    return file - 1;
  }

  private int getFieldIndex(int seriesIndex) {
    int fileIndex = getFileIndex(seriesIndex);
    int field = seriesIndex;
    for (int f=0; f<fileIndex; f++) {
      field -= fieldCount[f];
    }
    return field;
  }

  private int[] getWellCoordinates(String file) {
    String name = new Location(file).getName();
    Matcher m = platePattern.matcher(name);
    if (m.matches()) {
      String row = m.group(2);
      String col = m.group(3);
      return new int[] {row.charAt(0) - 'A', Integer.parseInt(col) - 1};
    }
    throw new IllegalArgumentException(file + " does not match plate format");
  }

}
