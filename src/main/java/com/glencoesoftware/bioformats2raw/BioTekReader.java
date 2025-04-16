/**
 * Copyright (c) 2021 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.bioformats2raw;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.parsers.ParserConfigurationException;

import loci.common.Constants;
import loci.common.DataTools;
import loci.common.DateTools;
import loci.common.Location;
import loci.common.RandomAccessInputStream;
import loci.common.xml.XMLTools;
import loci.formats.CoreMetadata;
import loci.formats.FormatException;
import loci.formats.FormatReader;
import loci.formats.FormatTools;
import loci.formats.MetadataTools;
import loci.formats.in.MinimalTiffReader;
import loci.formats.meta.MetadataStore;
import loci.formats.tiff.IFD;
import loci.formats.tiff.TiffParser;

import ome.units.UNITS;
import ome.units.quantity.Length;
import ome.units.quantity.Temperature;
import ome.xml.model.primitives.Color;
import ome.xml.model.primitives.PositiveInteger;
import ome.xml.model.primitives.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * Reader for BioTek Citation 5 data.
 */
public class BioTekReader extends FormatReader {

  // -- Constants --

  private static final Logger LOGGER =
    LoggerFactory.getLogger(BioTekReader.class);
  private static final String BIOTEK_MAGIC = "BTIImageMetaData";
  private static final String WELL_REGEX = "([A-Z]{1,2})(\\d{1,2})_(-?\\d+)";
  private static final String ALPHANUM = "([A-Za-z0-9 ,\\[\\]]+)";
  private static final String SUFFIX = ".tif[f]?";
  private static final String TIFF_REGEX_A =
    WELL_REGEX + "_(\\d+)_(\\d+)_" + ALPHANUM + "_(\\d+)" + SUFFIX;
  private static final String TIFF_REGEX_B =
    WELL_REGEX + "(Z(\\d+))?_" + ALPHANUM + "_(\\d+)_(\\d+)_(\\d+)?" + SUFFIX;
  private static final String TIFF_REGEX_Z =
    WELL_REGEX + "_.+\\[(.+)_" +
    ALPHANUM + "\\]_(\\d+)_(\\d+)_([0-9-]+)?" + SUFFIX;
  private static final String TIFF_REGEX_ROI =
    "([A-Z]{1,2})(\\d{1,2})ROI(\\d+)_(\\d+)_(\\d+)_(\\d+)(Z(\\d+))?_" +
    ALPHANUM + "_(-?\\d+)" + SUFFIX;
  private static final String DATE_FORMAT = "MM/dd/yy HH:mm:ss";

  // -- Fields --

  private MinimalTiffReader helperReader;
  private Location parent;
  private List<BioTekWell> wells = new ArrayList<BioTekWell>();

  // seriesMap[seriesIndex] = {wellIndex, fieldIndex}
  private int[][] seriesMap;

  private List<String> xptFiles = new ArrayList<String>();

  // -- Constructor --

  /** Constructs a new BioTek reader. */
  public BioTekReader() {
    super("BioTek Citation", new String[] {"tif", "tiff"});
    hasCompanionFiles = false;
    domains = new String[] {FormatTools.HCS_DOMAIN};
    datasetDescription = "Directory with one .tif/.tiff file per plane";
    suffixSufficient = false;
  }

  // -- IFormatReader API methods --

  /* @see loci.formats.IFormatReader#getRequiredDirectories(String[]) */
  @Override
  public int getRequiredDirectories(String[] files)
    throws FormatException, IOException
  {
    return 1;
  }

  /* @see loci.formats.IFormatReader#isSingleFile(String) */
  @Override
  public boolean isSingleFile(String id) throws FormatException, IOException {
    return false;
  }

  /* @see loci.formats.IFormatReader#fileGroupOption(String) */
  @Override
  public int fileGroupOption(String id) throws FormatException, IOException {
    return FormatTools.MUST_GROUP;
  }

  /* @see loci.formats.IFormatReader#isThisType(RandomAccessInputStream) */
  @Override
  public boolean isThisType(RandomAccessInputStream stream) throws IOException {
    stream.seek(0);
    TiffParser p = new TiffParser(stream);
    IFD ifd = p.getFirstIFD();
    if (ifd == null) {
      return false;
    }

    String descriptionXML = ifd.getIFDTextValue(IFD.IMAGE_DESCRIPTION);
    if (descriptionXML == null) {
      return false;
    }
    return descriptionXML.indexOf(BIOTEK_MAGIC) >= 0;
  }

  /* @see loci.formats.IFormatReader#getSeriesUsedFiles(boolean) */
  @Override
  public String[] getSeriesUsedFiles(boolean noPixels) {
    FormatTools.assertId(currentId, true, 1);
    List<String> files = new ArrayList<String>();
    files.addAll(xptFiles);
    if (!noPixels) {
      BioTekWell well = wells.get(getWellIndex(getSeries()));
      String[] wellFiles = well.getFiles(getFieldIndex(getSeries()));
      for (String f : wellFiles) {
        if (f != null) {
          files.add(f);
        }
      }
    }
    return files.toArray(new String[files.size()]);
  }

  /* @see loci.formats.IFormatReader#close(boolean) */
  @Override
  public void close(boolean fileOnly) throws IOException {
    super.close(fileOnly);
    if (helperReader != null) {
      helperReader.close(fileOnly);
    }
    if (!fileOnly) {
      helperReader = null;
      parent = null;
      wells.clear();
      xptFiles.clear();
      seriesMap = null;
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

    Arrays.fill(buf, getFillColor());

    BioTekWell well = wells.get(getWellIndex(getSeries()));
    String file = well.getFile(getFieldIndex(getSeries()), getZCTCoords(no));
    LOGGER.trace("reading file = {} for series = {}, no = {}",
      file, getSeries(), no);

    if (file != null) {
      if (helperReader == null) {
        helperReader = new MinimalTiffReader();
      }
      try {
        helperReader.setId(file);
        helperReader.openBytes(0, buf, x, y, w, h);
      }
      catch (FormatException|IOException e) {
        LOGGER.debug("Could not read from " + file, e);
      }
    }
    return buf;
  }

  // -- Internal FormatReader API methods --

  /* @see loci.formats.FormatReader#initFile(String) */
  @Override
  protected void initFile(String id) throws FormatException, IOException {
    super.initFile(id);

    Location currentPath = new Location(id).getAbsoluteFile();
    parent = currentPath.getParentFile();

    findXPTFiles(parent);

    String[] files = parent.list(true);
    for (int i=0; i<files.length; i++) {
      files[i] = new Location(parent, files[i]).getAbsolutePath();
    }
    Arrays.sort(files);

    // is there only one well in the directory?
    // compare the well identifiers (relative file name up to first _)
    boolean sameWell = true;
    int endIndex = files[0].indexOf("_");
    if (endIndex > 0) {
      String wellCheck = files[0].substring(0, endIndex);
      LOGGER.debug("well check string = {}", wellCheck);
      for (int i=0; i<files.length; i++) {
        if (!files[i].startsWith(wellCheck)) {
          sameWell = false;
          break;
        }
      }
    }
    LOGGER.debug("single well in {}: {}", parent, sameWell);
    // if only one well exists, look in other subdirectories of the parent
    if (sameWell) {
      Location plateDir = parent.getParentFile();
      LOGGER.debug("plate directory = {}", plateDir);
      String[] wellDirs = plateDir.list(true);
      ArrayList<String> allFiles = new ArrayList<String>();
      boolean multipleDirs = true;
      for (String well : wellDirs) {
        Location wellDir = new Location(plateDir, well).getAbsoluteFile();
        LOGGER.debug("looking in well directory = {}", wellDir);
        if (wellDir == null) {
          multipleDirs = false;
        }
        else {
          String[] f = wellDir.list(true);
          if (f == null) {
            multipleDirs = false;
          }
          else {
            for (String file : f) {
              LOGGER.debug("  adding well file {}", file);
              allFiles.add(new Location(wellDir, file).getAbsolutePath());
            }
          }
        }
      }
      if (multipleDirs) {
        LOGGER.debug("found files = {}", allFiles);
        files = allFiles.toArray(new String[allFiles.size()]);
        Arrays.sort(files);
      }
    }

    Pattern regexA = Pattern.compile(TIFF_REGEX_A);
    Pattern regexB = Pattern.compile(TIFF_REGEX_B);
    Pattern regexZ = Pattern.compile(TIFF_REGEX_Z);
    Pattern regexROI = Pattern.compile(TIFF_REGEX_ROI);
    ArrayList<WellIndex> validWellRowCol = new ArrayList<WellIndex>();
    int maxRow = 0;
    int maxCol = 0;
    int maxPlateAcq = 0;
    Map<Integer, Integer> maxField = new HashMap<Integer, Integer>();

    String matchingPath = new Location(currentId).getAbsolutePath();
    LOGGER.trace("matching path = {}", matchingPath);

    ArrayList<String> foundWellSamples = new ArrayList<String>();

    for (String absolutePath : files) {
      String f = new Location(absolutePath).getName();
      Matcher m = regexA.matcher(f);
      int rowIndex = -1;
      int colIndex = -1;
      int fieldIndex = -1;
      int roiIndex = -1;
      int z = 0;
      int t = 0;
      String channelName = "";

      if (m.matches()) {
        rowIndex = getWellRow(m.group(1));
        colIndex = Integer.parseInt(m.group(2)) - 1;
        fieldIndex = Integer.parseInt(m.group(5)) - 1;
        channelName = m.group(6);
      }
      else {
        m = regexB.matcher(f);
        if (!m.matches()) {
          m = regexZ.matcher(f);
        }

        if (m.matches()) {
          rowIndex = getWellRow(m.group(1));
          colIndex = Integer.parseInt(m.group(2)) - 1;
          fieldIndex = Integer.parseInt(m.group(3)) - 1;

          // explicit Z index is optional, but group count is the same
          try {
            z = Integer.parseInt(m.group(5));
            // can have two channels with same name
            // one with Z stack and one without
            channelName = "Z";
          }
          catch (NumberFormatException e) {
          }
          // recorded T index may be negative if no timepoints
          t = (int) Math.max(0, Integer.parseInt(m.group(8)) - 1);
          channelName += m.group(6);
        }
        else {
          m = regexROI.matcher(f);
          if (m.matches()) {
            rowIndex = getWellRow(m.group(1));
            colIndex = Integer.parseInt(m.group(2)) - 1;
            roiIndex = Integer.parseInt(m.group(3)) - 1;

            LOGGER.trace("absolutePath = {}, roiIndex = {}",
              absolutePath, roiIndex);

            int channelIndex = Integer.parseInt(m.group(5)) - 1;
            fieldIndex = Integer.parseInt(m.group(6)) - 1;
            try {
              z = Integer.parseInt(m.group(8));
              // can have two channels with same name
              // one with Z stack and one without
              channelName = "Z";
            }
            catch (NumberFormatException e) {
            }
            channelName += m.group(9);
            // recorded T index may be negative if no timepoints
            t = (int) Math.max(0, Integer.parseInt(m.group(10)) - 1);
          }
        }
      }

      if (rowIndex >= 0 && colIndex >= 0 &&
        (fieldIndex >= 0 || roiIndex >= 0))
      {
        // collapse field and ROI index into single field index
        if (roiIndex >= 0) {
          if (fieldIndex < 0) {
            // only ROIs, no field
            fieldIndex = roiIndex;
          }
          else {
            // both fields and ROIs
            String key = fieldIndex + "," + roiIndex;
            if (!foundWellSamples.contains(key)) {
              foundWellSamples.add(key);
            }
            fieldIndex = foundWellSamples.indexOf(key);
          }
        }

        BioTekWell well = lookupWell(0, rowIndex, colIndex);
        if (fieldIndex >= well.getFieldCount()) {
          well.setFieldCount(fieldIndex + 1);
        }
        int c = well.addChannelName(fieldIndex, channelName);
        well.addFile(new PlaneIndex(fieldIndex, z, c, t), absolutePath);

        if (rowIndex > maxRow) {
          maxRow = rowIndex;
        }
        if (colIndex > maxCol) {
          maxCol = colIndex;
        }
        WellIndex rowColPair = new WellIndex(rowIndex, colIndex);
        if (!validWellRowCol.contains(rowColPair)) {
          validWellRowCol.add(rowColPair);
        }
        Integer maxFieldIndex = maxField.get(0);
        if (maxFieldIndex == null) {
          maxFieldIndex = 0;
        }
        maxField.put(0, (int) Math.max(fieldIndex, maxFieldIndex));
      }
    }
    wells.sort(null);
    validWellRowCol.sort(null);

    // split brightfield channels into a separate plate acquisition
    maxField.put(1, -1);
    int originalWellCount = wells.size();
    Set<Integer> removedChannels = new HashSet<Integer>();
    for (int well=0; well<originalWellCount; well++) {
      BioTekWell w = wells.get(well);
      w.setWellIndex(well);
      for (int f=0; f<w.getFieldCount(); f++) {
        String[] fieldFiles = w.getFiles(f);
        for (String file : fieldFiles) {
          LOGGER.trace("found file {} for well index {}, field index {}",
            file, well, f);
          Element root = getXMLRoot(file);
          boolean brightfield = isBrightField(root);

          PlaneIndex index = w.getIndex(file);

          if (brightfield) {
            LOGGER.trace("found brightfield file: {}", file);
            w.removeFile(index, file);
            removedChannels.add(index.c);

            BioTekWell bfWell =
              lookupWell(1, w.getRowIndex(), w.getColumnIndex());
            BioTekWell fluorWell =
              lookupWell(0, w.getRowIndex(), w.getColumnIndex());
            bfWell.setWellIndex(wells.indexOf(fluorWell));
            String channelName = w.getChannels(f).get(index.c).name;
            index.c = bfWell.addChannelName(f, channelName);
            bfWell.addFile(index, file);
            maxPlateAcq = 1;
            maxField.put(1, (int) Math.max(maxField.get(1), f));
            bfWell.setFieldCount(maxField.get(1) + 1);

            Channel channel = bfWell.getChannels(f).get(index.c);
            channel.color = getChannelColor(root);
          }
          else {
            Length[] waves = getWavelengths(root);
            Channel channel = w.getChannels(f).get(index.c);
            if (waves != null && channel != null) {
              channel.emWave = waves[0];
              channel.exWave = waves[1];
            }
          }
        }
      }
    }
    // correct the channel indexes in the non-brightfield plate acquisition
    for (BioTekWell well : wells) {
      if (well.getPlateAcquisition() == 0) {
        Map<PlaneIndex, Integer> planeMap = well.getFilePlaneMap();
        Map<PlaneIndex, Integer> newPlaneMap =
          new HashMap<PlaneIndex, Integer>();
        for (PlaneIndex p : planeMap.keySet()) {
          PlaneIndex newPlane = new PlaneIndex(p);
          for (Integer channel : removedChannels) {
            if (p.c >= channel) {
              newPlane.c--;
            }
          }

          newPlaneMap.put(newPlane, planeMap.get(p));
        }
        well.setFilePlaneMap(newPlaneMap);

        for (int field=0; field<well.getFieldCount(); field++) {
          well.removeChannels(field, removedChannels);
        }
      }
    }
    wells.sort(null);

    core.clear();
    helperReader = new MinimalTiffReader();
    for (BioTekWell well : wells) {
      for (int f=0; f<well.getFieldCount(); f++) {
        String file = well.getFile(f, null);

        try {
          helperReader.setId(file);
          CoreMetadata m =
            new CoreMetadata(helperReader.getCoreMetadataList().get(0));
          m.sizeZ = well.getSizeZ(f);
          m.sizeC = well.getSizeC(f);
          m.sizeT = well.getSizeT(f);
          m.imageCount = m.sizeZ * m.sizeC * m.sizeT;
          core.add(m);
        }
        catch (FormatException|IOException e) {
          LOGGER.debug("Could not read file=" + file, e);
          // if the TIFF is corrupted or otherwise unreadable,
          // try to copy metadata from the previous series
          if (core.size() > 0) {
            core.add(new CoreMetadata(core.get(core.size() - 1)));
          }
        }
        finally {
          helperReader.close();
        }
      }
    }

    MetadataStore store = makeFilterMetadata();
    MetadataTools.populatePixels(store, this, true);

    for (int s=0; s<core.size(); s++) {
      setSeries(s);

      int wellIndex = getWellIndex(s);
      int field = getFieldIndex(s);
      BioTekWell well = wells.get(wellIndex);
      int row = well.getRowIndex();
      int column = well.getColumnIndex();

      String fieldName = " #" + (field + 1);
      if (field < foundWellSamples.size()) {
        // unpack the field/ROI indexes and store both
        // in the image name

        String[] indexes = foundWellSamples.get(field).split(",");
        if (indexes.length == 2) {
          fieldName = " Field #" + (Integer.parseInt(indexes[0]) + 1) +
            ", ROI #" + (Integer.parseInt(indexes[1]) + 1);
        }
      }

      store.setImageName(getWellName(row, column) + fieldName, s);

      List<Channel> channels = well.getChannels(field);
      for (int c=0; c<getEffectiveSizeC(); c++) {
        if (channels != null && c < channels.size()) {
          Channel channel = channels.get(c);
          store.setChannelName(channel.name, s, c);
          store.setChannelColor(channel.color, s, c);
          store.setChannelEmissionWavelength(channel.emWave, s, c);
          store.setChannelExcitationWavelength(channel.exWave, s, c);
        }
      }
    }
    setSeries(0);

    store.setPlateID(MetadataTools.createLSID("Plate", 0), 0);
    store.setPlateRows(new PositiveInteger(maxRow + 1), 0);
    store.setPlateColumns(new PositiveInteger(maxCol + 1), 0);

    for (int pa=0; pa<=maxPlateAcq; pa++) {
      String plateAcqID = MetadataTools.createLSID("PlateAcquisition", 0, pa);
      store.setPlateAcquisitionID(plateAcqID, 0, pa);

      PositiveInteger fieldCount =
        FormatTools.getMaxFieldCount(maxField.get(pa) + 1);
      store.setPlateAcquisitionMaximumFieldCount(fieldCount, 0, pa);

      if (pa == 0) {
        store.setPlateAcquisitionName("Fluorescence", 0, pa);
      }
      else if (pa == 1) {
        store.setPlateAcquisitionName("Bright-field", 0, pa);
      }
    }

    int nextImage = 0;
    int[] nextWellSample = new int[(maxRow + 1) * (maxCol + 1)];
    for (int w=0; w<wells.size(); w++) {
      BioTekWell well = wells.get(w);
      int wellIndex = validWellRowCol.indexOf(
        new WellIndex(well.getRowIndex(), well.getColumnIndex()));
      LOGGER.debug(
        "well #{}, row = {}, col = {}, index = {}",
        w, well.getRowIndex(), well.getColumnIndex(), wellIndex);

      well.fillMetadataStore(store, 0, well.getPlateAcquisition(), wellIndex,
        nextWellSample[wellIndex], nextImage);

      nextWellSample[wellIndex] += well.getFieldCount();
      nextImage += well.getFieldCount();
    }

    parseXMLMetadata(wells.get(0).getFile(0, null), store);
  }

  // -- Helper methods --

  private void findXPTFiles(Location plateDir) throws IOException {
    String[] plateDescription = plateDir.getName().split("!");
    String[] plateTokens =
      plateDescription[plateDescription.length - 1].split("_");
    if (plateTokens.length > 1) {
      String barcode = plateTokens[1];
      Location experimentDir = plateDir.getParentFile();
      if (experimentDir != null) {
        Location imageDir = experimentDir.getParentFile();
        if (imageDir != null) {
          Location rootDir = imageDir.getParentFile();

          if (rootDir != null) {
            String[] rootList = rootDir.list(true);
            for (String f : rootList) {
              if (f.endsWith(barcode + ".xpt")) {
                xptFiles.add(new Location(rootDir, f).getAbsolutePath());
              }
            }
          }
        }
      }
    }
  }

  private BioTekWell lookupWell(int pa, int row, int col) {
    for (BioTekWell well : wells) {
      if (well.getPlateAcquisition() == pa &&
        well.getRowIndex() == row && well.getColumnIndex() == col)
      {
        return well;
      }
    }
    BioTekWell well = new BioTekWell();
    well.setRowIndex(row);
    well.setColumnIndex(col);
    well.setPlateAcquisition(pa);
    wells.add(well);
    return well;
  }

  private void calculateSeriesMap() {
    seriesMap = new int[core.size()][2];
    int currentWell = 0;
    int currentField = 0;
    for (int i=0; i<seriesMap.length; i++) {
      seriesMap[i][0] = currentWell;
      seriesMap[i][1] = currentField;
      currentField++;
      BioTekWell well = wells.get(currentWell);
      if (currentField == well.getFieldCount()) {
        currentWell++;
        currentField = 0;
      }
    }
  }

  private int getWellIndex(int seriesIndex) {
    if (seriesMap == null) {
      calculateSeriesMap();
    }
    return seriesMap[seriesIndex][0];
  }

  private int getFieldIndex(int seriesIndex) {
    if (seriesMap == null) {
      calculateSeriesMap();
    }
    return seriesMap[seriesIndex][1];
  }

  private Element getXMLRoot(String file) throws FormatException, IOException {
    try (TiffParser p = new TiffParser(file)) {
      String xml = p.getComment();
      if (xml == null) {
        return null;
      }
      Element root = null;
      byte[] xmlBytes = xml.getBytes(Constants.ENCODING);
      try (ByteArrayInputStream s = new ByteArrayInputStream(xmlBytes)) {
        root = XMLTools.parseDOM(s).getDocumentElement();
      }
      catch (ParserConfigurationException|SAXException e) {
        throw new FormatException(e);
      }
      return root;
    }
  }

  private boolean isBrightField(Element root)
    throws FormatException, IOException
  {
    if (root != null) {
      Element imageAcquisition = getFirstChild(root, "ImageAcquisition");
      Element brightfield = getFirstChild(imageAcquisition, "ColorBrightField");
      return Boolean.valueOf(brightfield.getTextContent());
    }
    return false;
  }

  private Color getChannelColor(Element root) {
    if (root == null) {
      return null;
    }
    Element imageAcquisition = getFirstChild(root, "ImageAcquisition");
    Element channel = getFirstChild(imageAcquisition, "Channel");
    Element red = getFirstChild(channel, "RedStainFactor");
    Element green = getFirstChild(channel, "GreenStainFactor");
    Element blue = getFirstChild(channel, "BlueStainFactor");

    if (red != null && green != null && blue != null) {
      double redFactor = DataTools.parseDouble(red.getTextContent()) / 100;
      double greenFactor = DataTools.parseDouble(green.getTextContent()) / 100;
      double blueFactor = DataTools.parseDouble(blue.getTextContent()) / 100;
      return new Color((int) (redFactor * 255), (int) (greenFactor * 255),
        (int) (blueFactor * 255), 255);
    }
    return null;
  }

  private Length[] getWavelengths(Element root) {
    if (root == null) {
      return null;
    }
    Element imageAcquisition = getFirstChild(root, "ImageAcquisition");
    Element channel = getFirstChild(imageAcquisition, "Channel");
    Element emission = getFirstChild(channel, "EmissionWavelength");
    Element excitation = getFirstChild(channel, "ExcitationWavelength");

    Length[] rtn = new Length[2];

    if (emission != null) {
      Double emWave = DataTools.parseDouble(emission.getTextContent());
      rtn[0] = FormatTools.getEmissionWavelength(emWave);
    }
    if (excitation != null) {
      Double exWave = DataTools.parseDouble(excitation.getTextContent());
      rtn[1] = FormatTools.getExcitationWavelength(exWave);
    }

    return rtn;
  }

  private void parseXMLMetadata(String file, MetadataStore store)
    throws FormatException, IOException
  {
    Element root = getXMLRoot(file);

    if (root != null) {
      Element imageReference = getFirstChild(root, "ImageReference");
      Element imageAcquisition = getFirstChild(root, "ImageAcquisition");

      Length physicalSizeX = null;
      Length physicalSizeY = null;
      Length physicalSizeZ = null;
      Temperature environmentTemperature = null;
      String date = null;
      String time = null;

      String instrumentID = MetadataTools.createLSID("Instrument", 0);
      store.setInstrumentID(instrumentID, 0);
      String objectiveID = MetadataTools.createLSID("Objective", 0, 0);
      store.setObjectiveID(objectiveID, 0, 0);
      String detectorID = MetadataTools.createLSID("Detector", 0, 0);
      store.setDetectorID(detectorID, 0, 0);

      if (imageReference != null) {
        Element plateName = getFirstChild(imageReference, "Plate");
        if (plateName != null) {
          store.setPlateName(plateName.getTextContent(), 0);
        }

        Element acqDate = getFirstChild(imageReference, "Date");
        if (acqDate != null) {
          date = acqDate.getTextContent();
        }

        Element acqTime = getFirstChild(imageReference, "Time");
        if (acqTime != null) {
          time = acqTime.getTextContent();
        }

        Element horizontalFields =
          getFirstChild(imageReference, "HorizontalTotal");
        if (horizontalFields != null) {
          addGlobalMeta("X fields", horizontalFields.getTextContent());
        }

        Element verticalFields = getFirstChild(imageReference, "VerticalTotal");
        if (verticalFields != null) {
          addGlobalMeta("Y fields", verticalFields.getTextContent());
        }

        Element zStep = getFirstChild(imageReference, "ZStackStepSizeMicrons");
        if (zStep != null) {
          Double step = DataTools.parseDouble(zStep.getTextContent());
          physicalSizeZ = FormatTools.getPhysicalSizeZ(step);
        }
      }

      if (imageAcquisition != null) {
        Element width = getFirstChild(imageAcquisition, "ImageWidthMicrons");
        if (width != null) {
          Double physicalWidth = DataTools.parseDouble(width.getTextContent());
          physicalWidth /= getSizeX();
          physicalSizeX = FormatTools.getPhysicalSizeX(physicalWidth);
        }

        Element height = getFirstChild(imageAcquisition, "ImageHeightMicrons");
        if (height != null) {
          Double physicalHeight =
            DataTools.parseDouble(height.getTextContent());
          physicalHeight /= getSizeY();
          physicalSizeY = FormatTools.getPhysicalSizeY(physicalHeight);
        }

        Element objectiveMag = getFirstChild(imageAcquisition, "ObjectiveSize");
        if (objectiveMag != null) {
          Double mag = DataTools.parseDouble(objectiveMag.getTextContent());
          store.setObjectiveNominalMagnification(mag, 0, 0);
        }

        Element objectiveModel =
          getFirstChild(imageAcquisition, "ObjectiveBTIPartNumber");
        if (objectiveModel != null) {
          store.setObjectiveModel(objectiveModel.getTextContent(), 0, 0);
        }

        Element objectiveMaker =
          getFirstChild(imageAcquisition, "ObjectiveMfg");
        if (objectiveMaker != null) {
          store.setObjectiveManufacturer(objectiveMaker.getTextContent(), 0, 0);
        }

        Element lensNA = getFirstChild(imageAcquisition, "NumericalAperture");
        if (lensNA != null) {
          Double na = DataTools.parseDouble(lensNA.getTextContent());
          store.setObjectiveLensNA(na, 0, 0);
        }

        Element cameraGain = getFirstChild(imageAcquisition, "CameraGain");
        if (cameraGain != null) {
          Double gain = DataTools.parseDouble(cameraGain.getTextContent());
          store.setDetectorGain(gain, 0, 0);
        }

        Element temperature = getFirstChild(imageAcquisition, "Temperature");
        if (temperature != null) {
          temperature = getFirstChild(temperature, "Value");
          if (temperature != null) {
            Double temp = DataTools.parseDouble(temperature.getTextContent());
            if (temp != null) {
              environmentTemperature = new Temperature(temp, UNITS.CELSIUS);
            }
          }
        }

        Element reversePlate =
          getFirstChild(imageAcquisition, "ReversePlateOrientation");
        if (reversePlate != null) {
          addGlobalMeta(
            "Reverse plate orientation", reversePlate.getTextContent());
        }
      }

      Timestamp acqTimestamp = null;
      if (date != null && time != null) {
        acqTimestamp =
          new Timestamp(DateTools.formatDate(date + " " + time, DATE_FORMAT));
      }

      store.setPlateAcquisitionStartTime(acqTimestamp, 0, 0);
      for (int i=0; i<getSeriesCount(); i++) {
        setSeries(i);
        store.setImageAcquisitionDate(acqTimestamp, i);
        store.setPixelsPhysicalSizeX(physicalSizeX, i);
        store.setPixelsPhysicalSizeY(physicalSizeY, i);
        store.setPixelsPhysicalSizeZ(physicalSizeZ, i);

        store.setImagingEnvironmentTemperature(environmentTemperature, i);

        store.setImageInstrumentRef(instrumentID, i);
        store.setObjectiveSettingsID(objectiveID, i);
        for (int c=0; c<getEffectiveSizeC(); c++) {
          store.setDetectorSettingsID(detectorID, i, c);
        }
      }
      setSeries(0);
    }
  }

  private Element getFirstChild(Element parentNode, String name) {
    NodeList children = parentNode.getElementsByTagName(name);
    if (children == null || children.getLength() == 0) {
      return null;
    }
    return (Element) children.item(0);
  }

  // TODO: replace getWellRow and getWellName with FormatTools methods
  // once https://github.com/ome/bioformats/pull/3347 is released
  private int getWellRow(String rowLetters) {
    int row = 0;
    for (int r=rowLetters.length()-1; r>=0; r--) {
      int mul = (int) Math.pow(26, rowLetters.length() - r - 1);
      row += (mul * (rowLetters.charAt(r) - 'A'));
    }
    return row;
  }

  private String getWellName(int row, int col) {
    String name = String.valueOf(col + 1);
    if (name.length() == 1) {
      name = "0" + name;
    }
    name = (char) ('A' + (row % 26)) + name;
    if (row >= 26) {
      name = (char) ('A' + ((row / 26) - 1)) + name;
    }
    return name;
  }

  class BioTekWell extends WellContainer {
    private Map<PlaneIndex, Integer> filePlaneMap =
      new HashMap<PlaneIndex, Integer>();
    private Map<Integer, List<Channel>> channelMap =
      new HashMap<Integer, List<Channel>>();

    public BioTekWell() {
      super();
    }

    public BioTekWell(int pa, int row, int col, int fields) {
      super(pa, row, col, fields);
    }

    public Map<PlaneIndex, Integer> getFilePlaneMap() {
      return filePlaneMap;
    }

    public void setFilePlaneMap(Map<PlaneIndex, Integer> newMap) {
      filePlaneMap = newMap;
    }

    public List<Channel> getChannels(int fieldIndex) {
      return channelMap.get(fieldIndex);
    }

    public int addChannelName(int fieldIndex, String channel) {
      List<Channel> channels = channelMap.get(fieldIndex);
      if (channels == null) {
        channels = new ArrayList<Channel>();
      }
      int index = -1;
      for (int c=0; c<channels.size(); c++) {
        if (channel.equals(channels.get(c).name)) {
          index = c;
          break;
        }
      }
      if (index < 0) {
        index = channels.size();
        Channel c = new Channel(channel);
        channels.add(c);
      }
      channelMap.put(fieldIndex, channels);
      return index;
    }

    public void removeChannels(int fieldIndex, Collection<Integer> toRemove) {
      Integer[] sortedChannels = toRemove.toArray(new Integer[toRemove.size()]);
      Arrays.sort(sortedChannels);

      List<Channel> channels = channelMap.get(fieldIndex);
      for (int i=sortedChannels.length-1; i>=0; i--) {
        int channelIndex = sortedChannels[i];
        if (channels != null && channelIndex < channels.size()) {
          channels.remove(channelIndex);
        }
      }
      channelMap.put(fieldIndex, channels);
    }

    public void addFile(PlaneIndex index, String file) {
      super.addFile(file);

      filePlaneMap.put(index, getAllFiles().size() - 1);
    }

    public void removeFile(PlaneIndex index, String file) {
      int offset = filePlaneMap.get(index);
      filePlaneMap.remove(index);
      getAllFiles().set(offset, null);
    }

    public String[] getFiles(int field) {
      List<String> fieldFiles = new ArrayList<String>();
      List<String> allFiles = getAllFiles();
      for (PlaneIndex p : filePlaneMap.keySet()) {
        if (p.fieldIndex == field) {
          fieldFiles.add(allFiles.get(filePlaneMap.get(p)));
        }
      }
      return fieldFiles.toArray(new String[fieldFiles.size()]);
    }

    public int getSizeZ(int field) {
      int maxZ = 0;
      for (PlaneIndex p : filePlaneMap.keySet()) {
        maxZ = (int) Math.max(p.z, maxZ);
      }
      return maxZ + 1;
    }

    public int getSizeC(int field) {
      int maxC = 0;
      for (PlaneIndex p : filePlaneMap.keySet()) {
        maxC = (int) Math.max(p.c, maxC);
      }
      return maxC + 1;
    }

    public int getSizeT(int field) {
      int maxT = 0;
      for (PlaneIndex p : filePlaneMap.keySet()) {
        maxT = (int) Math.max(p.t, maxT);
      }
      return maxT + 1;
    }

    public String getFile(int fieldIndex, int[] zct) {
      if (zct == null) {
        for (PlaneIndex p : filePlaneMap.keySet()) {
          if (p.fieldIndex == fieldIndex) {
            return getAllFiles().get(filePlaneMap.get(p));
          }
        }
      }
      PlaneIndex p = new PlaneIndex(fieldIndex, zct[0], zct[1], zct[2]);
      Integer index = filePlaneMap.get(p);
      return index == null ? null : getAllFiles().get(index);
    }

    public PlaneIndex getIndex(String file) {
      List<String> allFiles = getAllFiles();
      for (int f=0; f<allFiles.size(); f++) {
        if (file.equals(allFiles.get(f))) {
          for (PlaneIndex p : filePlaneMap.keySet()) {
            if (filePlaneMap.get(p).equals(f)) {
              return p;
            }
          }
        }
      }
      return null;
    }
  }

  class PlaneIndex {
    public int fieldIndex;
    public int z;
    public int c;
    public int t;

    public PlaneIndex(int f, int z, int c, int t) {
      this.fieldIndex = f;
      this.z = z;
      this.c = c;
      this.t = t;
    }

    public PlaneIndex(PlaneIndex copy) {
      this.fieldIndex = copy.fieldIndex;
      this.z = copy.z;
      this.c = copy.c;
      this.t = copy.t;
    }

    @Override
    public int hashCode() {
      int fc = (fieldIndex & 0xff) << 24;
      int zc = (z & 0xff) << 16;
      int cc = (c & 0xff) << 8;
      int tc = (t & 0xff);
      return fc | zc | cc | tc;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof PlaneIndex) {
        PlaneIndex p = (PlaneIndex) o;
        return p.fieldIndex == fieldIndex && p.z == z &&
          p.c == c && p.t == t;
      }
      return false;
    }

    public String toString() {
      return "fieldIndex=" + fieldIndex + ", z=" + z +
        ", c=" + c + ", t=" + t;
    }

  }

  class Channel {
    public String name;
    public Color color;
    public Length emWave;
    public Length exWave;

    public Channel(String name) {
      this.name = name;
    }
  }

  class WellIndex implements Comparable<WellIndex> {
    public int row;
    public int col;

    public WellIndex(int r, int c) {
      this.row = r;
      this.col = c;
    }

    @Override
    public int compareTo(WellIndex w) {
      if (this.row != w.row) {
        return this.row - w.row;
      }
      return this.col - w.col;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof WellIndex)) {
        return false;
      }
      return compareTo((WellIndex) o) == 0;
    }

    @Override
    public int hashCode() {
      // this would need fixing if we had more than 65535 rows or columns
      return (row & 0xffff) << 16 | (col & 0xffff);
    }
  }

}
