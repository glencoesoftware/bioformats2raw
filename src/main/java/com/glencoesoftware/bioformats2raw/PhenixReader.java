/**
 * Copyright (c) 2021 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.bioformats2raw;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import loci.common.DataTools;
import loci.common.DateTools;
import loci.common.IniList;
import loci.common.IniParser;
import loci.common.IniTable;
import loci.common.RandomAccessInputStream;
import loci.formats.CoreMetadata;
import loci.formats.FormatException;
import loci.formats.FormatReader;
import loci.formats.FormatTools;
import loci.formats.MetadataTools;
import loci.formats.in.DynamicMetadataOptions;
import loci.formats.in.MetadataOptions;
import loci.formats.in.TiffReader;
import loci.formats.meta.MetadataStore;
import ome.units.quantity.Time;
import ome.xml.model.primitives.Color;
import ome.xml.model.primitives.NonNegativeInteger;
import ome.xml.model.primitives.PositiveInteger;
import ome.xml.model.primitives.Timestamp;

import org.perf4j.StopWatch;
import org.perf4j.slf4j.Slf4JStopWatch;

/**
 * PhenixReader is the file format reader for Opera Phenix database plates.
 */
public class PhenixReader extends FormatReader {

  // -- Constants --

  public static final String MAGIC_STRING = "#PHENIX FILE";

  public static final String INCLUDE_TIFFS_KEY = "phenix.include_tiffs";
  public static final boolean INCLUDE_TIFFS_DEFAULT = false;

  private static final String[] DATE_FORMATS = new String[] {
    DateTools.TIMESTAMP_FORMAT + ".SSS",
    DateTools.TIMESTAMP_FORMAT,
    DateTools.ISO8601_FORMAT_MS,
    DateTools.ISO8601_FORMAT,
  };

  // -- Fields --

  private ArrayList<PhenixImage> images = new ArrayList<PhenixImage>();

  private transient TiffReader planeReader = new TiffReader();

  // -- Constructor --

  /** Constructs a new Opera Phenix reader. */
  public PhenixReader() {
    super("Opera Phenix", "phenix");
    domains = new String[] {FormatTools.HCS_DOMAIN};
  }

  // -- Phenix-specific methods --

  /**
   * Check reader options to determine if TIFFs should be included
   * in used files list.
   *
   * @return true if TIFFS will be included in the used files list
   */
  public boolean canIncludeTIFFs() {
    MetadataOptions options = getMetadataOptions();
    if (options instanceof DynamicMetadataOptions) {
      return ((DynamicMetadataOptions) options).getBoolean(
        INCLUDE_TIFFS_KEY, INCLUDE_TIFFS_DEFAULT);
    }
    return INCLUDE_TIFFS_DEFAULT;
  }

  // -- IFormatReader API methods --

  /* @see loci.formats.IFormatReader#isThisType(RandomAccessInputStream) */
  @Override
  public boolean isThisType(RandomAccessInputStream stream) throws IOException {
    final int blockLen = 16;
    if (!FormatTools.validStream(stream, blockLen, false)) {
      return false;
    }
    return MAGIC_STRING.equals(stream.readString(blockLen));
  }

  /**
   * @see loci.formats.IFormatReader#openBytes(int, byte[], int, int, int, int)
   */
  @Override
  public byte[] openBytes(int no, byte[] buf, int x, int y, int w, int h)
    throws FormatException, IOException
  {
    FormatTools.checkPlaneParameters(this, no, buf.length, x, y, w, h);

    StopWatch s = stopWatch();
    PhenixImage image = getImage(getSeries(), no);
    s.stop("file lookup for [" + getSeries() + ", " + no + "]");
    if (image != null && image.filename != null) {
      LOGGER.debug("series: {}, no: {}, image.channel: {}, " +
        "well row: {}, well column: {}, file: {}",
        getSeries(), no, image.channel, image.row, image.col, image.filename);
      s = stopWatch();
      if (planeReader == null) {
        planeReader = new TiffReader();
      }
      try {
        planeReader.setId(image.filename);
        s.stop("setId on " + image.filename);
        LOGGER.debug("  IFD: {}", planeReader.getIFDs().get(0));
        s = stopWatch();
        planeReader.openBytes(0, buf, x, y, w, h);
      }
      catch (IOException e) {
        Arrays.fill(buf, getFillColor());
      }
      s.stop("openBytes(0) on " + image.filename);
    }
    else {
      Arrays.fill(buf, getFillColor());
    }

    return buf;
  }

  /* @see loci.formats.IFormatReader#getSeriesUsedFiles(boolean) */
  @Override
  public String[] getSeriesUsedFiles(boolean noPixels) {
    ArrayList<String> files = new ArrayList<String>();
    files.add(currentId);
    if (canIncludeTIFFs() && !noPixels) {
      List<PhenixImage> seriesImages = getImages(getSeries());
      for (PhenixImage img : seriesImages) {
        if (img != null && img.filename != null) {
          files.add(img.filename);
        }
      }
    }
    return files.toArray(new String[files.size()]);
  }

  /* @see loci.formats.IFormatReader#close(boolean) */
  @Override
  public void close(boolean fileOnly) throws IOException {
    super.close(fileOnly);
    planeReader.close(fileOnly);
    if (!fileOnly) {
      images.clear();
    }
  }

  // -- Internal FormatReader API methods --

  /* @see loci.formats.FormatReader#initFile(String) */
  protected void initFile(String id) throws FormatException, IOException {
    super.initFile(id);

    LOGGER.info("Parsing metadata file");
    StopWatch watch = stopWatch();

    IniList plateMetadata = new IniParser().parseINI(new File(currentId));
    watch.stop("parsed metadata file");

    watch = stopWatch();
    IniTable plate = plateMetadata.getTable("Plate");

    for (IniTable table : plateMetadata) {
      String tableName = table.get(IniTable.HEADER_KEY);
      if (tableName.startsWith("Image")) {
        images.add(new PhenixImage(table));
      }
    }
    images.sort(new Comparator<PhenixImage>() {
      public int compare(PhenixImage a, PhenixImage b) {
        if (a.series != b.series) {
          return a.series - b.series;
        }
        if (a.plane != b.plane) {
          return a.plane - b.plane;
        }
        return a.channel - b.channel;
      }
    });
    LOGGER.debug("Found {} individual image planes", images.size());

    core = new ArrayList<CoreMetadata>();

    String objective = plate.get("Objective");
    Double magnification = DataTools.parseDouble(plate.get("Magnification"));

    Double physicalSizeX = DataTools.parseDouble(plate.get("PhysicalSizeX"));
    Double physicalSizeY = DataTools.parseDouble(plate.get("PhysicalSizeY"));
    planeReader.setOriginalMetadataPopulated(isOriginalMetadataPopulated());
    planeReader.setMetadataFiltered(isMetadataFiltered());
    watch.stop("set up reader and image list");

    watch = stopWatch();
    CoreMetadata ms = null;
    CoreMetadata currentCore = null;
    for (int i=0; i<images.size(); i++) {
      PhenixImage img = images.get(i);

      if (i == 0) {
        planeReader.setId(img.filename);
        ms = planeReader.getCoreMetadataList().get(0);
      }

      if (img.series == core.size()) {
        currentCore = new CoreMetadata(ms);
      }

      currentCore.sizeZ = (int) Math.max(currentCore.sizeZ, img.plane);
      currentCore.sizeT = 1;
      currentCore.sizeC = (int) Math.max(currentCore.sizeC, img.channel);
      currentCore.imageCount =
        currentCore.sizeZ * currentCore.sizeC * currentCore.sizeT;
      currentCore.dimensionOrder = "XYCZT";

      if (img.series == core.size()) {
        core.add(currentCore);
      }
    }
    watch.stop("assembled CoreMetadata");
    watch = stopWatch();

    MetadataStore store = makeFilterMetadata();
    MetadataTools.populatePixels(store, this, true);

    int wellsX = Integer.parseInt(plate.get("Columns"));
    int wellsY = Integer.parseInt(plate.get("Rows"));

    store.setPlateID(MetadataTools.createLSID("Plate", 0), 0);
    store.setPlateColumns(new PositiveInteger(wellsX), 0);
    store.setPlateRows(new PositiveInteger(wellsY), 0);
    store.setPlateName(plate.get("Name"), 0);
    store.setPlateDescription(plate.get("Description"), 0);
    store.setPlateExternalIdentifier(plate.get("Barcode"), 0);

    String plateAcqId = MetadataTools.createLSID("PlateAcquisition", 0, 0);
    store.setPlateAcquisitionID(plateAcqId, 0, 0);

    // map a well index to a count, to find the max field count
    HashMap<Integer, Integer> validWells = new HashMap<Integer, Integer>();
    for (PhenixImage img : images) {
      int well = (img.row - 1) * wellsX + (img.col - 1);
      validWells.put(well, img.field);
    }
    LOGGER.trace("validWells = {}", validWells);

    // the field count may vary between wells, so find the
    // maximum field count across all wells
    int nFields = 0;
    for (Integer f : validWells.values()) {
      if (f > nFields) {
        nFields = f;
      }
    }
    LOGGER.debug("field count = {}", nFields);

    store.setPlateAcquisitionMaximumFieldCount(
      new PositiveInteger(nFields), 0, 0);
    Timestamp plateDate = getTimestamp(plate.get("Date"));
    if (plateDate != null) {
      store.setPlateAcquisitionStartTime(plateDate, 0, 0);
    }

    String instrument = MetadataTools.createLSID("Instrument", 0);
    store.setInstrumentID(instrument, 0);
    String objectiveID = MetadataTools.createLSID("Objective", 0, 0);
    store.setObjectiveID(objectiveID, 0, 0);
    store.setObjectiveModel(objective, 0, 0);
    if (magnification != null) {
      store.setObjectiveNominalMagnification(magnification, 0, 0);
    }

    for (int c=0; c<getEffectiveSizeC(); c++) {
      store.setLaserID(MetadataTools.createLSID("LightSource", 0, c), 0, c);
      PhenixImage img = getImage(0, getIndex(0, c, 0));
      store.setLaserWavelength(FormatTools.getWavelength(img.wavelength), 0, c);
    }

    watch.stop("populated plate metadata");
    watch = stopWatch();

    int image = 0;
    int well = 0;
    for (int row=0; row<wellsY; row++) {
      for (int col=0; col<wellsX; col++) {
        if (!validWells.containsKey(row * wellsX + col)) {
          continue;
        }

        String wellID = MetadataTools.createLSID("Well", 0, well);
        store.setWellID(wellID, 0, well);
        store.setWellRow(new NonNegativeInteger(row), 0, well);
        store.setWellColumn(new NonNegativeInteger(col), 0, well);

        for (int field=0; field<nFields; field++) {
          // check the index into images, to handle the
          // case when the last well is missing one or more fields
          if (image >= images.size()) {
            break;
          }
          PhenixImage img = getImage(image, 0);
          if (img.col - 1 != col || img.row - 1 != row) {
            // make sure that this site's well lines up with the
            // well that we're processing
            // this should catch the case when a well has fewer
            // fields than expected
            break;
          }

          LOGGER.debug("Using image {} for row = {}, col = {}, field = {}",
            image, row, col, field);

          String wellSampleID =
            MetadataTools.createLSID("WellSample", 0, well, field);
          store.setWellSampleID(wellSampleID, 0, well, field);

          String imageID = MetadataTools.createLSID("Image", image);
          store.setImageID(imageID, image);
          store.setImageName("Well " + FormatTools.getWellName(row, col) +
            ", Field #" + (field + 1), image);

          Timestamp imageDate = getTimestamp(img.acquisitionDate);
          if (imageDate != null) {
            store.setImageAcquisitionDate(imageDate, image);
          }
          store.setImageInstrumentRef(instrument, image);
          store.setObjectiveSettingsID(objectiveID, image);

          store.setWellSampleImageRef(imageID, 0, well, field);
          store.setWellSampleIndex(
            new NonNegativeInteger(image), 0, well, field);

          store.setPlateAcquisitionWellSampleRef(wellSampleID, 0, 0, image);

          store.setPixelsPhysicalSizeX(
            FormatTools.getPhysicalSizeX(physicalSizeX), image);
          store.setPixelsPhysicalSizeY(
            FormatTools.getPhysicalSizeY(physicalSizeY), image);

          for (int c=0; c<getEffectiveSizeC(); c++) {
            int cIndex = getIndex(0, c, 0);
            img = getImage(image, cIndex);
            store.setChannelName(img.channelName, image, c);
            store.setChannelLightSourceSettingsID(
              MetadataTools.createLSID("LightSource", 0, c), image, c);

            if (img.filterWavelength != null) {
              store.setChannelLightSourceSettingsWavelength(
                FormatTools.getWavelength(img.filterWavelength), image, c);
              store.setChannelColor(
                getChannelColor(img.filterWavelength.intValue()), image, c);
            }

            if (img.exposureTime != null) {
              Time expTime = FormatTools.getTime(img.exposureTime, null);
              for (int z=0; z<getSizeZ(); z++) {
                for (int t=0; t<getSizeT(); t++) {
                  int planeIndex = getIndex(z, c, t);
                  store.setPlaneExposureTime(expTime, image, planeIndex);
                }
              }
            }
          }

          image++;
        }

        well++;
      }
    }
    watch.stop("populated well metadata");
  }

  // -- Helper methods --

  private PhenixImage getImage(int series, int plane) {
    int[] zct = getZCTCoords(plane);
    for (PhenixImage img : images) {
      if (img.series == series && img.plane - 1 == zct[0] &&
        img.channel - 1 == zct[1])
      {
        return img;
      }
    }
    return null;
  }

  private List<PhenixImage> getImages(int series) {
    List<PhenixImage> imgs = new ArrayList<PhenixImage>();
    for (int i=0; i<core.get(series).imageCount; i++) {
      imgs.add(getImage(series, i));
    }
    return imgs;
  }

  private Timestamp getTimestamp(String dateTime) {
    String standardizedDateTime = DateTools.formatDate(dateTime, DATE_FORMATS);
    try {
      return new Timestamp(standardizedDateTime);
    }
    catch (IllegalArgumentException e) {
      LOGGER.error("Could not parse date: " +dateTime, e);
    }
    return null;
  }

  private StopWatch stopWatch() {
    return new Slf4JStopWatch(LOGGER, Slf4JStopWatch.TRACE_LEVEL);
  }

  /**
   * Similar but not quite identical to Operetta.
   *
   * @param wavelength filter wavelength in nm
   * @return corresponding RGBA color
   */
  private Color getChannelColor(int wavelength) {
    if (wavelength < 450) {
      // magenta (== violet)
      return new Color(255, 0, 255, 255);
    }
    else if (wavelength < 500) {
      // blue
      return new Color(0, 0, 255, 255);
    }
    else if (wavelength < 550) {
      // green
      return new Color(0, 255, 0, 255);
    }
    else if (wavelength < 600) {
      // orange
      return new Color(255, 128, 0, 255);
    }
    else if (wavelength < 750) {
      // red
      return new Color(255, 0, 0, 255);
    }
    // white
    return new Color(255, 255, 255, 255);
  }

}
