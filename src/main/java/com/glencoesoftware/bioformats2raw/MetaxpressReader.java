/**
 * Copyright (c) 2023 Glencoe Software, Inc. All rights reserved.
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
import loci.formats.in.MetamorphReader;
import loci.formats.in.MinimalTiffReader;
import loci.formats.meta.MetadataStore;
import loci.formats.ome.OMEXMLMetadata;
import ome.units.quantity.Length;
import ome.units.quantity.Time;
import ome.units.UNITS;
import ome.xml.model.primitives.NonNegativeInteger;
import ome.xml.model.primitives.PositiveInteger;
import ome.xml.model.primitives.Timestamp;

import org.perf4j.StopWatch;
import org.perf4j.slf4j.Slf4JStopWatch;

/**
 * MetaxpressReader is the file format reader for MetaXpress plates.
 */
public class MetaxpressReader extends FormatReader {

  // -- Constants --

  public static final String INCLUDE_TIFFS_KEY = "metaxpress.include_tiffs";
  public static final boolean INCLUDE_TIFFS_DEFAULT = false;

  public static final String MAGIC_STRING = "#METAXPRESS FILE";

  // -- Fields --

  private ArrayList<MetaxpressSite> sites = new ArrayList<MetaxpressSite>();

  private transient MinimalTiffReader planeReader = new MinimalTiffReader();

  // -- Constructor --

  /** Constructs a new MetaXpress reader. */
  public MetaxpressReader() {
    super("MetaXpress", "metaxpress");
    domains = new String[] {FormatTools.HCS_DOMAIN};
  }

  // -- Metaxpress-specific methods --

  /**
   * Check reader options to determine if TIFFs
   * should be included in used files.
   *
   * @return true if TIFFs should be added to used files list
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

  @Override
  public boolean isThisType(RandomAccessInputStream stream) throws IOException {
    final int blockLen = 16;
    if (!FormatTools.validStream(stream, blockLen, false)) {
      return false;
    }
    return MAGIC_STRING.equals(stream.readString(blockLen));
  }

  @Override
  public byte[] openBytes(int no, byte[] buf, int x, int y, int w, int h)
    throws FormatException, IOException
  {
    FormatTools.checkPlaneParameters(this, no, buf.length, x, y, w, h);

    StopWatch s = stopWatch();
    String file = sites.get(getSeries()).files.get(no);
    s.stop("file lookup for [" + getSeries() + ", " + no + "]");
    if (file != null) {
      s = stopWatch();
      if (planeReader == null) {
        planeReader = new MinimalTiffReader();
      }
      try {
        planeReader.setId(file);
        s.stop("setId on " + file);
        s = stopWatch();
        planeReader.openBytes(0, buf, x, y, w, h);
      }
      catch (IOException e) {
        Arrays.fill(buf, (byte) 0);
      }
      s.stop("openBytes(0) on " + file);
    }
    else {
      Arrays.fill(buf, (byte) 0);
    }

    return buf;
  }

  @Override
  public String[] getSeriesUsedFiles(boolean noPixels) {
    ArrayList<String> files = new ArrayList<String>();
    files.add(currentId);
    if (canIncludeTIFFs() && !noPixels) {
      files.addAll(sites.get(getSeries()).files);
    }
    return files.toArray(new String[files.size()]);
  }

  @Override
  public void close(boolean fileOnly) throws IOException {
    super.close(fileOnly);
    planeReader.close(fileOnly);
    if (!fileOnly) {
      sites.clear();
    }
  }

  // -- Internal FormatReader API methods --

  @Override
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
      if (tableName.startsWith("Site")) {
        sites.add(new MetaxpressSite(table));
      }
    }
    sites.sort(new Comparator<MetaxpressSite>() {
      public int compare(MetaxpressSite a, MetaxpressSite b) {
        if (a.y != b.y) {
          return a.y - b.y;
        }
        if (a.x != b.x) {
          return a.x - b.x;
        }
        return a.id.compareTo(b.id);
      }
    });

    core = new ArrayList<CoreMetadata>();

    Length physicalSizeX = null;
    Length physicalSizeY = null;
    Length[] wavelengths = null;
    Time[] exposureTimes = null;

    MetamorphReader reader = new MetamorphReader();
    reader.setGroupFiles(false);
    reader.setOriginalMetadataPopulated(isOriginalMetadataPopulated());
    reader.setMetadataFiltered(isMetadataFiltered());
    watch.stop("set up reader and site list");

    watch = stopWatch();
    for (int i=0; i<sites.size(); i++) {
      CoreMetadata ms = null;

      if (i == 0) {
        reader.setMetadataStore(MetadataTools.createOMEXMLMetadata());
        reader.setId(sites.get(i).files.get(0));
        ms = reader.getCoreMetadataList().get(0);
      }
      else {
        // speed up initialization time by assuming that
        // all wells have the same XY size and pixel type
        ms = new CoreMetadata(core.get(0));
      }

      ms.sizeZ = sites.get(i).z;
      ms.sizeT = sites.get(i).t;
      ms.sizeC = sites.get(i).c;
      ms.imageCount = ms.sizeZ * ms.sizeC * ms.sizeT;
      ms.dimensionOrder = "XYZTC";
      core.add(ms);

      if (i == 0) {
        OMEXMLMetadata meta = (OMEXMLMetadata) reader.getMetadataStore();

        wavelengths = new Length[getSizeC()];
        exposureTimes = new Time[getSizeC()];
        physicalSizeX = meta.getPixelsPhysicalSizeX(0);
        physicalSizeY = meta.getPixelsPhysicalSizeY(0);

        wavelengths[0] = meta.getChannelEmissionWavelength(0, 0);
        exposureTimes[0] = meta.getPlaneExposureTime(0, 0);

        for (int q=1; q<getSizeC(); q++) {
          reader.close();
          reader.setMetadataStore(MetadataTools.createOMEXMLMetadata());

          int index = getIndex(0, q, 0);
          reader.setId(sites.get(i).files.get(index));
          meta = (OMEXMLMetadata) reader.getMetadataStore();

          wavelengths[q] = meta.getChannelEmissionWavelength(0, 0);
          exposureTimes[q] = meta.getPlaneExposureTime(0, 0);
        }
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
    for (MetaxpressSite s : sites) {
      int well = s.y * wellsX + s.x;
      if (!validWells.containsKey(well)) {
        validWells.put(well, 1);
      }
      else {
        validWells.put(well, validWells.get(well) + 1);
      }
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
    store.setPlateAcquisitionStartTime(new Timestamp(plate.get("Date")), 0, 0);

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
          // check the index into sites, to handle the
          // case when the last well is missing one or more fields
          if (image >= sites.size()) {
            break;
          }
          MetaxpressSite site = sites.get(image);
          if (site.x != col || site.y != row) {
            // make sure that this site's well lines up with the
            // well that we're processing
            // this should catch the case when a well has fewer
            // fields than expected
            break;
          }

          LOGGER.debug("Using site {} for row = {}, col = {}, field = {}",
            image, row, col, field);

          String wellSampleID =
            MetadataTools.createLSID("WellSample", 0, well, field);
          store.setWellSampleID(wellSampleID, 0, well, field);

          String imageID = MetadataTools.createLSID("Image", image);
          store.setImageID(imageID, image);
          store.setImageName("Well " + FormatTools.getWellName(row, col) +
            ", Field #" + (field + 1), image);
          store.setImageAcquisitionDate(
            new Timestamp(plate.get("Date")), image);

          store.setWellSampleImageRef(imageID, 0, well, field);
          store.setWellSampleIndex(
            new NonNegativeInteger(image), 0, well, field);

          store.setWellSamplePositionX(
            new Length(site.xpos, UNITS.REFERENCEFRAME), 0, well, field);
          store.setWellSamplePositionY(
            new Length(site.ypos, UNITS.REFERENCEFRAME), 0, well, field);

          store.setPlateAcquisitionWellSampleRef(wellSampleID, 0, 0, image);

          store.setPixelsPhysicalSizeX(physicalSizeX, image);
          store.setPixelsPhysicalSizeY(physicalSizeY, image);

          // update series before checking effective SizeC and image count
          // otherwise plane counts are assumed to be the same for all wells
          setSeries(image);
          for (int c=0; c<getEffectiveSizeC(); c++) {
            int cIndex = getIndex(0, c, 0);
            if (cIndex < site.channelNames.size()) {
              store.setChannelName(site.channelNames.get(cIndex), image, c);
            }
            if (c < wavelengths.length && wavelengths[c] != null) {
              store.setChannelEmissionWavelength(wavelengths[c], image, c);
            }
          }

          for (int img=0; img<getImageCount(); img++) {
            int c = getZCTCoords(img)[1];
            if (c < core.get(image).sizeC && exposureTimes[c] != null) {
              store.setPlaneExposureTime(exposureTimes[c], image, img);
            }
          }

          image++;
        }

        well++;
      }
    }
    setSeries(0);
    watch.stop("populated well metadata");
  }

  // -- Helper methods --

  private StopWatch stopWatch() {
    return new Slf4JStopWatch(LOGGER, Slf4JStopWatch.TRACE_LEVEL);
  }

}
