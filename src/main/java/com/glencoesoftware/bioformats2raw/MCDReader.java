/**
 * Copyright (c) 2023 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.bioformats2raw;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import loci.common.ByteArrayHandle;
import loci.common.Location;
import loci.common.RandomAccessInputStream;
import loci.common.xml.BaseHandler;
import loci.common.xml.XMLTools;
import loci.formats.CoreMetadata;
import loci.formats.FormatException;
import loci.formats.FormatReader;
import loci.formats.FormatTools;
import loci.formats.MetadataTools;
import loci.formats.in.APNGReader;
import loci.formats.meta.MetadataStore;

import org.xml.sax.Attributes;

/**
 * MCDReader is the file format reader for Fluidigm Hyperion .mcd files.
 */
public class MCDReader extends FormatReader {

  // -- Constants --

  private static final String HEADER_MARKER = "CytofShared.MCDHeader";
  private static final String XML_MARKER = "<SchemasOffset>";

  // -- Fields --

  private transient APNGReader pngHelper;

  private List<Panorama> panoramas = new ArrayList<Panorama>();
  private List<Acquisition> acquisitions = new ArrayList<Acquisition>();
  private transient List<Channel> channels = new ArrayList<Channel>();

  // -- Constructor --

  /** Constructs a new .mcd reader. */
  public MCDReader() {
    super("Fluidigm Hyperion", new String[] {"mcd"});
    domains = new String[] {FormatTools.UNKNOWN_DOMAIN};
  }

  // -- IFormatReader API methods --

  /**
   * @see loci.formats.IFormatReader#openBytes(int, byte[], int, int, int, int)
   */
  @Override
  public byte[] openBytes(int no, byte[] buf, int x, int y, int w, int h)
    throws FormatException, IOException
  {
    FormatTools.checkPlaneParameters(this, no, buf.length, x, y, w, h);
    Arrays.fill(buf, getFillColor());

    if (getSeries() < panoramas.size()) {
      LOGGER.debug("Reading PNG panorama #{}", getSeries());
      if (pngHelper == null) {
        pngHelper = new APNGReader();
      }

      pngHelper.setId(panoramas.get(getSeries()).map(in));
      return pngHelper.openBytes(0, buf, x, y, w, h);
    }

    LOGGER.debug("Reading raw acquisition #{}", getSeries() - panoramas.size());
    in.seek(acquisitions.get(getSeries() - panoramas.size()).start);
    LOGGER.debug("File offset = {}", in.getFilePointer());

    int bpp = FormatTools.getBytesPerPixel(getPixelType());

    // channels are stored interleaved, so reading requires
    // a lot of skipping through the pixel data block

    in.skipBytes(bpp * getReversePlaneIndex(no));
    int skip = bpp * (getImageCount() - 1);

    in.skipBytes(y * (skip + bpp) * getSizeX());
    for (int row=0; row<h; row++) {
      in.skipBytes(x * (skip + bpp));
      for (int col=0; col<w; col++) {
        in.read(buf, ((row * w) + col) * bpp, bpp);
        in.skipBytes(skip);
      }
      in.skipBytes((skip + bpp) * (getSizeX() - w - x));
    }

    return buf;
  }

  /* @see loci.formats.IFormatReader#close(boolean) */
  @Override
  public void close(boolean fileOnly) throws IOException {
    super.close(fileOnly);
    if (pngHelper != null) {
      pngHelper.close(fileOnly);
    }
    for (Panorama p : panoramas) {
      p.unmap();
    }
    if (!fileOnly) {
      pngHelper = null;
      panoramas.clear();
      acquisitions.clear();
      channels.clear();
    }
  }

  // -- Internal FormatReader API methods --

  /* @see loci.formats.FormatReader#initFile(String) */
  @Override
  protected void initFile(String id) throws FormatException, IOException {
    super.initFile(id);

    // TODO: panoramas assumed to be always PNG, this might not be correct?
    pngHelper = new APNGReader();

    in = new RandomAccessInputStream(id);
    // first look for a header with a pointer to the XML
    findMarker();
    String version = readString(in);
    in.skipBytes(2);
    long startPointer = in.getFilePointer();
    String headerMarker = readString(in);
    if (!headerMarker.equals(HEADER_MARKER)) {
      LOGGER.warn("Unexpected header marker: {} at: {}",
        headerMarker, startPointer);
    }
    in.skipBytes(1);
    startPointer = in.getFilePointer();
    String xmlMarker = readString(in);
    if (!xmlMarker.startsWith(XML_MARKER)) {
      LOGGER.warn("Unexpected XML marker: {} at: {}",
        xmlMarker, startPointer);
    }
    in.skipBytes(6);
    in.order(true);
    long offset = in.readLong();
    LOGGER.debug("Found XML offset: {}", offset);

    if (offset < in.getFilePointer() || offset >= in.length()) {
      throw new FormatException("Invalid XML offset. File is corrupted.");
    }

    // now read XML
    // it's a string with two separate documents (MCDSchema and MCDPublic)
    // the string has to be split, or parsing will fail
    in.seek(offset);
    byte[] xmlBytes = new byte[(int) (in.length() - offset)];
    LOGGER.debug("Reading {} XML bytes as UTF-16LE", xmlBytes.length);
    in.read(xmlBytes);
    String xml = new String(xmlBytes, "UTF-16LE");
    LOGGER.trace("XML: {}", xml);
    String mcdSchema = xml;
    String mcdPublic = null;
    int split = mcdSchema.indexOf("<MCDPublic");
    if (split > 0) {
      mcdPublic = mcdSchema.substring(split);
      mcdSchema = mcdSchema.substring(0, split);
    }
    LOGGER.trace("MCDSchema XML: {}", mcdSchema);
    LOGGER.trace("MCDPublic XML: {}", mcdPublic);

    XMLTools.parseXML(mcdSchema, new MCDHandler());
    // TODO: parse mcdPublic as well? not sure it adds much value

    LOGGER.debug("Found {} panoramas", panoramas.size());
    LOGGER.debug("Found {} acquisitions", acquisitions.size());

    core.clear();

    // assemble dimension information for panoramas by reading
    // the PNG streams directly
    for (Panorama p : panoramas) {
      String file = p.map(in);
      try {
        LOGGER.debug("Reading internal PNG {}", file);

        pngHelper.setId(file);
        core.add(pngHelper.getCoreMetadataList().get(0));
      }
      finally {
        pngHelper.close();
        p.unmap();
      }
    }

    // assemble dimension information for raw acquisitions
    // using stored metadata
    for (int a=0; a<acquisitions.size(); a++) {
      Acquisition acq = acquisitions.get(a);
      LOGGER.debug("Reading acquisition {}", acq.description);
      CoreMetadata m = new CoreMetadata();
      m.sizeX = acq.sizeX;
      m.sizeY = acq.sizeY;

      // have only seen 32-bit float so far
      if (acq.bpp == 4 && acq.dataType.equalsIgnoreCase("float")) {
        m.pixelType = FormatTools.FLOAT;
      }
      else {
        throw new FormatException("Unexpected data type '" + acq.dataType +
          "' with " + acq.bpp + " bytes per pixel");
      }

      m.sizeZ = 1;
      m.sizeT = 1;

      int plane = acq.sizeX * acq.sizeY * acq.bpp;
      long totalBytes = acq.end - acq.start;
      if (totalBytes <= 0) {
        LOGGER.debug(
          "Skipping acquistiion {} with 0 pixel bytes", acq.description);
        acquisitions.remove(acq);
        a--;
        continue;
      }
      long totalPlanes = totalBytes / plane;
      if (totalPlanes > Integer.MAX_VALUE) {
        throw new FormatException(
          "Too many channels (" + totalPlanes + ") for series " + core.size());
      }
      m.sizeC = (int) totalPlanes;
      m.imageCount = m.sizeC * m.sizeZ * m.sizeT;
      m.dimensionOrder = "XYCZT";
      m.littleEndian = true;
      core.add(m);

      // the XML defines a big list of all channels across all acquisitions
      // map each channel to the correct location in the correct acquisition
      acq.channelIndexes = new int[m.sizeC];
      for (int c=0; c<channels.size(); c++) {
        Channel channel = channels.get(c);
        if (channel.acqID == acq.id) {
          acq.channelIndexes[channel.index] = c;
        }
      }
    }

    MetadataStore store = makeFilterMetadata();
    MetadataTools.populatePixels(store, this, true);

    for (int p=0; p<panoramas.size(); p++) {
      store.setImageName(panoramas.get(p).description, p);
    }
    for (int a=0; a<acquisitions.size(); a++) {
      int imageIndex = a + panoramas.size();
      setSeries(imageIndex);

      Acquisition acq = acquisitions.get(a);
      store.setImageName(acq.description, imageIndex);

      for (int c=0; c<acq.channelIndexes.length; c++) {
        Channel channel = channels.get(acq.channelIndexes[c]);

        // this is kind of wrong, but the reference viewer
        // exposes both the name and label (which are often slightly different)
        // and this is the easiest way to do that in OME/OMERO model
        int channelIndex = getPlaneIndex(c);
        store.setChannelName(channel.name, imageIndex, channelIndex);
        store.setChannelFluor(channel.label, imageIndex, channelIndex);
      }
    }
    setSeries(0);
  }

  // -- Helper methods --

  /**
   * Find the next string marker during header parsing.
   *
   * @return true if a marker was found
   */
  private boolean findMarker() throws IOException {
    while (in.getFilePointer() + 14 < in.length()) {
      if (in.readInt() == (int) 0xffffffff) {
        in.skipBytes(10);
        return true;
      }
      else {
        in.seek(in.getFilePointer() - 3);
      }
    }
    return false;
  }

  /**
   * Read a string, as needed during header parsing.
   * An int length is read first, followed by length bytes.
   *
   * @param s open stream from which to read
   * @return string read from the stream
   */
  private static String readString(RandomAccessInputStream s)
    throws IOException
  {
    int len = s.readInt();
    return s.readString(len);
  }

  /**
   * Calculate internal plane index.
   * Allows moving the XYZ channels to the end of the channel list.
   *
   * @param no original plane index
   * @return adjusted plane index
   */
  private int getPlaneIndex(int no) {
    if (getImageCount() <= 3) {
      return no;
    }
    if (no < 3) {
      return (getImageCount() - 3) + no;
    }
    return no - 3;
  }

  /**
   * Calculate internal plane index.
   * Allows moving the XYZ channels to the end of the channel list.
   *
   * @param no original plane index
   * @return adjusted plane index
   */
  private int getReversePlaneIndex(int no) {
    if (getImageCount() <= 3) {
      return no;
    }
    int threshold = getImageCount() - 3;
    if (no < threshold) {
      return no + 3;
    }
    return no - threshold;
  }

  // -- Helper class --

  class MCDHandler extends BaseHandler {
    private String currentElement;
    private Panorama currentPanorama = null;
    private Acquisition currentAcq = null;
    private Channel currentChannel = null;

    private String value = "";

    @Override
    public void characters(char[] ch, int start, int length) {
      value += new String(ch, start, length).trim();
    }

    @Override
    public void startElement(String uri, String localName, String qName,
      Attributes attributes)
    {
      currentElement = qName;

      if (qName.equals("Panorama")) {
        currentPanorama = new Panorama();
      }
      else if (qName.equals("Acquisition")) {
        currentAcq = new Acquisition();
      }
      else if (qName.equals("AcquisitionChannel")) {
        currentChannel = new Channel();
      }
    }

    @Override
    public void endElement(String uri, String localName, String qName) {
      if (qName.equals("Panorama")) {
        // an extra panorama is defined, with no bytes associated
        if (currentPanorama.description != null &&
          currentPanorama.start > 0 && currentPanorama.end > 0)
        {
          panoramas.add(currentPanorama);
        }
        currentPanorama = null;
      }
      else if (qName.equals("Acquisition")) {
        acquisitions.add(currentAcq);
        currentAcq = null;
      }
      else if (qName.equals("AcquisitionChannel")) {
        channels.add(currentChannel);
        currentChannel = null;
      }

      if (currentPanorama != null) {
        if ("ImageStartOffset".equals(currentElement)) {
          currentPanorama.start = Long.parseLong(value);
        }
        else if ("ImageEndOffset".equals(currentElement)) {
          currentPanorama.end = Long.parseLong(value);
        }
        else if ("Description".equals(currentElement)) {
          currentPanorama.description = value;
        }
        else if ("ImageFormat".equals(currentElement)) {
          currentPanorama.type = value;
        }
      }
      else if (currentAcq != null) {
        if ("ID".equals(currentElement)) {
          currentAcq.id = Integer.parseInt(value);
        }
        else if ("DataStartOffset".equals(currentElement)) {
          currentAcq.start = Long.parseLong(value);
        }
        else if ("DataEndOffset".equals(currentElement)) {
          currentAcq.end = Long.parseLong(value);
        }
        else if ("Description".equals(currentElement)) {
          currentAcq.description = value;
        }
        else if ("MaxX".equals(currentElement)) {
          currentAcq.sizeX = Integer.parseInt(value);
        }
        else if ("MaxY".equals(currentElement)) {
          currentAcq.sizeY = Integer.parseInt(value);
        }
        else if ("SegmentDataFormat".equals(currentElement)) {
          currentAcq.dataType = value;
        }
        else if ("ValueBytes".equals(currentElement)) {
          currentAcq.bpp = Integer.parseInt(value);
        }
      }
      else if (currentChannel != null) {
        if ("ID".equals(currentElement)) {
          currentChannel.id = Integer.parseInt(value);
        }
        else if ("ChannelName".equals(currentElement)) {
          currentChannel.name = value;
        }
        else if ("OrderNumber".equals(currentElement)) {
          currentChannel.index = Integer.parseInt(value);
        }
        else if ("AcquisitionID".equals(currentElement)) {
          currentChannel.acqID = Integer.parseInt(value);
        }
        else if ("ChannelLabel".equals(currentElement)) {
          currentChannel.label = value;
        }
      }
      value = "";
    }
  }

  /**
   * Models a panorama (similar to overview/macro) image.
   * Example data so far indicates this is always a single embedded PNG.
   */
  class Panorama {
    public String description;
    public String type;
    public long start;
    public long end;
    private boolean mapped = false;

    /**
     * Map the range of bytes that contain the panorama image
     * into a Location ID. This allows a reader to be initialized
     * only on the defined byte range, but does require all of the
     * bytes to be read into memory.
     * If this panorama's data has already been mapped,
     * returns without re-mapping.
     *
     * @param s open stream from which to map bytes
     * @return ID to be used with Location
     */
    public String map(RandomAccessInputStream s) throws IOException {
      String ref = description + "." + type.toLowerCase();
      if (mapped) {
        return ref;
      }

      s.order(false);
      s.seek(start + 19);
      String version = readString(s);
      s.skipBytes(2);
      String bitmapMarker = readString(s);
      s.skipBytes(1);
      String dataMarker = readString(s);
      s.skipBytes(21);

      long totalBytes = end - s.getFilePointer();
      if (totalBytes < 0 || totalBytes > Integer.MAX_VALUE) {
        throw new IOException(
          "Panorama size is invalid (" + totalBytes + " bytes)");
      }
      byte[] buf = new byte[(int) totalBytes];
      s.readFully(buf);

      ByteArrayHandle stream = new ByteArrayHandle(buf);
      Location.mapFile(ref, stream);
      return ref;
    }

    /**
     * Free byte array mapped during a previous call to map().
     */
    public void unmap() {
      String ref = description + "." + type.toLowerCase();
      Location.mapFile(ref, null);
      mapped = false;
    }
  }

  /**
   * Models a single acquisition. This is the raw data.
   */
  class Acquisition {
    /** Acquisition ID, starting from 1. */
    public int id;
    public String description;

    /** File pointer indicating start of pixel data. */
    public long start;

    /** File pointer indicating end of pixel data. */
    public long end;
    public int sizeX;
    public int sizeY;

    /** Pixel type; only "Float" recognized so far. */
    public String dataType;

    /** Bytes per pixel, based on data type. */
    public int bpp;

    /** Indexes into list of channels; length matches SizeC. */
    public int[] channelIndexes;
  }

  /**
   * Models a single channel.
   * Channels definitions are not shared across acquisitions,
   * so this contains enough information to map a specific channel
   * definition back to a single channel index within a particular acquisition.
   */
  class Channel {
    public int id; // starts from 0
    public int index; // starts from 0
    public String name;
    public String label;
    public int acqID; // starts from 1, matches up with Acquisition.id
  }

}
