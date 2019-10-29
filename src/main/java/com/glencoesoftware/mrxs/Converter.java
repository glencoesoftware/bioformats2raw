/**
 * Copyright (c) 2019 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.mrxs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import loci.common.DebugTools;
import loci.common.RandomAccessInputStream;
import loci.common.RandomAccessOutputStream;
import loci.common.image.IImageScaler;
import loci.common.image.SimpleImageScaler;
import loci.common.services.DependencyException;
import loci.common.services.ServiceException;
import loci.common.services.ServiceFactory;
import loci.formats.ClassList;
import loci.formats.FormatException;
import loci.formats.FormatTools;
import loci.formats.IFormatReader;
import loci.formats.ImageReader;
import loci.formats.MetadataTools;
import loci.formats.MissingLibraryException;
import loci.formats.meta.IMetadata;
import loci.formats.ome.OMEPyramidStore;
import loci.formats.out.JPEGWriter;
import loci.formats.out.PyramidOMETiffWriter;
import loci.formats.out.TiffWriter;
import loci.formats.services.OMEXMLService;
import loci.formats.services.OMEXMLServiceImpl;
import loci.formats.tiff.IFD;
import loci.formats.tiff.TiffSaver;

import ome.xml.model.primitives.PositiveInteger;

import org.perf4j.slf4j.Slf4JStopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Command line tool for converting .mrxs files to TIFF/OME-TIFF.
 * Both Bio-Formats 5.9.x ("Faas") pyramids and true OME-TIFF pyramids
 * are supported.
 */
public class Converter implements Callable<Void> {

  private static final Logger LOGGER = LoggerFactory.getLogger(Converter.class);

  static class CompressionTypes extends ArrayList<String> {
    CompressionTypes() {
      super(CompressionTypes.getCompressionTypes());
    }

    private static List<String> getCompressionTypes() {
      try (TiffWriter v = new TiffWriter()) {
        return Arrays.asList(v.getCompressionTypes());
      }
      catch (Exception e) {
        return new ArrayList<String>();
      }
    }
  }

  // minimum size of the largest XY dimension in the smallest resolution,
  // when calculating the number of resolutions to generate
  private static final int MIN_SIZE = 256;

  // scaling factor in X and Y between any two consecutive resolutions
  private static final int PYRAMID_SCALE = 2;

  @Option(
    names = "--output",
    arity = "1",
    required = true,
    description = "Relative path to the output pyramid file"
  )
  private String outputFile;

  @Parameters(
    index = "0",
    arity = "1",
    description = ".mrxs file to convert"
  )
  private String inputFile;

  @Option(
    names = {"-r", "--resolutions"},
    description = "Number of pyramid resolutions to generate"
  )
  private int pyramidResolutions = 0;

  @Option(
    names = {"-w", "--tile-width"},
    description = "Maximum tile width to read (default: ${DEFAULT-VALUE})"
  )
  private int tileWidth = 2048;

  @Option(
    names = {"-h", "--tile-height"},
    description = "Maximum tile height to read (default: ${DEFAULT-VALUE})"
  )
  private int tileHeight = 2048;

  @Option(
      names = {"-c", "--compression"},
      completionCandidates = CompressionTypes.class,
      description = "Compression type for output OME-TIFF file " +
                    "(${COMPLETION-CANDIDATES}; default: ${DEFAULT-VALUE})"
  )
  private String compression = "JPEG-2000";

  @Option(
    names = "--legacy",
    description = "Write a Bio-Formats 5.9.x pyramid instead of OME-TIFF"
  )
  private boolean legacy = false;

  @Option(
    names = "--debug",
    description = "Turn on debug logging"
  )
  private boolean debug = false;

  private IImageScaler scaler = new SimpleImageScaler();

  public Converter() {
  }

  @Override
  public Void call() {
    DebugTools.enableLogging(debug ? "DEBUG" : "INFO");
    try {
      convert();
    }
    catch (FormatException|IOException e) {
      throw new RuntimeException("Could not create pyramid", e);
    }
    return null;
  }

  /**
   * Perform the pyramid conversion according to the specified
   * command line arguments.
   *
   * @throws FormatException
   * @throws IOException
   */
  public void convert() throws FormatException, IOException {
    // insert our own MiraxReader so that .mrxs files are correctly detected
    ClassList<IFormatReader> readers = ImageReader.getDefaultReaderClasses();
    readers.addClass(0, MiraxReader.class);
    ImageReader reader = new ImageReader(readers);
    reader.setFlattenedResolutions(false);
    reader.setMetadataFiltered(true);

    reader.setMetadataStore(createMetadata());

    try {
      reader.setId(inputFile);

      // calculate a reasonable pyramid depth if not specified as an argument
      if (pyramidResolutions == 0) {
        int width = reader.getSizeX();
        int height = reader.getSizeY();
        while (width > MIN_SIZE || height > MIN_SIZE) {
          pyramidResolutions++;
          width /= PYRAMID_SCALE;
          height /= PYRAMID_SCALE;
        }
      }

      // set up extra resolutions to be generated
      // assume first series is the largest resolution

      OMEPyramidStore meta = (OMEPyramidStore) reader.getMetadataStore();
      int width = meta.getPixelsSizeX(0).getValue();
      int height = meta.getPixelsSizeY(0).getValue();
      for (int i=1; i<=pyramidResolutions; i++) {
        int scale = (int) Math.pow(PYRAMID_SCALE, i);

        if (legacy) {
          // writing 5.9.x pyramids requires one Image per resolution
          // the relationship between the resolutions is implicit
          MetadataTools.populateMetadata(meta, i, null,
            reader.isLittleEndian(), "XYCZT",
            FormatTools.getPixelTypeString(reader.getPixelType()),
            width / scale, height / scale, reader.getSizeZ(),
            reader.getSizeC(), reader.getSizeT(), reader.getRGBChannelCount());
        }
        else {
          // writing 6.x pyramids requires using the OMEPyramidStore API
          // to explicitly define subresolutions of the current Image
          meta.setResolutionSizeX(new PositiveInteger(width / scale), 0, i);
          meta.setResolutionSizeY(new PositiveInteger(height / scale), 0, i);
        }
      }

      if (legacy) {
        writeFaasPyramid(reader, meta, outputFile);
      }
      else {
        writeOMEPyramid(reader, meta, outputFile);
      }
    }
    finally {
      reader.close();
    }
  }

  /**
   * Convert the data specified by the given initialized reader to
   * a Bio-Formats 5.9.x-compatible TIFF pyramid with the given
   * file name.  If the reader does not represent a pyramid,
   * additional resolutions will be generated as needed.
   *
   * @param reader an initialized reader
   * @param meta metadata store specifying the pyramid resolutions
   * @param outputFile the path to the output TIFF file
   * @throws FormatException
   * @throws IOException
   */
  public void writeFaasPyramid(IFormatReader reader, IMetadata meta,
    String outputFile)
    throws FormatException, IOException
  {
    // write the pyramid first
    try (TiffWriter writer = new TiffWriter()) {
      setupWriter(reader, writer, meta, outputFile);

      reader.setSeries(0);
      for (int r=0; r<=pyramidResolutions; r++) {
        writer.setSeries(r);
        LOGGER.info("writing resolution {}", r);
        saveResolution(r, reader, writer);
      }
    }

    // overwrite the TIFF comment so that the file will be detected as a pyramid
    RandomAccessInputStream in = null;
    RandomAccessOutputStream out = null;
    try {
      in = new RandomAccessInputStream(outputFile);
      out = new RandomAccessOutputStream(outputFile);
      TiffSaver saver = new TiffSaver(out, outputFile);
      saver.overwriteComment(in, "Faas-mrxs2ometiff");
    }
    finally {
      if (in != null) {
        in.close();
      }
      if (out != null) {
        out.close();
      }
    }

    // write any extra images to separate files
    // since the 5.9.x pyramid reader doesn't support extra images
    String baseOutput = outputFile.substring(0, outputFile.lastIndexOf("."));
    for (int i=1; i<reader.getSeriesCount(); i++) {
      LOGGER.info("Writing extra image #{}", i);

      reader.setSeries(i);
      IMetadata extraMeta = createMetadata();
      MetadataTools.populateMetadata(extraMeta, 0, null,
        reader.getCoreMetadataList().get(reader.getCoreIndex()));
      try (JPEGWriter writer = new JPEGWriter()) {
        writer.setMetadataRetrieve(extraMeta);
        writer.setId(baseOutput + "-" + i + ".jpg");
        writer.saveBytes(0, reader.openBytes(0));
      }
    }
  }

  /**
   * Convert the data specified by the given initialized reader to
   * a Bio-Formats 6.x-compatible OME-TIFF pyramid with the given
   * file name.  If the reader does not represent a pyramid,
   * additional resolutions will be generated as needed.
   *
   * @param reader an initialized reader
   * @param meta metadata store specifying the pyramid resolutions
   * @param outputFile the path to the output OME-TIFF file
   * @throws FormatException
   * @throws IOException
   */
  public void writeOMEPyramid(IFormatReader reader, OMEPyramidStore meta,
    String outputFile)
    throws FormatException, IOException
  {
    try (PyramidOMETiffWriter writer = new PyramidOMETiffWriter()) {
      setupWriter(reader, writer, meta, outputFile);

      for (int i=0; i<reader.getSeriesCount(); i++) {
        reader.setSeries(i);
        writer.setSeries(i);

        int resolutions = i == 0 ? pyramidResolutions + 1: 1;
        for (int r=0; r<resolutions; r++) {
          writer.setResolution(r);
          LOGGER.info("writing resolution {} in series {}", r, i);
          saveResolution(r, reader, writer);
        }
      }
    }
  }

  /**
   * Write the given resolution, using the given writer.
   * Pixel data is read from the given reader and downsampled accordingly.
   * The reader and writer should be initialized and have the correct
   * series and/or resolution state.
   *
   * @param resolution the pyramid resolution to write, indexed from 0
   * @param reader the reader from which to read pixel data
   * @param writer the writer to use for saving pixel data
   * @throws FormatException
   * @throws IOException
   */
  public void saveResolution(int resolution, IFormatReader reader,
    TiffWriter writer)
    throws FormatException, IOException
  {
    int scale = (int) Math.pow(PYRAMID_SCALE, resolution);
    int xStep = tileWidth / scale;
    int yStep = tileHeight / scale;

    int sizeX = reader.getSizeX() / scale;
    int sizeY = reader.getSizeY() / scale;

    Slf4JStopWatch watch = null;
    for (int plane=0; plane<reader.getImageCount(); plane++) {
      LOGGER.info("writing plane {} of {}", plane, reader.getImageCount());
      IFD ifd = makeIFD(scale);

      for (int yy=0; yy<sizeY; yy+=yStep) {
        int height = (int) Math.min(yStep, sizeY - yy);
        for (int xx=0; xx<sizeX; xx+=xStep) {
          int width = (int) Math.min(xStep, sizeX - xx);
          watch = stopWatch();
          byte[] tile =
            getTile(reader, resolution, plane, xx, yy, width, height);
          watch.stop("getTile: resolution = " + resolution);
          watch = stopWatch();
          writer.saveBytes(plane, tile, ifd, xx,  yy, width, height);
          watch.stop("saveBytes");
        }
      }
    }
  }

  /**
   * Retrieve a tile of pixels corresponding to the given resolution,
   * plane index, and tile bounding box.  Downsampling is performed as needed.
   *
   * @param reader the initialized reader used to retrieve pixel data
   * @param res the pyramid resolution (from 0)
   * @param no the plane index
   * @param x the X coordinate of the upper-left corner of the tile
   * @param y the Y coordinate of the upper-left corner of the tile
   * @param w the tile width in pixels
   * @param h the tile height in pixels
   * @throws FormatException
   * @throws IOException
   */
  public byte[] getTile(IFormatReader reader, int res,
    int no, int x, int y, int w, int h)
    throws FormatException, IOException
  {
    if (res == 0) {
      reader.setResolution(res);
      return reader.openBytes(no, x, y, w, h);
    }

    int scale = (int) Math.pow(PYRAMID_SCALE, res);
    byte[] fullTile = getTile(reader, 0, no,
      x * scale, y * scale, w * scale, h * scale);
    int pixelType = reader.getPixelType();
    return scaler.downsample(fullTile, w * scale, h * scale,
      scale, FormatTools.getBytesPerPixel(pixelType),
      reader.isLittleEndian(), FormatTools.isFloatingPoint(pixelType),
      reader.getRGBChannelCount(), reader.isInterleaved());
  }

  /**
   * Set all appropriate options on an uninitialized writer and initialize it.
   *
   * @param reader
   * @param writer the writer to initialize
   * @param meta the initialized IMetadata object to be used by the writer
   * @param outputFile the absolute path to the output file
   * @throws FormatException
   * @throws IOException
   */
  private void setupWriter(IFormatReader reader, TiffWriter writer,
    IMetadata meta, String outputFile)
    throws FormatException, IOException
  {
    writer.setBigTiff(true);
    writer.setMetadataRetrieve(meta);
    writer.setInterleaved(reader.isInterleaved());
    writer.setCompression(compression);
    writer.setWriteSequentially(true);
    writer.setId(outputFile);
  }

  /**
   * Construct an IFD with the given scale factor.
   * The scale factor is applied to the current tile dimensions,
   * and is used to set the tile dimensions for this IFD.
   *
   * @param scale tile dimension scale factor
   * @return IFD object with tile dimensions populated
   */
  private IFD makeIFD(int scale) {
    IFD ifd = new IFD();
    ifd.put(IFD.TILE_WIDTH, tileWidth / scale);
    ifd.put(IFD.TILE_LENGTH, tileHeight / scale);
    return ifd;
  }

  /**
   * @return an empty IMetadata object for metadata transport.
   * @throws FormatException
   */
  private IMetadata createMetadata() throws FormatException {
    OMEXMLService service = null;
    try {
      ServiceFactory factory = new ServiceFactory();
      service = factory.getInstance(OMEXMLService.class);
      return service.createOMEXMLMetadata();
    }
    catch (DependencyException de) {
      throw new MissingLibraryException(OMEXMLServiceImpl.NO_OME_XML_MSG, de);
    }
    catch (ServiceException se) {
      throw new FormatException(se);
    }
  }

  private Slf4JStopWatch stopWatch() {
    return new Slf4JStopWatch(LOGGER, Slf4JStopWatch.DEBUG_LEVEL);
  }

  public static void main(String[] args) {
    CommandLine.call(new Converter(), args);
  }

}
