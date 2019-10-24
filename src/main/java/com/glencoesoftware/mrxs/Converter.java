package com.glencoesoftware.mrxs;

import java.io.IOException;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

public class Converter implements Callable<Void> {

  private static final Logger LOGGER =
    LoggerFactory.getLogger(MiraxReader.class);

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
    required = true,
    description = "Number of pyramid resolutions to generate"
  )
  private int pyramidResolutions = 0;

  @Option(
    names = {"-w", "--tile-width"},
    description = "Maximum tile width to read (default: 2048)"
  )
  private int tileWidth = 2048;

  @Option(
    names = {"-h", "--tile-height"},
    description = "Maximum tile height to read (default: 2048)"
  )
  private int tileHeight = 2048;

  @Option(
    names = {"-c", "--compression"},
    description = "Compression type for output file (default: JPEG-2000)"
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

  public void convert() throws FormatException, IOException {
    ClassList<IFormatReader> readers = ImageReader.getDefaultReaderClasses();
    readers.addClass(0, MiraxReader.class);
    ImageReader reader = new ImageReader(readers);
    reader.setFlattenedResolutions(false);
    reader.setMetadataFiltered(true);

    reader.setMetadataStore(createMetadata());

    try {
      reader.setId(inputFile);

      // set up extra resolutions to be generated
      // assume first series is the largest resolution

      OMEPyramidStore meta = (OMEPyramidStore) reader.getMetadataStore();
      int width = meta.getPixelsSizeX(0).getValue();
      int height = meta.getPixelsSizeY(0).getValue();
      for (int i=1; i<=pyramidResolutions; i++) {
        int scale = (int) Math.pow(PYRAMID_SCALE, i);

        if (legacy) {
          MetadataTools.populateMetadata(meta, i, null,
            reader.isLittleEndian(), "XYCZT",
            FormatTools.getPixelTypeString(reader.getPixelType()),
            width / scale, height / scale, reader.getSizeZ(),
            reader.getSizeC(), reader.getSizeT(), reader.getRGBChannelCount());
        }
        else {
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

  public void saveResolution(int resolution, IFormatReader reader,
    TiffWriter writer)
    throws FormatException, IOException
  {
    int scale = (int) Math.pow(PYRAMID_SCALE, resolution);
    int xStep = tileWidth / scale;
    int yStep = tileHeight / scale;

    int sizeX = reader.getSizeX() / scale;
    int sizeY = reader.getSizeY() / scale;

    for (int plane=0; plane<reader.getImageCount(); plane++) {
      LOGGER.info("writing plane {} of {}", plane, reader.getImageCount());
      IFD ifd = makeIFD(scale);

      for (int yy=0; yy<sizeY; yy+=yStep) {
        int height = (int) Math.min(yStep, sizeY - yy);
        for (int xx=0; xx<sizeX; xx+=xStep) {
          int width = (int) Math.min(xStep, sizeX - xx);
          byte[] tile =
            getTile(reader, resolution, plane, xx, yy, width, height);
          writer.saveBytes(plane, tile, ifd, xx,  yy, width, height);
        }
      }
    }
  }

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

  private IFD makeIFD(int scale) {
    IFD ifd = new IFD();
    ifd.put(IFD.TILE_WIDTH, tileWidth / scale);
    ifd.put(IFD.TILE_LENGTH, tileHeight / scale);
    return ifd;
  }

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

  public static void main(String[] args) {
    CommandLine.call(new Converter(), args);
  }

}
