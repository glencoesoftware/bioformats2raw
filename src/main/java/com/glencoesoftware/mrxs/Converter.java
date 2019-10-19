package com.glencoesoftware.mrxs;

import java.io.IOException;
import java.util.concurrent.Callable;

import loci.common.DebugTools;
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
import loci.formats.MissingLibraryException;
import loci.formats.ome.OMEPyramidStore;
import loci.formats.out.PyramidOMETiffWriter;
import loci.formats.services.OMEXMLService;
import loci.formats.services.OMEXMLServiceImpl;
import loci.formats.tiff.IFD;

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

    OMEXMLService service = null;
    try {
      ServiceFactory factory = new ServiceFactory();
      service = factory.getInstance(OMEXMLService.class);
      reader.setMetadataStore(service.createOMEXMLMetadata());
    }
    catch (DependencyException de) {
      throw new MissingLibraryException(OMEXMLServiceImpl.NO_OME_XML_MSG, de);
    }
    catch (ServiceException se) {
      throw new FormatException(se);
    }

    try {
      reader.setId(inputFile);

      // set up extra resolutions to be generated
      // assume first series is the largest resolution

      // TODO: need extra Images if legacy
      OMEPyramidStore meta = (OMEPyramidStore) reader.getMetadataStore();
      int width = meta.getPixelsSizeX(0).getValue();
      int height = meta.getPixelsSizeY(0).getValue();
      for (int i=1; i<pyramidResolutions; i++) {
        int scale = (int) Math.pow(PYRAMID_SCALE, i);
        meta.setResolutionSizeX(new PositiveInteger(width / scale), 0, i);
        meta.setResolutionSizeY(new PositiveInteger(height / scale), 0, i);
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

  public void writeFaasPyramid(IFormatReader reader, OMEPyramidStore meta, String outputFile) {
    // TODO
  }

  /**
   * Convert the data specified by the given initialized reader to
   * a Bio-Formats 6.x-compatible OME-TIFF pyramid with the given
   * file name.  If the reader does not represent a pyramid,
   * additional resolutions will be generated as needed.
   *
   * @param reader an initialized reader
   * @param outputFile the path to the output OME-TIFF file
   */
  public void writeOMEPyramid(IFormatReader reader, OMEPyramidStore meta, String outputFile)
    throws FormatException, IOException
  {
    try (PyramidOMETiffWriter writer = new PyramidOMETiffWriter()) {
      writer.setBigTiff(true);
      writer.setMetadataRetrieve(meta);
      writer.setInterleaved(reader.isInterleaved());
      writer.setCompression(compression);
      writer.setWriteSequentially(true);
      writer.setId(outputFile);

      for (int i=0; i<reader.getSeriesCount(); i++) {
        reader.setSeries(i);
        writer.setSeries(i);

        int resolutions = i == 0 ? pyramidResolutions + 1: 1;
        for (int r=0; r<resolutions; r++) {
          writer.setResolution(r);

          LOGGER.info("writing resolution {} in series {}", r, i);

          int scale = (int) Math.pow(PYRAMID_SCALE, r);
          int xStep = tileWidth / scale;
          int yStep = tileHeight / scale;

          int sizeX = reader.getSizeX() / scale;
          int sizeY = reader.getSizeY() / scale;

          for (int plane=0; plane<reader.getImageCount(); plane++) {
            LOGGER.info("writing plane {} of {}", plane, reader.getImageCount());
            IFD ifd = new IFD();
            ifd.put(IFD.TILE_WIDTH, xStep);
            ifd.put(IFD.TILE_LENGTH, yStep);

            for (int yy=0; yy<sizeY; yy+=yStep) {
              int height = (int) Math.min(yStep, sizeY - yy);
              for (int xx=0; xx<sizeX; xx+=xStep) {
                int width = (int) Math.min(xStep, sizeX - xx);
                byte[] tile = getTile(reader, r, plane, xx, yy, width, height);
                writer.saveBytes(plane, tile, ifd, xx,  yy, width, height);
              }
            }
          }
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

  public static void main(String[] args) {
    CommandLine.call(new Converter(), args);
  }

}
