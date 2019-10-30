/**
 * Copyright (c) 2019 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.mrxs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import loci.common.image.IImageScaler;
import loci.common.image.SimpleImageScaler;
import loci.common.services.DependencyException;
import loci.common.services.ServiceException;
import loci.common.services.ServiceFactory;
import loci.formats.FormatException;
import loci.formats.FormatTools;
import loci.formats.IFormatReader;
import loci.formats.IFormatWriter;
import loci.formats.ImageWriter;
import loci.formats.MetadataTools;
import loci.formats.MissingLibraryException;
import loci.formats.meta.IMetadata;
import loci.formats.out.TiffWriter;
import loci.formats.services.OMEXMLService;
import loci.formats.services.OMEXMLServiceImpl;
import ome.xml.model.enums.DimensionOrder;
import ome.xml.model.enums.EnumerationException;
import ome.xml.model.enums.PixelType;
import ome.xml.model.primitives.PositiveInteger;

import org.perf4j.slf4j.Slf4JStopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
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

  @Parameters(
    index = "0",
    arity = "1",
    description = ".mrxs file to convert"
  )
  private Path inputPath;

  @Parameters(
    index = "1",
    arity = "1",
    description = "path to the output pyramid directory"
  )
  private Path outputPath;

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

  @Option(
    names = "--max_workers",
    description = "Maximum number of workers (default: ${DEFAULT-VALUE})"
  )
  private int maxWorkers = Runtime.getRuntime().availableProcessors();

  private IImageScaler scaler = new SimpleImageScaler();

  private BlockingQueue<IFormatReader> readers;

  private BlockingQueue<Runnable> queue;

  private ExecutorService executor;

  private int series;

  private boolean isLittleEndian;

  private int resolutions;

  private int sizeX;

  private int sizeY;

  private int sizeC;

  private int rgbChannelCount;

  private boolean isInterleaved;

  private int imageCount;

  private int pixelType;

  private int tileCount;

  private AtomicInteger nTile;

  public Converter() {
  }

  @Override
  public Void call() throws InterruptedException {
    ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger)
        LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    if (debug) {
      root.setLevel(Level.DEBUG);
    } else {
      root.setLevel(Level.INFO);
    }
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
   * @throws InterruptedException
   */
  public void convert()
      throws FormatException, IOException, InterruptedException {
    readers = new ArrayBlockingQueue<IFormatReader>(maxWorkers);
    queue = new LimitedQueue<Runnable>(maxWorkers);
    executor = new ThreadPoolExecutor(
      maxWorkers, maxWorkers, 0L, TimeUnit.MILLISECONDS, queue);
    for (int i=0; i < maxWorkers; i++) {
      IFormatReader reader = new MiraxReader();
      reader.setFlattenedResolutions(false);
      reader.setMetadataFiltered(true);
      reader.setMetadataStore(createMetadata());
      reader.setId(inputPath.toString());
      reader.setResolution(0);
      readers.add(reader);
    }

    try {
      // calculate a reasonable pyramid depth if not specified as an argument
      IFormatReader reader = readers.take();
      try {
        if (pyramidResolutions == 0) {
          int width = reader.getSizeX();
          int height = reader.getSizeY();
          while (width > MIN_SIZE || height > MIN_SIZE) {
            pyramidResolutions++;
            width /= PYRAMID_SCALE;
            height /= PYRAMID_SCALE;
          }
        }
      }
      finally {
        readers.put(reader);
      }

      write();
    }
    finally {
      // Shut down first, tasks may still be running
      executor.shutdown();
      executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
      readers.forEach((v) -> {
        try {
          v.close();
        } catch (IOException e) {
          LOGGER.error("Exception while closing reader", e);
        }
      });
    }
  }

  /**
   * Convert the data specified by the given initialized reader to
   * an intermediate form.
   *
   * @throws FormatException
   * @throws IOException
   * @throws InterruptedException 
   */
  public void write()
    throws FormatException, IOException, InterruptedException
  {
      int seriesCount;
      IFormatReader v = readers.take();
      try {
        seriesCount = v.getSeriesCount();
      }
      finally {
        readers.put(v);
      }
      for (int i=0; i<seriesCount; i++) {
        final int series = i;
        readers.forEach((reader) -> {
          reader.setSeries(series);
        });

        saveResolutions();
        return;
      }
  }

  public void processTile(
      int plane, int xx, int yy, int width, int height)
        throws EnumerationException, FormatException, IOException,
          InterruptedException {
    Slf4JStopWatch t0 = new Slf4JStopWatch("getTile");
    byte[] tile;
    IFormatReader reader = readers.take();
    try {
      tile = reader.openBytes(plane, xx, yy, width, height);
    }
    finally {
      readers.put(reader);
      nTile.incrementAndGet();
      LOGGER.info("tile read complete {}/{}", nTile.get(), tileCount);
      t0.stop();
    }
    for (int resolution=0; resolution<resolutions; resolution++) {
      Path directory = outputPath
          .resolve(Integer.toString(resolution))
          .resolve(Integer.toString(xx));
      Files.createDirectories(directory);
      int scale = (int) Math.pow(PYRAMID_SCALE, resolution);
      Slf4JStopWatch t1 = stopWatch();
      try (IFormatWriter writer = createWriter(
          pixelType, width / scale, height / scale, sizeC)) {
        byte[] scaledTile = tile;
        if (resolution > 0) {
          scaledTile = scaler.downsample(tile, width, height,
            scale, FormatTools.getBytesPerPixel(pixelType),
            isLittleEndian, FormatTools.isFloatingPoint(pixelType),
            rgbChannelCount, isInterleaved);
        }
        Path id = directory.resolve(String.format("%d.tiff", yy));
        LOGGER.info("Writing to: {}", id);
        writer.setId(id.toString());
        writer.saveBytes(0, scaledTile);
      }
      finally {
        t1.stop("saveBytes");
      }
    }
    return;
  }

  /**
   * Write all resolutions for the current series to an intermediate form.
   * Readers should be initialized and have the correct series state.
   *
   * @throws FormatException
   * @throws IOException
   * @throws InterruptedException 
   */
  public void saveResolutions()
    throws FormatException, IOException, InterruptedException
  {
    IFormatReader _reader = readers.take();
    try {
      series = _reader.getSeries();
      isLittleEndian = _reader.isLittleEndian();
      resolutions = series == 0 ? pyramidResolutions + 1: 1;
      sizeX = _reader.getSizeX();
      sizeY = _reader.getSizeY();
      sizeC = _reader.getEffectiveSizeC();
      rgbChannelCount = _reader.getRGBChannelCount();
      isInterleaved = _reader.isInterleaved();
      imageCount = _reader.getImageCount();
      pixelType = _reader.getPixelType();
    }
    finally {
      readers.put(_reader);
    }
    tileCount = (int) Math.ceil((double) sizeX / tileWidth)
      * (int) Math.ceil((double) sizeY / tileHeight)
      * imageCount;

    LOGGER.info(
      "Preparing to read series {} sizeX {} (tileWidth: {}) " +
      "sizeY {} (tileWidth: {}) imageCount {}",
        series, sizeX, tileWidth, sizeY, tileHeight, imageCount
    );

    // Prepare directories
    for (int resolution=0; resolution<resolutions; resolution++) {
      Path tileDirectory =
          outputPath.resolve(Integer.toString(resolution));
        Files.createDirectories(tileDirectory);
    }

    nTile = new AtomicInteger(0);
    for (int i=0; i<imageCount; i++) {
      final int plane = i;
      LOGGER.info("writing plane {} of {}", plane, imageCount);

      for (int j=0; j<sizeY; j+=tileHeight) {
        final int yy = j;
        int height = (int) Math.min(tileHeight, sizeY - yy);
        for (int k=0; k<sizeX; k+=tileWidth) {
          final int xx = k;
          int width = (int) Math.min(tileWidth, sizeX - xx);

          executor.execute(() -> {
            try {
              processTile(plane, xx, yy, width, height);
            }
            catch (Exception e) {
              LOGGER.error(
                "Failure processing tile; plane={} xx={} yy={} " +
                "width={} height={}", plane, xx, yy, width, height, e);
            }
          });
        }
      }
    }
  }

  /**
   * Create a writer with all appropriate options and initialize it.
   *
   * @param pixelType
   * @param sizeX
   * @param sizeY
   * @param sizeC
   * @return
   * @throws EnumerationException
   */
  private IFormatWriter createWriter(int pixelType, int sizeX, int sizeY, int sizeC)
      throws EnumerationException {
    IMetadata metadata = MetadataTools.createOMEXMLMetadata();
    metadata.setImageID("Image:0", 0);
    metadata.setPixelsID("Pixels:0", 0);
    metadata.setChannelID("Channel:0:0", 0, 0);
    metadata.setChannelSamplesPerPixel(new PositiveInteger(1), 0, 0);
    metadata.setPixelsBigEndian(true, 0);
    metadata.setPixelsSizeX(
      new PositiveInteger(sizeX), 0);
    metadata.setPixelsSizeY(
      new PositiveInteger(sizeY), 0);
    metadata.setPixelsSizeZ(new PositiveInteger(1), 0);
    metadata.setPixelsSizeC(new PositiveInteger(sizeC), 0);
    metadata.setPixelsSizeT(new PositiveInteger(1), 0);
    metadata.setPixelsDimensionOrder(DimensionOrder.XYCZT, 0);
    metadata.setPixelsType(PixelType.fromString(
      FormatTools.getPixelTypeString(pixelType)), 0);
    ImageWriter writer = new ImageWriter();
    writer.setMetadataRetrieve(metadata);
    return writer;
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
