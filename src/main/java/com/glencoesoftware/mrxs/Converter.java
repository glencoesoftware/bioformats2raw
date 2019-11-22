/**
 * Copyright (c) 2019 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.mrxs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
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
import loci.formats.ImageWriter;
import loci.formats.MetadataTools;
import loci.formats.MissingLibraryException;
import loci.formats.meta.IMetadata;
import loci.formats.services.OMEXMLService;
import loci.formats.services.OMEXMLServiceImpl;
import ome.xml.model.enums.EnumerationException;

import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.ShortArrayDataBlock;
import org.janelia.saalfeldlab.n5.blosc.BloscCompression;
import org.perf4j.slf4j.Slf4JStopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.glencoesoftware.mrxs.MiraxReader.TilePointer;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import ch.qos.logback.classic.Level;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Command line tool for converting whole slide imaging files to N5.
 */
public class Converter implements Callable<Void> {

  private static final Logger LOGGER = LoggerFactory.getLogger(Converter.class);

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
  private Integer pyramidResolutions;

  @Option(
    names = {"-w", "--tile_width"},
    description = "Maximum tile width to read (default: ${DEFAULT-VALUE})"
  )
  private int tileWidth = 1024;

  @Option(
    names = {"-h", "--tile_height"},
    description = "Maximum tile height to read (default: ${DEFAULT-VALUE})"
  )
  private int tileHeight = 1024;

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

  @Option(
    names = "--max_cached_tiles",
    description =
      "Maximum number of tiles that will be cached across all "
      + "workers (default: ${DEFAULT-VALUE})"
  )
  private int maxCachedTiles = 64;

  private IImageScaler scaler = new SimpleImageScaler();

  private BlockingQueue<IFormatReader> readers;

  private BlockingQueue<Runnable> queue;

  private ExecutorService executor;

  private boolean isLittleEndian;

  private int resolutions;

  private int sizeX;

  private int sizeY;

  private int rgbChannelCount;

  private boolean isInterleaved;

  private int imageCount;

  private int pixelType;

  private int tileCount;

  private AtomicInteger nTile;

  @Override
  public Void call()
      throws FormatException, IOException, InterruptedException
  {
    ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger)
        LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    if (debug) {
      root.setLevel(Level.DEBUG);
    }
    else {
      root.setLevel(Level.INFO);
    }
    convert();
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
      throws FormatException, IOException, InterruptedException
  {
    readers = new ArrayBlockingQueue<IFormatReader>(maxWorkers);
    queue = new LimitedQueue<Runnable>(maxWorkers);
    executor = new ThreadPoolExecutor(
      maxWorkers, maxWorkers, 0L, TimeUnit.MILLISECONDS, queue);
    Cache<TilePointer, byte[]> tileCache = CacheBuilder.newBuilder()
        .maximumSize(maxCachedTiles)
        .build();
    try {
      for (int i=0; i < maxWorkers; i++) {
        MiraxReader reader = new MiraxReader();
        reader.setFlattenedResolutions(false);
        reader.setMetadataFiltered(true);
        reader.setMetadataStore(createMetadata());
        reader.setId(inputPath.toString());
        reader.setResolution(0);
        reader.setTileCache(tileCache);
        readers.add(reader);
      }

      try {
        // only process the first series here
        // wait until all tiles have been written
        // before processing the remaining series
        //
        // otherwise, the readers' series will be changed from under
        // in-process tiles, leading to exceptions
        write(0);
      }
      catch (Exception e) {
        LOGGER.error("Error while writing series 0");
        return;
      }
      finally {
        // Shut down first, tasks may still be running
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
      }

      int seriesCount;
      IFormatReader v = readers.take();
      try {
        seriesCount = v.getSeriesCount();
        IMetadata meta = (IMetadata) v.getMetadataStore();
        String xml = getService().getOMEXML(meta);

        // write the original OME-XML to a file
        Path omexmlFile = outputPath.resolve("METADATA.ome.xml");
        Files.write(omexmlFile, xml.getBytes());
      }
      catch (ServiceException se) {
        LOGGER.error("Could not retrieve OME-XML", se);
        return;
      }
      finally {
        readers.put(v);
      }

      // write each of the extra images to a separate file
      for (int i=1; i<seriesCount; i++) {
        try {
          write(i);
        }
        catch (Exception e) {
          LOGGER.error("Error while writing series {}", i, e);
          return;
        }
      }
    }
    finally {
      readers.forEach((v) -> {
        try {
          v.close();
        }
        catch (IOException e) {
          LOGGER.error("Exception while closing reader", e);
        }
      });
    }
  }

  /**
   * Convert the data specified by the given initialized reader to
   * an intermediate form.
   *
   * @param series the reader series index to be converted
   * @throws FormatException
   * @throws IOException
   * @throws InterruptedException
   */
  public void write(int series)
    throws FormatException, IOException, InterruptedException
  {
    readers.forEach((reader) -> {
      reader.setSeries(series);
    });

    if (series == 0) {
      saveResolutions();
    }
    else {
      String filename = series + ".jpg";
      if (series == 1) {
        filename = "LABELIMAGE.jpg";
      }
      saveExtraImage(filename);
    }
  }

  private byte[] getTileDownsampled(
      int resolution, int plane, int xx, int yy, int width, int height)
          throws FormatException, IOException, InterruptedException
  {
    String pathName = "/" + Integer.toString(resolution - 1);
    N5Reader n5 = new N5FSReader(
        outputPath.resolve("pyramid.n5").toString());
    DatasetAttributes datasetAttributes = n5.getDatasetAttributes(pathName);
    long[] dimensions = datasetAttributes.getDimensions();

    // Upscale our base X and Y offsets, and sizes to the previous resolution
    // based on the pyramid scaling factor
    xx *= PYRAMID_SCALE;
    yy *= PYRAMID_SCALE;
    width = (int) Math.min(tileWidth * PYRAMID_SCALE, dimensions[0] - xx);
    height = (int) Math.min(tileHeight * PYRAMID_SCALE, dimensions[1] - yy);

    long[] startGridPosition = new long[] {
      xx / tileWidth, yy / tileHeight, plane
    };
    int xBlocks = (int) Math.ceil((double) width / tileWidth);
    int yBlocks = (int) Math.ceil((double) height / tileHeight);

    int bytesPerPixel = FormatTools.getBytesPerPixel(pixelType);
    byte[] tile = new byte[width * height * bytesPerPixel];
    for (int xBlock=0; xBlock<xBlocks; xBlock++) {
      for (int yBlock=0; yBlock<yBlocks; yBlock++) {
        int blockWidth = Math.min(
          width - (xBlock * tileWidth), tileWidth);
        int blockHeight = Math.min(
          height - (yBlock * tileHeight),  tileHeight);
        long[] gridPosition = new long[] {
          startGridPosition[0] + xBlock, startGridPosition[1] + yBlock, plane
        };
        ByteBuffer subTile = n5.readBlock(
          pathName, datasetAttributes, gridPosition
        ).toByteBuffer();

        int length = blockWidth * bytesPerPixel;
        for (int y=0; y<blockHeight; y++) {
          int srcPos = y * length;
          int destPos = ((yBlock * width * tileHeight)
            + (y * width) + (xBlock * tileWidth)) * bytesPerPixel;
          subTile.position(srcPos);
          subTile.get(tile, destPos, length);
        }
      }
    }
    return scaler.downsample(tile, width, height,
        PYRAMID_SCALE, bytesPerPixel, isLittleEndian,
        FormatTools.isFloatingPoint(pixelType),
        rgbChannelCount, isInterleaved);
  }

  private byte[] getTile(
      int resolution, int plane, int xx, int yy, int width, int height)
          throws FormatException, IOException, InterruptedException
  {
    if (resolution == 0) {
      IFormatReader reader = readers.take();
      try {
        return reader.openBytes(plane, xx, yy, width, height);
      }
      finally {
        readers.put(reader);
      }
    }
    else {
      Slf4JStopWatch t0 = new Slf4JStopWatch("getTileDownsampled");
      try {
        return getTileDownsampled(resolution, plane, xx, yy, width, height);
      }
      finally {
        t0.stop();
      }
    }
  }

  private void processTile(
      int resolution, int plane, int xx, int yy, int width, int height)
        throws EnumerationException, FormatException, IOException,
          InterruptedException
  {
    String pathName = "/" + Integer.toString(resolution);
    long[] gridPosition = new long[] {
      xx / tileWidth, yy / tileHeight, plane
    };
    int[] size = new int[] {width, height, 1};

    Slf4JStopWatch t0 = new Slf4JStopWatch("getTile");
    DataBlock<?> dataBlock;
    try {
      LOGGER.info("requesting tile to write at {} to {}",
        gridPosition, pathName);
      byte[] tile = getTile(resolution, plane, xx, yy, width, height);
      if (tile == null) {
        return;
      }

      int bytesPerPixel = FormatTools.getBytesPerPixel(pixelType);
      switch (bytesPerPixel) {
        case 1:
          dataBlock = new ByteArrayDataBlock(size, gridPosition, tile);
          break;
        case 2:
          short[] asShort = new short[tile.length / 2];
          ByteBuffer.wrap(tile).asShortBuffer().get(asShort);
          dataBlock = new ShortArrayDataBlock(size, gridPosition, asShort);
          break;
        default:
          throw new FormatException(
              "Unsupported bytes per pixel: " + bytesPerPixel);
      }
    }
    finally {
      nTile.incrementAndGet();
      LOGGER.info("tile read complete {}/{}", nTile.get(), tileCount);
      t0.stop();
    }

    // TODO: ZCT
    // int[] zct = reader.getZCTCoords(plane);
    N5Writer n5 = new N5FSWriter(outputPath.resolve("pyramid.n5").toString());
    Slf4JStopWatch t1 = stopWatch();
    try {
      n5.writeBlock(
        pathName,
        n5.getDatasetAttributes(pathName),
        dataBlock
      );
      LOGGER.info("successfully wrote at {} to {}", gridPosition, pathName);
    }
    finally {
      t1.stop("saveBytes");
    }
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
    IFormatReader workingReader = readers.take();
    try {
      isLittleEndian = workingReader.isLittleEndian();
      resolutions = 1;
      // calculate a reasonable pyramid depth if not specified as an argument
      if (pyramidResolutions == null) {
        int width = workingReader.getSizeX();
        int height = workingReader.getSizeY();
        while (width > MIN_SIZE || height > MIN_SIZE) {
          resolutions++;
          width /= PYRAMID_SCALE;
          height /= PYRAMID_SCALE;
        }
      }
      else {
        resolutions = pyramidResolutions;
      }
      LOGGER.info("Using {} pyramid resolutions", resolutions);
      sizeX = workingReader.getSizeX();
      sizeY = workingReader.getSizeY();
      rgbChannelCount = workingReader.getRGBChannelCount();
      isInterleaved = workingReader.isInterleaved();
      imageCount = workingReader.getImageCount();
      pixelType = workingReader.getPixelType();
    }
    finally {
      readers.put(workingReader);
    }

    LOGGER.info(
      "Preparing to write pyramid sizeX {} (tileWidth: {}) " +
      "sizeY {} (tileWidth: {}) imageCount {}",
        sizeX, tileWidth, sizeY, tileHeight, imageCount
    );

    // Prepare N5 dataset
    DataType dataType;
    switch (pixelType) {
      case FormatTools.INT8:
        dataType = DataType.INT8;
        break;
      case FormatTools.UINT8:
        dataType = DataType.UINT8;
        break;
      case FormatTools.INT16:
        dataType = DataType.INT16;
        break;
      case FormatTools.UINT16:
        dataType = DataType.UINT16;
        break;
      default:
        throw new FormatException("Unsupported pixel type: "
            + FormatTools.getPixelTypeString(pixelType));
    }
    // Use compression scheme that is most compatible with Zarr
    Compression compression = new BloscCompression(
      "lz4",
      5,  // clevel
      BloscCompression.SHUFFLE,  // shuffle
      0,  // blocksize (0 = auto)
      1  // nthreads
    );
    //Compression compression = new RawCompression();
    N5Writer n5 = new N5FSWriter(outputPath.resolve("pyramid.n5").toString());
    for (int resCounter=0; resCounter<resolutions; resCounter++) {
      final int resolution = resCounter;
      int scale = (int) Math.pow(PYRAMID_SCALE, resolution);
      int scaledWidth = sizeX / scale;
      int scaledHeight = sizeY / scale;
      n5.createDataset(
          "/" + Integer.toString(resolution),
          new long[] {scaledWidth, scaledHeight, imageCount},
          new int[] {tileWidth, tileHeight, 1},
          dataType, compression
      );

      nTile = new AtomicInteger(0);
      tileCount = (int) Math.ceil((double) scaledWidth / tileWidth)
          * (int) Math.ceil((double) scaledHeight / tileHeight)
          * imageCount;
      List<CompletableFuture<Void>> futures =
        new ArrayList<CompletableFuture<Void>>();
      for (int j=0; j<scaledHeight; j+=tileHeight) {
        final int yy = j;
        int height = (int) Math.min(tileHeight, scaledHeight - yy);
        for (int k=0; k<scaledWidth; k+=tileWidth) {
          final int xx = k;
          int width = (int) Math.min(tileWidth, scaledWidth - xx);
          for (int i=0; i<imageCount; i++) {
            final int plane = i;

            CompletableFuture<Void> future = new CompletableFuture<Void>();
            futures.add(future);
            executor.execute(() -> {
              try {
                processTile(resolution, plane, xx, yy, width, height);
                LOGGER.info(
                    "Successfully processed tile; resolution={} plane={} " +
                    "xx={} yy={} width={} height={}",
                    resolution, plane, xx, yy, width, height);
                future.complete(null);
              }
              catch (Exception e) {
                future.completeExceptionally(e);
                LOGGER.error(
                  "Failure processing tile; resolution={} plane={} " +
                  "xx={} yy={} width={} height={}",
                  resolution, plane, xx, yy, width, height, e);
              }
            });
          }
        }
      }
      // Wait until the entire resolution has completed before proceeding to
      // the next one
      CompletableFuture.allOf(
        futures.toArray(new CompletableFuture[futures.size()])).join();
    }
  }

  /**
   * Save the current series as a separate image (label/barcode, etc.).
   *
   * @param filename the relative path to the output file
   */
  public void saveExtraImage(String filename)
    throws FormatException, IOException, InterruptedException
  {
    IFormatReader reader = readers.take();
    try (ImageWriter writer = new ImageWriter()) {
      IMetadata metadata = MetadataTools.createOMEXMLMetadata();
      MetadataTools.populateMetadata(metadata, 0, null,
        reader.getCoreMetadataList().get(reader.getCoreIndex()));
      writer.setMetadataRetrieve(metadata);
      writer.setId(outputPath.resolve(filename).toString());
      writer.saveBytes(0, reader.openBytes(0));
    }
    finally {
      readers.put(reader);
    }
  }

  private OMEXMLService getService() throws FormatException {
    try {
      ServiceFactory factory = new ServiceFactory();
      return factory.getInstance(OMEXMLService.class);
    }
    catch (DependencyException de) {
      throw new MissingLibraryException(OMEXMLServiceImpl.NO_OME_XML_MSG, de);
    }
  }

  /**
   * @return an empty IMetadata object for metadata transport.
   * @throws FormatException
   */
  private IMetadata createMetadata() throws FormatException {
    try {
      return getService().createOMEXMLMetadata();
    }
    catch (ServiceException se) {
      throw new FormatException(se);
    }
  }

  private Slf4JStopWatch stopWatch() {
    return new Slf4JStopWatch(LOGGER, Slf4JStopWatch.DEBUG_LEVEL);
  }

  /**
   * Perform file conversion as specified by command line arguments.
   * @param args command line arguments
   */
  public static void main(String[] args) {
    CommandLine.call(new Converter(), args);
  }

}
