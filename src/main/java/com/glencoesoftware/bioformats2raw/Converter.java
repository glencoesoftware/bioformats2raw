/**
 * Copyright (c) 2019 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.bioformats2raw;

import java.io.File;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import loci.common.Constants;
import loci.common.image.IImageScaler;
import loci.common.image.SimpleImageScaler;
import loci.common.services.DependencyException;
import loci.common.services.ServiceException;
import loci.common.services.ServiceFactory;
import loci.formats.ChannelSeparator;
import loci.formats.ClassList;
import loci.formats.FormatException;
import loci.formats.FormatTools;
import loci.formats.IFormatReader;
import loci.formats.ImageReader;
import loci.formats.ImageWriter;
import loci.formats.Memoizer;
import loci.formats.MetadataTools;
import loci.formats.MissingLibraryException;
import loci.formats.in.DynamicMetadataOptions;
import loci.formats.meta.IMetadata;
import loci.formats.ome.OMEXMLMetadata;
import loci.formats.services.OMEXMLService;
import loci.formats.services.OMEXMLServiceImpl;
import ome.xml.model.enums.DimensionOrder;
import ome.xml.model.enums.EnumerationException;

import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.DoubleArrayDataBlock;
import org.janelia.saalfeldlab.n5.FloatArrayDataBlock;
import org.janelia.saalfeldlab.n5.IntArrayDataBlock;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.ShortArrayDataBlock;
import org.perf4j.slf4j.Slf4JStopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.glencoesoftware.bioformats2raw.MiraxReader.TilePointer;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import ch.qos.logback.classic.Level;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Command line tool for converting whole slide imaging files to N5.
 */
public class Converter implements Callable<Void> {

  private static final Logger LOGGER = LoggerFactory.getLogger(Converter.class);

  /**
   * Relative path to OME-XML metadata file.
   */
  private static final String METADATA_FILE = "METADATA.ome.xml";

  /**
   * Minimum size of the largest XY dimension in the smallest resolution,
   * when calculating the number of resolutions to generate.
   */
  private static final int MIN_SIZE = 256;

  /** Scaling factor in X and Y between any two consecutive resolutions. */
  private static final int PYRAMID_SCALE = 2;

  /** Version of the bioformats2raw layout. */
  public static final Integer LAYOUT = 1;

  @Parameters(
    index = "0",
    arity = "1",
    description = "file to convert"
  )
  private volatile Path inputPath;

  @Parameters(
    index = "1",
    arity = "1",
    description = "path to the output pyramid directory"
  )
  private volatile Path outputPath;

  @Option(
    names = {"-r", "--resolutions"},
    description = "Number of pyramid resolutions to generate"
  )
  private volatile Integer pyramidResolutions;

  @Option(
    names = {"-w", "--tile_width"},
    description = "Maximum tile width to read (default: ${DEFAULT-VALUE})"
  )
  private volatile int tileWidth = 1024;

  @Option(
    names = {"-h", "--tile_height"},
    description = "Maximum tile height to read (default: ${DEFAULT-VALUE})"
  )
  private volatile int tileHeight = 1024;

  @Option(
    names = "--debug",
    description = "Turn on debug logging"
  )
  private volatile boolean debug = false;

  @Option(
    names = "--version",
    description = "Print version information and exit",
    help = true
  )
  private volatile boolean printVersion = false;

  @Option(
    names = "--max_workers",
    description = "Maximum number of workers (default: ${DEFAULT-VALUE})"
  )
  // cap the default worker count at 4, to prevent problems with
  // large images that are not tiled
  private volatile int maxWorkers =
      (int) Math.min(4, Runtime.getRuntime().availableProcessors());

  @Option(
    names = "--max_cached_tiles",
    description =
      "Maximum number of tiles that will be cached across all "
      + "workers (default: ${DEFAULT-VALUE})"
  )
  private volatile int maxCachedTiles = 64;

  @Option(
          names = {"-c", "--compression"},
          description = "Compression type for n5 " +
                  "(${COMPLETION-CANDIDATES}; default: ${DEFAULT-VALUE})"
  )
  private volatile N5Compression.CompressionTypes compressionType =
          N5Compression.CompressionTypes.blosc;

  @Option(
          names = {"--compression-parameter"},
          description = "Integer parameter for chosen compression (see " +
                  "https://github.com/saalfeldlab/n5/blob/master/README.md" +
                  " )"
  )
  private volatile Integer compressionParameter = null;

  @Option(
          names = "--extra-readers",
          arity = "0..1",
          split = ",",
          description = "Separate set of readers to include; " +
                  "(default: ${DEFAULT-VALUE})"
  )
  private volatile Class<?>[] extraReaders = new Class[] {
    PyramidTiffReader.class, MiraxReader.class
  };

  @Option(
          names = "--file_type",
          description = "Tile file extension: ${COMPLETION-CANDIDATES} " +
                  "(default: ${DEFAULT-VALUE}) " +
                  "[Can break compatibility with raw2ometiff]"
  )
  private volatile FileType fileType = FileType.n5;

  @Option(
          names = "--pyramid-name",
          description = "Name of pyramid (default: ${DEFAULT-VALUE}) " +
                  "[Can break compatibility with raw2ometiff]"
  )
  private volatile String pyramidName = "data.n5";

  @Option(
          names = "--scale-format-string",
          description = "Format string for scale paths; the first two " +
                  "arguments will always be series and resolution followed " +
                  "by any additional arguments brought in from " +
                  "`--additional-scale-format-string-args` " +
                  "[Can break compatibility with raw2ometiff] " +
                  "(default: ${DEFAULT-VALUE})"
  )
  private volatile String scaleFormatString = "%d/%d";

  @Option(
          names = "--additional-scale-format-string-args",
          description = "Additional format string argument CSV file (without " +
                  "header row).  Arguments will be added to the end of the " +
                  "scale format string mapping the at the corresponding CSV " +
                  "row index.  It is expected that the CSV file contain " +
                  "exactly the same number of rows as the input file has " +
                  "series"
  )
  private volatile Path additionalScaleFormatStringArgsCsv;

  /** Additional scale format string arguments after parsing. */
  private volatile List<String[]> additionalScaleFormatStringArgs;

  @Option(
          names = "--dimension-order",
          description = "Override the input file dimension order in the " +
                  "output file [Can break compatibility with raw2ometiff] " +
                  "(${COMPLETION-CANDIDATES})"
  )
  private volatile DimensionOrder dimensionOrder;

  @Option(
          names = "--memo-directory",
          description = "Directory used to store .bfmemo cache files"
  )
  private volatile File memoDirectory;

  @Option(
          names = "--downsample-type",
          description = "Tile downsampling algorithm (${COMPLETION-CANDIDATES})"
  )
  private volatile Downsampling downsampling = Downsampling.SIMPLE;

  @Option(
          names = "--overwrite",
          description = "Overwrite the output directory if it exists"
  )
  private volatile boolean overwrite = false;

  @Option(
          names = "--fill-value",
          description = "Default value to fill in for missing tiles (0-255)" +
                        " (currently .mrxs only)"
  )
  private volatile Short fillValue = null;

  @Option(
          arity = "0..1",
          names = "--options",
          split = ",",
          description =
            "Reader-specific options, in format key=value[,key2=value2]"
  )
  private volatile List<String> readerOptions = new ArrayList<String>();

  @Option(
          names = "--no-hcs",
          description = "Turn off HCS writing"
  )
  private volatile boolean noHCS = false;

  /** Scaling implementation that will be used during downsampling. */
  private volatile IImageScaler scaler = new SimpleImageScaler();

  /**
   * Set of readers that can be used concurrently, size will be equal to
   * {@link #maxWorkers}.
   */
  private volatile BlockingQueue<IFormatReader> readers;

  /**
   * Bounded task queue limiting the number of in flight conversion operations
   * happening in parallel.  Size will be equal to {@link #maxWorkers}.
   */
  private volatile BlockingQueue<Runnable> queue;

  private volatile ExecutorService executor;

  /** Whether or not the source file is little endian. */
  private boolean isLittleEndian;

  /**
   * The source file's pixel type.  Retrieved from
   * {@link IFormatReader#getPixelType()}.
   */
  private volatile int pixelType;

  /** Total number of tiles at the current resolution during processing. */
  private volatile int tileCount;

  /** Current number of tiles processed at the current resolution. */
  private volatile AtomicInteger nTile;

  private List<HCSIndex> hcsIndexes = new ArrayList<HCSIndex>();

  @Override
  public Void call() throws Exception {
    if (printVersion) {
      String version = Optional.ofNullable(
        this.getClass().getPackage().getImplementationVersion()
        ).orElse("development");
      System.out.println("Version = " + version);
      System.out.println("Bio-Formats version = " + FormatTools.VERSION);
      return null;
    }

    if (fillValue != null && (fillValue < 0 || fillValue > 255)) {
      throw new IllegalArgumentException("Invalid fill value: " + fillValue);
    }

    OpenCVTools.loadOpenCV();

    ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger)
        LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    if (debug) {
      root.setLevel(Level.DEBUG);
    }
    else {
      root.setLevel(Level.INFO);
    }

    if (Files.exists(outputPath)) {
      if (!overwrite) {
        throw new IllegalArgumentException(
          "Output path " + outputPath + " already exists");
      }
      LOGGER.warn("Overwriting output path {}", outputPath);
      Files.walk(outputPath)
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
    }

    readers = new ArrayBlockingQueue<IFormatReader>(maxWorkers);
    queue = new LimitedQueue<Runnable>(maxWorkers);
    executor = new ThreadPoolExecutor(
      maxWorkers, maxWorkers, 0L, TimeUnit.MILLISECONDS, queue);
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
   * @throws EnumerationException
   */
  public void convert()
      throws FormatException, IOException, InterruptedException,
             EnumerationException
  {
    checkOutputPaths();

    Cache<TilePointer, byte[]> tileCache = CacheBuilder.newBuilder()
        .maximumSize(maxCachedTiles)
        .build();

    // First find which reader class we need
    Class<?> readerClass = getBaseReaderClass();

    if (!readerClass.equals(MiraxReader.class) && fillValue != null) {
      throw new IllegalArgumentException(
        "--fill-value not yet supported for " + readerClass);
    }

    // Now with our found type instantiate our queue of readers for use
    // during conversion
    for (int i=0; i < maxWorkers; i++) {
      IFormatReader reader;
      Memoizer memoizer;
      try {
        reader = (IFormatReader) readerClass.getConstructor().newInstance();
        if (fillValue != null && reader instanceof MiraxReader) {
          ((MiraxReader) reader).setFillValue(fillValue.byteValue());
        }
        if (memoDirectory == null) {
          memoizer = new Memoizer(reader);
        }
        else {
          memoizer = new Memoizer(
            reader, Memoizer.DEFAULT_MINIMUM_ELAPSED, memoDirectory);
        }
      }
      catch (Exception e) {
        LOGGER.error("Failed to instantiate reader: {}", readerClass, e);
        return;
      }

      if (readerOptions.size() > 0) {
        DynamicMetadataOptions options = new DynamicMetadataOptions();
        for (String option : readerOptions) {
          String[] pair = option.split("=");
          if (pair.length == 2) {
            options.set(pair[0], pair[1]);
          }
        }
        memoizer.setMetadataOptions(options);
      }

      memoizer.setOriginalMetadataPopulated(true);
      memoizer.setFlattenedResolutions(false);
      memoizer.setMetadataFiltered(true);
      memoizer.setMetadataStore(createMetadata());
      memoizer.setId(inputPath.toString());
      memoizer.setResolution(0);
      if (reader instanceof MiraxReader) {
        ((MiraxReader) reader).setTileCache(tileCache);
      }
      readers.add(new ChannelSeparator(memoizer));
    }

    // Finally, perform conversion on all series
    try {
      int seriesCount;
      IFormatReader v = readers.take();
      IMetadata meta = null;
      try {
        seriesCount = v.getSeriesCount();
        meta = (IMetadata) v.getMetadataStore();
        ((OMEXMLMetadata) meta).resolveReferences();

        if (!noHCS) {
          noHCS = meta.getPlateCount() == 0;
        }

        for (int s=0; s<meta.getImageCount(); s++) {
          meta.setPixelsBigEndian(true, s);

          if (!noHCS) {
            HCSIndex index = new HCSIndex(meta, s);
            hcsIndexes.add(index);
          }
        }

        String xml = getService().getOMEXML(meta);

        // write the original OME-XML to a file
        if (!Files.exists(outputPath)) {
          Files.createDirectories(outputPath);
        }
        Path omexmlFile = outputPath.resolve(METADATA_FILE);
        Files.write(omexmlFile, xml.getBytes(Constants.ENCODING));
      }
      catch (ServiceException se) {
        LOGGER.error("Could not retrieve OME-XML", se);
        return;
      }
      finally {
        readers.put(v);
      }

      if (!noHCS) {
        scaleFormatString = "%d/%d/%d/%d/%d/%d";
      }

      for (int i=0; i<seriesCount; i++) {
        try {
          write(i);
        }
        catch (Throwable t) {
          LOGGER.error("Error while writing series {}", i, t);
          unwrapException(t);
          return;
        }
      }

      if (meta != null) {
        saveHCSMetadata(getWriter(), meta);
      }
    }
    finally {
      // Shut down first, tasks may still be running
      executor.shutdown();
      executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
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
   * @throws EnumerationException
   */
  public void write(int series)
    throws FormatException, IOException, InterruptedException,
           EnumerationException
  {
    readers.forEach((reader) -> {
      reader.setSeries(series);
    });
    saveResolutions(series);
  }

  /**
   * Retrieves the scaled format string arguments, adding additional ones
   * from a provided CSV file.
   * @param series current series to be prepended to the argument list
   * @param resolution current resolution to be prepended to the argument list
   * @return Array of arguments to be used in format string application.  Order
   * will be <code>[series, resolution, &lt;additionalArgs&gt;...]</code> where
   * <code>additionalArgs</code> is sourced from an optionally provided CSV
   * file.
   */
  private Object[] getScaleFormatStringArgs(
      Integer series, Integer resolution)
  {
    List<Object> args = new ArrayList<Object>();
    if (!noHCS) {
      HCSIndex index = hcsIndexes.get(series);
      args.add(index.getPlateIndex());
      args.add(index.getPlateAcquisitionIndex());
      args.add(index.getWellRowIndex());
      args.add(index.getWellColumnIndex());
      args.add(index.getFieldIndex());
      args.add(resolution);
    }
    else {
      args.add(series);
      args.add(resolution);
      if (additionalScaleFormatStringArgs != null) {
        String[] row = additionalScaleFormatStringArgs.get(series);
        for (String arg : row) {
          args.add(arg);
        }
      }
    }
    return args.toArray();
  }

  private byte[] getTileDownsampled(
      int series, int resolution, int plane, int xx, int yy,
      int width, int height)
          throws FormatException, IOException, InterruptedException,
                 EnumerationException
  {
    final String pathName = "/" +
        String.format(scaleFormatString,
            getScaleFormatStringArgs(series, resolution - 1));
    final String pyramidPath = outputPath.resolve(pyramidName).toString();
    final N5Reader n5 = fileType.reader(pyramidPath);

    DatasetAttributes datasetAttributes = n5.getDatasetAttributes(pathName);
    long[] dimensions = datasetAttributes.getDimensions();
    int[] blockSizes = datasetAttributes.getBlockSize();
    int activeTileWidth = blockSizes[0];
    int activeTileHeight = blockSizes[1];

    // Upscale our base X and Y offsets, and sizes to the previous resolution
    // based on the pyramid scaling factor
    xx *= PYRAMID_SCALE;
    yy *= PYRAMID_SCALE;
    width = (int) Math.min(
        activeTileWidth * PYRAMID_SCALE, dimensions[0] - xx);
    height = (int) Math.min(
        activeTileHeight * PYRAMID_SCALE, dimensions[1] - yy);

    IFormatReader reader = readers.take();
    long[] startGridPosition;
    try {
      startGridPosition = getGridPosition(
        reader, xx / activeTileWidth, yy / activeTileHeight, plane);
    }
    finally {
      readers.put(reader);
    }
    int xBlocks = (int) Math.ceil((double) width / activeTileWidth);
    int yBlocks = (int) Math.ceil((double) height / activeTileHeight);

    int bytesPerPixel = FormatTools.getBytesPerPixel(pixelType);
    byte[] tile = new byte[width * height * bytesPerPixel];
    for (int xBlock=0; xBlock<xBlocks; xBlock++) {
      for (int yBlock=0; yBlock<yBlocks; yBlock++) {
        int blockWidth = Math.min(
          width - (xBlock * activeTileWidth), activeTileWidth);
        int blockHeight = Math.min(
          height - (yBlock * activeTileHeight), activeTileHeight);
        long[] gridPosition = new long[] {
          startGridPosition[0] + xBlock, startGridPosition[1] + yBlock,
          startGridPosition[2], startGridPosition[3], startGridPosition[4]
        };
        ByteBuffer subTile = n5.readBlock(
          pathName, datasetAttributes, gridPosition
        ).toByteBuffer();

        int destLength = blockWidth * bytesPerPixel;
        int srcLength = destLength;
        if (fileType == FileType.zarr) {
          // n5/n5-zarr does not de-pad on read
          srcLength = activeTileWidth * bytesPerPixel;
        }
        for (int y=0; y<blockHeight; y++) {
          int srcPos = y * srcLength;
          int destPos = ((yBlock * width * activeTileHeight)
            + (y * width) + (xBlock * activeTileWidth)) * bytesPerPixel;
          // Cast to Buffer to avoid issues if compilation is performed
          // on JDK9+ and execution is performed on JDK8.  This is due
          // to the existence of covariant return types in the resultant
          // byte code if compiled on JDK0+.  For reference:
          //   https://issues.apache.org/jira/browse/MRESOLVER-85
          ((Buffer) subTile).position(srcPos);
          subTile.get(tile, destPos, destLength);
        }
      }
    }

    if (downsampling == Downsampling.SIMPLE) {
      return scaler.downsample(tile, width, height,
        PYRAMID_SCALE, bytesPerPixel, false,
        FormatTools.isFloatingPoint(pixelType),
        1, false);
    }

    return OpenCVTools.downsample(
      tile, pixelType, width, height, PYRAMID_SCALE, downsampling);
  }

  private byte[] getTile(
      int series, int resolution, int plane, int xx, int yy,
      int width, int height)
          throws FormatException, IOException, InterruptedException,
                 EnumerationException
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
        return getTileDownsampled(
            series, resolution, plane, xx, yy, width, height);
      }
      finally {
        t0.stop();
      }
    }
  }

  /**
   * Retrieve the dimensions based on either the configured or input file
   * dimension order at the current resolution.
   * @param reader initialized reader for the input file
   * @param scaledWidth size of the X dimension at the current resolution
   * @param scaledHeight size of the Y dimension at the current resolution
   * @return dimension array ready for use with N5
   * @throws EnumerationException
   */
  private long[] getDimensions(
    IFormatReader reader, int scaledWidth, int scaledHeight)
      throws EnumerationException
  {
    int sizeZ = reader.getSizeZ();
    int sizeC = reader.getSizeC();
    int sizeT = reader.getSizeT();
    String o = dimensionOrder != null? dimensionOrder.toString()
        : reader.getDimensionOrder();
    long[] dimensions = new long[] {scaledWidth, scaledHeight, 0, 0, 0};
    dimensions[o.indexOf("Z")] = sizeZ;
    dimensions[o.indexOf("C")] = sizeC;
    dimensions[o.indexOf("T")] = sizeT;
    return dimensions;
  }

  /**
   * Retrieve the grid position based on either the configured or input file
   * dimension order at the current resolution.
   * @param reader initialized reader for the input file
   * @param x X position at the current resolution
   * @param y Y position at the current resolution
   * @param plane current plane being operated upon
   * @return grid position array ready for use with N5
   * @throws EnumerationException
   */
  private long[] getGridPosition(
    IFormatReader reader, int x, int y, int plane) throws EnumerationException
  {
    String o = dimensionOrder != null? dimensionOrder.toString()
        : reader.getDimensionOrder();
    int[] zct = reader.getZCTCoords(plane);
    long[] gridPosition = new long[] {x, y, 0, 0, 0};
    gridPosition[o.indexOf("Z")] = zct[0];
    gridPosition[o.indexOf("C")] = zct[1];
    gridPosition[o.indexOf("T")] = zct[2];
    return gridPosition;
  }

  private void processTile(
      int series, int resolution, int plane, int xx, int yy,
      int width, int height)
        throws EnumerationException, FormatException, IOException,
          InterruptedException
  {
    String pathName =
        "/" + String.format(scaleFormatString,
            getScaleFormatStringArgs(series, resolution));
    final N5Writer n5 = getWriter();
    DatasetAttributes datasetAttributes = n5.getDatasetAttributes(pathName);
    int[] blockSizes = datasetAttributes.getBlockSize();
    int activeTileWidth = blockSizes[0];
    int activeTileHeight = blockSizes[1];
    IFormatReader reader = readers.take();
    long[] gridPosition;
    try {
      gridPosition = getGridPosition(
          reader, xx / activeTileWidth, yy / activeTileHeight, plane);
    }
    finally {
      readers.put(reader);
    }
    int[] size = new int[] {width, height, 1, 1, 1};

    Slf4JStopWatch t0 = new Slf4JStopWatch("getTile");
    DataBlock<?> dataBlock;
    try {
      LOGGER.info("requesting tile to write at {} to {}",
        gridPosition, pathName);
      byte[] tile = getTile(series, resolution, plane, xx, yy, width, height);
      if (tile == null) {
        return;
      }

      dataBlock = makeDataBlock(resolution, size, gridPosition, tile);
    }
    finally {
      nTile.incrementAndGet();
      LOGGER.info("tile read complete {}/{}", nTile.get(), tileCount);
      t0.stop();
    }

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
   * @param series the reader series index to be converted
   * @throws FormatException
   * @throws IOException
   * @throws InterruptedException
   * @throws EnumerationException
   */
  public void saveResolutions(int series)
    throws FormatException, IOException, InterruptedException,
           EnumerationException
  {
    IFormatReader workingReader = readers.take();
    int resolutions = 1;
    int sizeX;
    int sizeY;
    int imageCount;
    try {
      isLittleEndian = workingReader.isLittleEndian();
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
    DataType dataType = getN5Type(pixelType);
    Compression compression = N5Compression.getCompressor(compressionType,
            compressionParameter);

    // fileset level metadata
    final N5Writer n5 = getWriter();
    n5.setAttribute("/", "bioformats2raw.layout", LAYOUT);

    // series level metadata
    setSeriesLevelMetadata(n5, series, resolutions);

    for (int resCounter=0; resCounter<resolutions; resCounter++) {
      final int resolution = resCounter;
      int scale = (int) Math.pow(PYRAMID_SCALE, resolution);
      int scaledWidth = sizeX / scale;
      int scaledHeight = sizeY / scale;

      int activeTileWidth = tileWidth;
      int activeTileHeight = tileHeight;
      if (scaledWidth < activeTileWidth) {
        LOGGER.warn("Reducing active tileWidth to {}", scaledWidth);
        activeTileWidth = scaledWidth;
      }

      if (scaledHeight < activeTileHeight) {
        LOGGER.warn("Reducing active tileHeight to {}", scaledHeight);
        activeTileHeight = scaledHeight;
      }

      String resolutionString = "/" +  String.format(
              scaleFormatString, getScaleFormatStringArgs(series, resolution));
      n5.createDataset(
          resolutionString,
          getDimensions(workingReader, scaledWidth, scaledHeight),
          new int[] {activeTileWidth, activeTileHeight, 1, 1, 1},
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
                processTile(series, resolution, plane, xx, yy, width, height);
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

      // TODO: some of these futures may be completelyExceptionally
      //  and need re-throwing

    }

  }

  private void saveHCSMetadata(N5Writer n5, IMetadata meta) throws IOException {
    if (noHCS) {
      return;
    }

    for (int plate=0; plate<meta.getPlateCount(); plate++) {
      Map<String, Object> plateMap = new HashMap<String, Object>();

      plateMap.put("name", meta.getPlateName(plate));

      List<Map<String, Object>> columns = new ArrayList<Map<String, Object>>();
      for (int c=0; c<meta.getPlateColumns(plate).getValue(); c++) {
        Map<String, Object> column = new HashMap<String, Object>();
        column.put("name", c);
        columns.add(column);
      }
      plateMap.put("columns", columns);

      List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
      for (int r=0; r<meta.getPlateRows(plate).getValue(); r++) {
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("name", r);
        rows.add(row);
      }
      plateMap.put("rows", rows);

      List<Map<String, Object>> acquisitions =
        new ArrayList<Map<String, Object>>();
      for (int pa=0; pa<meta.getPlateAcquisitionCount(plate); pa++) {
        Map<String, Object> acquisition = new HashMap<String, Object>();
        acquisition.put("path", pa);
        acquisitions.add(acquisition);
      }
      plateMap.put("acquisitions", acquisitions);

      String platePath = "/" + plate;
      List<Map<String, Object>> wells = new ArrayList<Map<String, Object>>();
      int maxField = Integer.MIN_VALUE;
      for (HCSIndex index : hcsIndexes) {
        if (index.getPlateIndex() == plate) {
          String wellPath = index.getWellPath();

          Map<String, Object> well = new HashMap<String, Object>();
          well.put("path", wellPath);
          wells.add(well);

          List<Map<String, Object>> imageList =
            new ArrayList<Map<String, Object>>();
          String fullPath = platePath + "/" + wellPath;
          for (String field : n5.list(fullPath)) {
            Map<String, Object> image = new HashMap<String, Object>();
            image.put("path", field);
            imageList.add(image);
          }

          Map<String, Object> wellMap = new HashMap<String, Object>();
          wellMap.put("images", imageList);
          n5.setAttribute(fullPath, "well", wellMap);

          maxField = (int) Math.max(maxField, index.getFieldIndex());
        }
      }
      plateMap.put("wells", wells);


      plateMap.put("field_count", maxField);

      n5.setAttribute(platePath, "plate", plateMap);
    }
  }

  /**
   * Use {@link N5Writer#setAttribute(String, String, Object)}
   * to attach the multiscales metadata to the group containing
   * the pyramids.
   *
   * @param n5 Active {@link N5Writer}.
   * @param series Series which is currently being written.
   * @param resolutions Total number of resolutions from which
   *                    names will be generated.
   * @throws IOException
   */
  private void setSeriesLevelMetadata(N5Writer n5, int series, int resolutions)
          throws IOException
  {
    String resolutionString = "/" +  String.format(
            scaleFormatString, getScaleFormatStringArgs(series, 0));
    String seriesString = resolutionString.substring(0,
            resolutionString.lastIndexOf('/'));
    List<Map<String, Object>> multiscales =
            new ArrayList<Map<String, Object>>();
    Map<String, Object> multiscale = new HashMap<String, Object>();
    Map<String, String> metadata = new HashMap<String, String>();

    if (downsampling == Downsampling.SIMPLE) {
      metadata.put("method", "loci.common.image.SimpleImageScaler");
      metadata.put("version", "Bio-Formats " + FormatTools.VERSION);
    }
    else {
      String method =
        downsampling == Downsampling.GAUSSIAN ? "pyrDown" : "resize";
      metadata.put("method", "org.opencv.imgproc.Imgproc." + method);
      metadata.put("version", OpenCVTools.getVersion());
      multiscale.put("type", downsampling.getName());
    }
    multiscale.put("metadata", metadata);
    multiscale.put("version", "0.1");
    multiscales.add(multiscale);
    List<Map<String, String>> datasets = new ArrayList<Map<String, String>>();
    for (int r = 0; r < resolutions; r++) {
      resolutionString = "/" +  String.format(
              scaleFormatString, getScaleFormatStringArgs(series, r));
      String lastPath = resolutionString.substring(
              resolutionString.lastIndexOf('/') + 1);
      datasets.add(Collections.singletonMap("path", lastPath));
    }
    multiscale.put("datasets", datasets);
    n5.createGroup(seriesString);
    n5.setAttribute(seriesString, "multiscales", multiscales);
  }

  /**
   * Takes exception from asynchronous execution and re-throw known exception
   * types. If the end is reached with no known exception detected, either the
   * exception itself will be thrown if {@link RuntimeException}, otherwise
   * wrap in a {@link RuntimeException}.
   *
   * @param t Exception raised during processing.
   */
  private void unwrapException(Throwable t)
          throws FormatException, IOException, InterruptedException
  {
    if (t instanceof CompletionException) {
      try {
        throw ((CompletionException) t).getCause();
      }
      catch (FormatException | IOException | InterruptedException e2) {
        throw e2;
      }
      catch (RuntimeException rt) {
        throw rt;
      }
      catch (Throwable t2) {
        throw new RuntimeException(t);
      }
    }
    else if (t instanceof RuntimeException) {
      throw (RuntimeException) t;
    }
    else {
      throw new RuntimeException(t);
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
      IMetadata metadata = createMetadata();
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

  /**
   * Convert Bio-Formats pixel type to N5 data type.
   *
   * @param type Bio-Formats pixel type
   * @return corresponding N5 data type
   */
  private DataType getN5Type(int type) {
    switch (type) {
      case FormatTools.INT8:
        return DataType.INT8;
      case FormatTools.UINT8:
        return DataType.UINT8;
      case FormatTools.INT16:
        return DataType.INT16;
      case FormatTools.UINT16:
        return DataType.UINT16;
      case FormatTools.INT32:
        return DataType.INT32;
      case FormatTools.UINT32:
        return DataType.UINT32;
      case FormatTools.FLOAT:
        return DataType.FLOAT32;
      case FormatTools.DOUBLE:
        return DataType.FLOAT64;
      default:
        throw new IllegalArgumentException("Unsupported pixel type: "
            + FormatTools.getPixelTypeString(type));
    }
  }

  private DataBlock<?> makeDataBlock(
    int resolution, int[] size, long[] gridPosition, byte[] tile)
    throws FormatException
  {
    ByteBuffer bb = ByteBuffer.wrap(tile);
    if (resolution == 0 && isLittleEndian) {
      bb = bb.order(ByteOrder.LITTLE_ENDIAN);
    }
    switch (pixelType) {
      case FormatTools.INT8:
      case FormatTools.UINT8: {
        return new ByteArrayDataBlock(size, gridPosition, tile);
      }
      case FormatTools.INT16:
      case FormatTools.UINT16: {
        short[] asShort = new short[tile.length / 2];
        bb.asShortBuffer().get(asShort);
        return new ShortArrayDataBlock(size, gridPosition, asShort);
      }
      case FormatTools.INT32:
      case FormatTools.UINT32: {
        int[] asInt = new int[tile.length / 4];
        bb.asIntBuffer().get(asInt);
        return new IntArrayDataBlock(size, gridPosition, asInt);
      }
      case FormatTools.FLOAT: {
        float[] asFloat = new float[tile.length / 4];
        bb.asFloatBuffer().get(asFloat);
        return new FloatArrayDataBlock(size, gridPosition, asFloat);
      }
      case FormatTools.DOUBLE: {
        double[] asDouble = new double[tile.length / 8];
        bb.asDoubleBuffer().get(asDouble);
        return new DoubleArrayDataBlock(size, gridPosition, asDouble);
      }
      default:
        throw new FormatException("Unsupported pixel type: "
            + FormatTools.getPixelTypeString(pixelType));
    }
  }

  private void checkOutputPaths() {
    if (fileType.equals(FileType.zarr) && pyramidName.equals("data.n5")) {
      pyramidName = "data.zarr";
    }

    if (!pyramidName.equals("data.n5") ||
              !scaleFormatString.equals("%d/%d"))
    {
      LOGGER.info("Output will be incompatible with raw2ometiff " +
              "(pyramidName: {}, scaleFormatString: {})",
              pyramidName, scaleFormatString);
    }

    if (additionalScaleFormatStringArgsCsv != null) {
      CsvParserSettings parserSettings = new CsvParserSettings();
      parserSettings.detectFormatAutomatically();
      parserSettings.setLineSeparatorDetectionEnabled(true);

      CsvParser parser = new CsvParser(parserSettings);
      additionalScaleFormatStringArgs =
          parser.parseAll(additionalScaleFormatStringArgsCsv.toFile());
    }
  }

  private Class<?> getBaseReaderClass() throws FormatException, IOException {
    ClassList<IFormatReader> readerClasses =
        ImageReader.getDefaultReaderClasses();

    for (Class<?> reader : extraReaders) {
      readerClasses.addClass(0, (Class<IFormatReader>) reader);
      LOGGER.debug("Added extra reader: {}", reader);
    }

    ImageReader imageReader = new ImageReader(readerClasses);
    try {
      imageReader.setId(inputPath.toString());
      return imageReader.getReader().getClass();
    }
    finally {
      imageReader.close();
    }
  }

  private N5Writer getWriter() throws IOException {
    final String pyramidPath = outputPath.resolve(pyramidName).toString();
    final N5Writer n5 = fileType.writer(pyramidPath);
    return n5;
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
