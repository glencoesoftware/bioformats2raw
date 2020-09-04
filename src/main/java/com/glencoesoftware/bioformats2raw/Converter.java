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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
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
import loci.formats.meta.IMetadata;
import loci.formats.services.OMEXMLService;
import loci.formats.services.OMEXMLServiceImpl;
import ome.xml.model.enums.DimensionOrder;
import ome.xml.model.enums.EnumerationException;

import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.Bzip2Compression;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.CompressionAdapter;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.DoubleArrayDataBlock;
import org.janelia.saalfeldlab.n5.FloatArrayDataBlock;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.Lz4Compression;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.zarr.N5ZarrReader;
import org.janelia.saalfeldlab.n5.zarr.N5ZarrWriter;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.ShortArrayDataBlock;
import org.janelia.saalfeldlab.n5.XzCompression;
import org.janelia.saalfeldlab.n5.blosc.BloscCompression;
import org.perf4j.slf4j.Slf4JStopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.glencoesoftware.bioformats2raw.MiraxReader.TilePointer;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import ch.qos.logback.classic.Level;
import io.tiledb.java.api.Array;
import io.tiledb.java.api.ArraySchema;
import io.tiledb.java.api.ArrayType;
import io.tiledb.java.api.Attribute;
import io.tiledb.java.api.Bzip2Filter;
import io.tiledb.java.api.CompressionFilter;
import io.tiledb.java.api.Context;
import io.tiledb.java.api.Datatype;
import io.tiledb.java.api.Dimension;
import io.tiledb.java.api.Domain;
import io.tiledb.java.api.DoubleDeltaFilter;
import io.tiledb.java.api.FilterList;
import io.tiledb.java.api.GzipFilter;
import io.tiledb.java.api.Layout;
import io.tiledb.java.api.LZ4Filter;
import io.tiledb.java.api.NativeArray;
import io.tiledb.java.api.Pair;
import io.tiledb.java.api.Query;
import io.tiledb.java.api.QueryType;
import io.tiledb.java.api.RleFilter;
import io.tiledb.java.api.TileDBError;
import io.tiledb.java.api.ZstdFilter;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Command line tool for converting whole slide imaging files to N5.
 */
public class Converter implements Callable<Void> {

  private static final Logger LOGGER = LoggerFactory.getLogger(Converter.class);

  /**
   * Minimum size of the largest XY dimension in the smallest resolution,
   * when calculating the number of resolutions to generate.
   */
  private static final int MIN_SIZE = 256;

  /** Scaling factor in X and Y between any two consecutive resolutions. */
  private static final int PYRAMID_SCALE = 2;

  /** Version of the bioformats2raw layout. */
  public static final Integer LAYOUT = 1;

  /**
   * Enumeration that backs the --file_type flag.
   */
  enum FileType { n5, zarr, tiledb };

  enum CompressionTypes {
    blosc, bzip2, delta, gzip, lz4, raw, rle, xz, zstd
  }

  class TileDBAttributes extends DatasetAttributes {
    private CompressionFilter compression;

    public TileDBAttributes(long[] dims, int[] blockSize,
      DataType type, Compression codec)
    {
      super(dims, blockSize, type, codec);
    }

    public CompressionFilter getCompressionFilter() {
      return compression;
    }

    public void setCompressionFilter(CompressionFilter c) {
      compression = c;
    }
  }

  static class TileDBCompression {

    private static CompressionFilter getCompressor(
            CompressionTypes type, Context ctx) throws TileDBError
    {
      switch (type) {
        case bzip2:
          return new Bzip2Filter(ctx);
        case delta:
          return new DoubleDeltaFilter(ctx);
        case gzip:
          return new GzipFilter(ctx);
        case lz4:
          return new LZ4Filter(ctx);
        case rle:
          return new RleFilter(ctx);
        case zstd:
          return new ZstdFilter(ctx);
        default:
          return null;
      }
    }
  }

  static class N5Compression {
    private static Compression getCompressor(
            CompressionTypes type,
            Integer compressionParameter)
    {
      switch (type) {
        case blosc:
          return new BloscCompression(
                  "lz4",
                  5, // clevel
                  BloscCompression.SHUFFLE,  // shuffle
                  0, // blocksize (0 = auto)
                  1  // nthreads
          );
        case gzip:
          if (compressionParameter == null) {
            return new GzipCompression();
          }
          else {
            return new GzipCompression(compressionParameter.intValue());
          }
        case bzip2:
          if (compressionParameter == null) {
            return new Bzip2Compression();
          }
          else {
            return new Bzip2Compression(compressionParameter.intValue());
          }
        case xz:
          if (compressionParameter == null) {
            return new XzCompression();
          }
          else {
            return new XzCompression(compressionParameter.intValue());
          }
        case lz4:
          if (compressionParameter == null) {
            return new Lz4Compression();
          }
          else {
            return new Lz4Compression(compressionParameter.intValue());
          }
        case raw:
          return new RawCompression();
        default:
          return null;
      }
    }
  }

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
          description = "Compression type; not all compression types are " +
                  "supported by all file types " +
                  "(${COMPLETION-CANDIDATES}; default: blosc (n5/zarr), " +
                  "zstd (tiledb)"
  )
  private volatile CompressionTypes compressionType = null;

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
  private volatile DimensionOrder dimensionOrder = null;

  @Option(
          names = "--memo-directory",
          description = "Directory used to store .bfmemo cache files"
  )
  private volatile File memoDirectory;

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

  /**
   * Set of byte buffers that can be used concurrently, size will be equal to
   * {@link #maxWorkers}.  Each buffer will be the size of one chunk.
   */
  private volatile BlockingQueue<ByteBuffer> byteBuffers;

  private volatile ExecutorService executor;

  /** Whether or not the source file is little endian. */
  private boolean isLittleEndian;

  /**
   * The source file's pixel type.  Retrieved from
   * {@link IFormatReader#getPixelType()}.
   */
  private volatile int pixelType;

  /** The source file's pixel type bytes per pixel. */
  private volatile int bytesPerPixel;

  /** Total number of tiles at the current resolution during processing. */
  private volatile int tileCount;

  /** Current number of tiles processed at the current resolution. */
  private volatile AtomicInteger nTile;

  /** Gson instance configured similarly to the N5 library itself. */
  private final Gson gson;

  /**
   * Default constructor.
   */
  public Converter() {
    gson = new GsonBuilder()
        .registerTypeAdapter(DataType.class, new DataType.JsonAdapter())
        .registerTypeHierarchyAdapter(
            Compression.class, CompressionAdapter.getJsonAdapter())
        .disableHtmlEscaping()
        .create();
  }

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
    if (compressionType == null) {
      switch (fileType) {
        case n5:
        case zarr:
          compressionType = CompressionTypes.raw;
          break;
        case tiledb:
          compressionType = CompressionTypes.zstd;
          break;
        default:
          throw new IllegalArgumentException(
            "Unknown compression type: " + compressionType);
      }
    }

    ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger)
        LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    if (debug) {
      root.setLevel(Level.DEBUG);
    }
    else {
      root.setLevel(Level.INFO);
    }
    readers = new ArrayBlockingQueue<IFormatReader>(maxWorkers);
    queue = new LimitedQueue<Runnable>(maxWorkers);
    executor = new ThreadPoolExecutor(
      maxWorkers, maxWorkers, 0L, TimeUnit.MILLISECONDS, queue);
    byteBuffers = new ArrayBlockingQueue<ByteBuffer>(maxWorkers);
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
    if (pyramidName.equals("data.n5")) {
      switch (fileType) {
        case n5:
          break;
        case zarr:
          pyramidName = "data.zarr";
          break;
        case tiledb:
          pyramidName = "data.tiledb";
          break;
        default:
          throw new IllegalArgumentException(
              "Unsupported file type: " + fileType);
      }
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

    Cache<TilePointer, byte[]> tileCache = CacheBuilder.newBuilder()
        .maximumSize(maxCachedTiles)
        .build();

    // First find which reader class we need
    ClassList<IFormatReader> readerClasses =
        ImageReader.getDefaultReaderClasses();

    for (Class<?> reader : extraReaders) {
      readerClasses.addClass(0, (Class<IFormatReader>) reader);
      LOGGER.debug("Added extra reader: {}", reader);
    }

    ImageReader imageReader = new ImageReader(readerClasses);
    Class<?> readerClass;
    try {
      imageReader.setId(inputPath.toString());
      readerClass = imageReader.getReader().getClass();
    }
    finally {
      imageReader.close();
    }
    // Now with our found type instantiate our queue of readers for use
    // during conversion
    for (int i=0; i < maxWorkers; i++) {
      IFormatReader reader;
      Memoizer memoizer;
      try {
        reader = (IFormatReader) readerClass.getConstructor().newInstance();
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
      try {
        seriesCount = v.getSeriesCount();
        IMetadata meta = (IMetadata) v.getMetadataStore();

        for (int s=0; s<meta.getImageCount(); s++) {
          meta.setPixelsBigEndian(true, s);
        }

        String xml = getService().getOMEXML(meta);

        // write the original OME-XML to a file
        if (!Files.exists(outputPath)) {
          Files.createDirectories(outputPath);
        }
        Path omexmlFile = outputPath.resolve("METADATA.ome.xml");
        Files.write(omexmlFile, xml.getBytes(Constants.ENCODING));
      }
      catch (ServiceException se) {
        LOGGER.error("Could not retrieve OME-XML", se);
        return;
      }
      finally {
        readers.put(v);
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
   * @throws TileDBError
   */
  public void write(int series)
    throws FormatException, IOException, InterruptedException,
           EnumerationException, TileDBError
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
    args.add(series);
    args.add(resolution);
    if (additionalScaleFormatStringArgs != null) {
      String[] row = additionalScaleFormatStringArgs.get(series);
      for (String arg : row) {
        args.add(arg);
      }
    }
    return args.toArray();
  }

  /**
   * Converts from an N5 data type to a TileDB data type.
   * @param dataType N5 data type
   * @return TileDB data type
   */
  public static DataType n5TypeFromTileDbType(Datatype dataType) {
    switch (dataType) {
      case TILEDB_INT8:
        return DataType.INT8;
      case TILEDB_UINT8:
        return DataType.UINT8;
      case TILEDB_INT16:
        return DataType.INT16;
      case TILEDB_UINT16:
        return DataType.UINT16;
      case TILEDB_INT32:
        return DataType.INT32;
      case TILEDB_UINT32:
        return DataType.UINT32;
      case TILEDB_INT64:
        return DataType.INT64;
      case TILEDB_UINT64:
        return DataType.UINT64;
      case TILEDB_FLOAT32:
        return DataType.FLOAT32;
      case TILEDB_FLOAT64:
        return DataType.FLOAT64;
      default:
        throw new IllegalArgumentException(
            "Unsupported TileDB datatype: " + dataType);
    }
  }

  /**
   * Converts from a TileDB data type to an N5 data type.
   * @param dataType TileDB data type
   * @return N5 data type
   */
  public static Datatype tileDbTypeFromN5Type(DataType dataType) {
    switch (dataType) {
      case INT8:
        return Datatype.TILEDB_INT8;
      case UINT8:
        return Datatype.TILEDB_UINT8;
      case INT16:
        return Datatype.TILEDB_INT16;
      case UINT16:
        return Datatype.TILEDB_UINT16;
      case INT32:
        return Datatype.TILEDB_INT32;
      case UINT32:
        return Datatype.TILEDB_UINT32;
      case INT64:
        return Datatype.TILEDB_INT64;
      case UINT64:
        return Datatype.TILEDB_UINT64;
      case FLOAT32:
        return Datatype.TILEDB_FLOAT32;
      case FLOAT64:
        return Datatype.TILEDB_FLOAT64;
      default:
        throw new IllegalArgumentException(
            "Unsupported N5 datatype: " + dataType);
    }
  }

  private DatasetAttributes getDatasetAttributes(
      String pyramidPath, String pathName) throws IOException, TileDBError
  {
    switch (fileType) {
      case n5: {
        N5Reader n5 = new N5FSReader(pyramidPath);
        return n5.getDatasetAttributes(pathName);
      }
      case zarr: {
        N5ZarrReader n5 = new N5ZarrReader(pyramidPath);
        return n5.getDatasetAttributes(pathName);
      }
      case tiledb: {
        pathName = pathName.replaceAll("^/+", "");
        String uri = Paths.get(pyramidPath).resolve(pathName).toString();
        try (Context ctx = new Context();
             Array array = new Array(ctx, uri, QueryType.TILEDB_READ);
             ArraySchema schema = array.getSchema();
             Domain domain = schema.getDomain();
             Attribute attribute = schema.getAttribute("a1");
             FilterList filterList = attribute.getFilterList();)
        {
          int ndim = (int) domain.getNDim();
          final long[] dimensions = new long[ndim];
          final int[] blockSize = new int[ndim];
          for (int i = 0; i < ndim; i++) {
            try (Dimension<Long> dimension = domain.getDimension(i)) {
              Pair<Long, Long> dimensionDomain = dimension.getDomain();
              dimensions[ndim - i - 1] = dimensionDomain.getSecond() + 1;
              blockSize[ndim - i - 1] = dimension.getTileExtent().intValue();
            }
          }
          DataType dataType = n5TypeFromTileDbType(attribute.getType());

          TileDBAttributes attributes = new TileDBAttributes(
              dimensions, blockSize, dataType, null);

          if (filterList.getNumFilters() == 1) {
            try (CompressionFilter filter =
                  (CompressionFilter) attribute.getFilterList().getFilter(0)) {
              attributes.setCompressionFilter(filter);
            }
          }
          return attributes;
        }
      }
      default:
        throw new IllegalArgumentException(
            "Unsupported file type: " + fileType);
    }
  }

  private void createDataset(
      String pyramidPath, String resolutionString, long[] dimensions,
      int[] blockSize, DataType dataType)
          throws IOException, TileDBError
  {
    switch (fileType) {
      case n5: {
        Compression compression = N5Compression.getCompressor(compressionType,
              compressionParameter);
        N5Writer n5 = new N5FSWriter(pyramidPath);
        n5.createDataset(
            resolutionString, dimensions, blockSize, dataType, compression);
        break;
      }
      case zarr: {
        Compression compression = N5Compression.getCompressor(compressionType,
              compressionParameter);
        N5Writer n5 = new N5ZarrWriter(pyramidPath);
        n5.createDataset(
            resolutionString, dimensions, blockSize, dataType, compression);
        break;
      }
      case tiledb: {
        resolutionString = resolutionString.replaceAll("^/+", "");
        Path path = Paths.get(pyramidPath).resolve(resolutionString);
        Files.createDirectories(path.getParent());
        Datatype datatype = tileDbTypeFromN5Type(dataType);
        try (Context ctx = new Context();
             Domain domain = new Domain(ctx);
             Attribute a1 = new Attribute(ctx, "a1", datatype);
             ArraySchema schema = new ArraySchema(ctx, ArrayType.TILEDB_DENSE);
             FilterList filterList = new FilterList(ctx);
             CompressionFilter filter =
                 TileDBCompression.getCompressor(compressionType, ctx);
            )
        {
          if (filter != null) {
            a1.setFilterList(filterList.addFilter(filter));
          }

          for (int i = dimensions.length - 1; i >= 0; i--) {
            long dimension = dimensions[i];
            long extent = blockSize[i];
            domain.addDimension(new Dimension<Long>(
                ctx, dimensionOrder.toString().substring(i).toLowerCase(),
                Long.class, new Pair<Long, Long>(0L, dimension - 1), extent));
          }
          schema.setDomain(domain);
          schema.addAttribute(a1);
          Array.create(path.toString(), schema);
        }
        break;
      }
      default:
        throw new IllegalArgumentException(
            "Unsupported file type: " + fileType);
    }
  }

  private DataBlock<?> readBlock(
      String pyramidPath, String pathName, DatasetAttributes datasetAttributes,
      long[] gridPosition)
          throws IOException, TileDBError, InterruptedException
  {
    switch (fileType) {
      case n5: {
        N5Reader n5 = new N5FSReader(pyramidPath);
        return n5.readBlock(pathName, datasetAttributes, gridPosition);
      }
      case zarr: {
        N5ZarrReader n5 = new N5ZarrReader(pyramidPath);
        return n5.readBlock(pathName, datasetAttributes, gridPosition);
      }
      case tiledb: {
        pathName = pathName.replaceAll("^/+", "");
        String uri = Paths.get(pyramidPath).resolve(pathName).toString();
        long[] dimensions = datasetAttributes.getDimensions();
        long sizeX = dimensions[0];
        long sizeY = dimensions[1];
        int[] blockSize = datasetAttributes.getBlockSize().clone();
        int blockSizeX = blockSize[0];
        int blockSizeY = blockSize[1];
        long yStart = gridPosition[1] * blockSizeY;
        long yEnd = Math.min(sizeY - 1, yStart + blockSizeY - 1);
        int ySize = (int) (yEnd - yStart + 1);
        blockSize[1] = ySize;
        long xStart = gridPosition[0] * blockSizeX;
        long xEnd = Math.min(sizeX - 1, xStart + blockSizeX - 1);
        int xSize = (int) (xEnd - xStart + 1);
        long[] offsets = new long[] {
          gridPosition[4], gridPosition[4],  // Z, C, or T
          gridPosition[3], gridPosition[3],  // Z, C, or T
          gridPosition[2], gridPosition[2],  // Z, C, or T
          yStart, yEnd,
          xStart, xEnd
        };
        blockSize[0] = xSize;
        final ByteBuffer buffer = byteBuffers.take();
        try (Context ctx = new Context();
             Array array = new Array(ctx, uri, QueryType.TILEDB_READ);
             Query query = new Query(array, QueryType.TILEDB_READ);
             NativeArray subarray = new NativeArray(ctx, offsets, Long.class);
            )
        {
          query.setLayout(Layout.TILEDB_ROW_MAJOR);
          DataType dataType = datasetAttributes.getDataType();

          buffer.clear();
          buffer.position(buffer.capacity() - ySize * xSize * bytesPerPixel);
          ByteBuffer bufferSlice = buffer.slice();
          bufferSlice.order(ByteOrder.nativeOrder());
          query.setSubarray(subarray);
          query.setBuffer("a1", bufferSlice);
          query.submit();

          buffer.order(ByteOrder.BIG_ENDIAN);
          DataBlock<?> dataBlock =
              dataType.createDataBlock(blockSize, gridPosition);
          switch (dataType) {
            case INT8:
            case UINT8:
              buffer.get((byte[]) dataBlock.getData());
              break;
            case INT16:
            case UINT16:
              buffer.asShortBuffer().get((short[]) dataBlock.getData());
              break;
            case INT32:
            case UINT32:
              buffer.asIntBuffer().get((int[]) dataBlock.getData());
              break;
            case INT64:
            case UINT64:
              buffer.asLongBuffer().get((long[]) dataBlock.getData());
              break;
            case FLOAT32:
              buffer.asFloatBuffer().get((float[]) dataBlock.getData());
              break;
            case FLOAT64:
              buffer.asDoubleBuffer().get((double[]) dataBlock.getData());
              break;
            default:
              throw new IllegalArgumentException(
                  "Unsupported N5 datatype: " + dataType);
          }
          return dataBlock;
        } finally {
          byteBuffers.put(buffer);
        }
      }
      default:
        throw new IllegalArgumentException(
            "Unsupported file type: " + fileType);
    }
  }

  private DataBlock<?> writeBlock(
      String pyramidPath, String pathName, DatasetAttributes datasetAttributes,
      DataBlock<?> dataBlock)
          throws IOException, TileDBError, InterruptedException
  {
    switch (fileType) {
      case n5: {
        N5Writer n5 = new N5FSWriter(pyramidPath);
        n5.writeBlock(pathName, datasetAttributes, dataBlock);
        break;
      }
      case zarr: {
        N5Writer n5 = new N5ZarrWriter(pyramidPath);
        n5.writeBlock(pathName, datasetAttributes, dataBlock);
        break;
      }
      case tiledb: {
        pathName = pathName.replaceAll("^/+", "");
        long[] gridPosition = dataBlock.getGridPosition();
        String uri = Paths.get(pyramidPath).resolve(pathName).toString();
        long[] dimensions = datasetAttributes.getDimensions();
        long sizeX = dimensions[0];
        long sizeY = dimensions[1];
        int[] blockSize = datasetAttributes.getBlockSize();
        int blockSizeX = blockSize[0];
        int blockSizeY = blockSize[1];
        long yStart = gridPosition[1] * blockSizeY;
        long yEnd = Math.min(sizeY - 1, yStart + blockSizeY - 1);
        int ySize = (int) (yEnd - yStart + 1);
        long xStart = gridPosition[0] * blockSizeX;
        long xEnd = Math.min(sizeX - 1, xStart + blockSizeX - 1);
        int xSize = (int) (xEnd - xStart + 1);
        long[] offsets = new long[] {
          gridPosition[4], gridPosition[4],  // Z, C, or T
          gridPosition[3], gridPosition[3],  // Z, C, or T
          gridPosition[2], gridPosition[2],  // Z, C, or T
          yStart, yEnd,
          xStart, xEnd
        };
        final ByteBuffer buffer = byteBuffers.take();
        try (Context ctx = new Context();
             Array array = new Array(ctx, uri, QueryType.TILEDB_WRITE);
             Query query = new Query(array, QueryType.TILEDB_WRITE);
             NativeArray subarray = new NativeArray(ctx, offsets, Long.class);
            )
        {
          query.setLayout(Layout.TILEDB_ROW_MAJOR);

          int size = ySize * xSize * bytesPerPixel;
          buffer.clear();
          buffer.position(buffer.capacity() - size);
          ByteBuffer bufferSlice = buffer.slice();
          bufferSlice.order(ByteOrder.nativeOrder());
          bufferSlice.put(dataBlock.toByteBuffer().array());
          query.setSubarray(subarray);
          query.setBuffer("a1", bufferSlice);
          query.submit();
        } finally {
          byteBuffers.put(buffer);
        }
        break;
      }
      default:
        throw new IllegalArgumentException(
            "Unsupported file type: " + fileType);
    }
    return null;
  }

  private void setAttribute(
      String pyramidPath, String pathName, String key, Object value)
          throws IOException, TileDBError
  {
    switch (fileType) {
      case n5: {
        N5Writer n5 = new N5FSWriter(pyramidPath);
        n5.setAttribute(pathName, key, value);
        break;
      }
      case zarr: {
        N5Writer n5 = new N5ZarrWriter(pyramidPath);
        n5.setAttribute(pathName, key, value);
        break;
      }
      case tiledb: {
        pathName = pathName.replaceAll("^/+", "");
        String uri = Paths.get(pyramidPath).resolve(pathName).toString();
        try (Context ctx = new Context()) {
          if (!Array.exists(ctx, uri)) {
            // FIXME: Hack, can we do this better?
            createDataset(
                pyramidPath, pathName, new long[] {1}, new int[] {1},
                DataType.INT8);
          }

          try (Array array = new Array(ctx, uri, QueryType.TILEDB_WRITE);
               NativeArray metadata = new NativeArray(
                   ctx, gson.toJson(value).getBytes(StandardCharsets.UTF_8),
                   byte[].class)
              )
          {
            array.putMetadata(key, metadata);
          }
        }
        break;
      }
      default:
        throw new IllegalArgumentException(
            "Unsupported file type: " + fileType);
    }
  }

  private byte[] getTileDownsampled(
      int series, int resolution, int plane, int xx, int yy,
      int width, int height)
          throws FormatException, IOException, InterruptedException,
                 EnumerationException, TileDBError
  {
    final String pathName = "/" +
        String.format(scaleFormatString,
            getScaleFormatStringArgs(series, resolution - 1));
    final String pyramidPath = outputPath.resolve(pyramidName).toString();

    DatasetAttributes datasetAttributes = getDatasetAttributes(
        pyramidPath, pathName);
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
        ByteBuffer subTile = readBlock(
            pyramidPath, pathName, datasetAttributes, gridPosition
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
    return scaler.downsample(tile, width, height,
        PYRAMID_SCALE, bytesPerPixel, false,
        FormatTools.isFloatingPoint(pixelType),
        1, false);
  }

  private byte[] getTile(
      int series, int resolution, int plane, int xx, int yy,
      int width, int height)
          throws FormatException, IOException, InterruptedException,
                 EnumerationException, TileDBError
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
    String o = this.dimensionOrder.toString();
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
    String o = this.dimensionOrder.toString();
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
          InterruptedException, TileDBError
  {
    String pathName =
        "/" + String.format(scaleFormatString,
            getScaleFormatStringArgs(series, resolution));
    final String pyramidPath = outputPath.resolve(pyramidName).toString();
    DatasetAttributes datasetAttributes =
        getDatasetAttributes(pyramidPath, pathName);
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

      ByteBuffer bb = ByteBuffer.wrap(tile);
      if (resolution == 0 && isLittleEndian) {
        bb = bb.order(ByteOrder.LITTLE_ENDIAN);
      }
      switch (pixelType) {
        case FormatTools.INT8:
        case FormatTools.UINT8: {
          dataBlock = new ByteArrayDataBlock(size, gridPosition, tile);
          break;
        }
        case FormatTools.INT16:
        case FormatTools.UINT16: {
          short[] asShort = new short[tile.length / 2];
          bb.asShortBuffer().get(asShort);
          dataBlock = new ShortArrayDataBlock(size, gridPosition, asShort);
          break;
        }
        case FormatTools.FLOAT: {
          float[] asFloat = new float[tile.length / 4];
          bb.asFloatBuffer().get(asFloat);
          dataBlock = new FloatArrayDataBlock(size, gridPosition, asFloat);
          break;
        }
        case FormatTools.DOUBLE: {
          double[] asDouble = new double[tile.length / 8];
          bb.asDoubleBuffer().get(asDouble);
          dataBlock = new DoubleArrayDataBlock(size, gridPosition, asDouble);
          break;
        }
        default:
          throw new FormatException("Unsupported pixel type: "
              + FormatTools.getPixelTypeString(pixelType));
      }
    }
    finally {
      nTile.incrementAndGet();
      LOGGER.info("tile read complete {}/{}", nTile.get(), tileCount);
      t0.stop();
    }

    Slf4JStopWatch t1 = stopWatch();
    try {
      writeBlock(pyramidPath, pathName, datasetAttributes, dataBlock);
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
   * @throws TileDBError
   */
  public void saveResolutions(int series)
    throws FormatException, IOException, InterruptedException,
           EnumerationException, TileDBError
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
      bytesPerPixel = FormatTools.getBytesPerPixel(pixelType);
      if (dimensionOrder == null) {
        dimensionOrder = DimensionOrder.fromString(
            workingReader.getDimensionOrder());
      }

      byteBuffers.clear();
      for (int i = 0; i < maxWorkers; i++) {
        byteBuffers.add(ByteBuffer.allocateDirect(
            tileWidth * PYRAMID_SCALE * tileHeight * PYRAMID_SCALE
            * bytesPerPixel));
      }
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
      case FormatTools.FLOAT:
        dataType = DataType.FLOAT32;
        break;
      case FormatTools.DOUBLE:
        dataType = DataType.FLOAT64;
        break;
      default:
        throw new FormatException("Unsupported pixel type: "
            + FormatTools.getPixelTypeString(pixelType));
    }

    // fileset level metadata
    final String pyramidPath = outputPath.resolve(pyramidName).toString();
    setAttribute(pyramidPath, "/", "bioformats2raw.layout", LAYOUT);

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
      createDataset(
          pyramidPath, resolutionString,
          getDimensions(workingReader, scaledWidth, scaledHeight),
          new int[] {activeTileWidth, activeTileHeight, 1, 1, 1},
          dataType
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

    // series level metadata
    setSeriesLevelMetadata(pyramidPath, series, resolutions);
  }

  /**
   * Use {@link N5Writer#setAttribute(String, String, Object)}
   * to attach the multiscales metadata to the group containing
   * the pyramids.
   *
   * @param pyramidPath Pyramid root path.
   * @param series Series which is currently being written.
   * @param resolutions Total number of resolutions from which
   *                    names will be generated.
   * @throws IOException
   * @throws TileDBError
   */
  private void setSeriesLevelMetadata(
      String pyramidPath, int series, int resolutions)
          throws IOException, TileDBError
  {
    String resolutionString = "/" +  String.format(
            scaleFormatString, getScaleFormatStringArgs(series, 0));
    String seriesString = resolutionString.substring(0,
            resolutionString.lastIndexOf('/'));
    List<Map<String, Object>> multiscales =
            new ArrayList<Map<String, Object>>();
    Map<String, Object> multiscale = new HashMap<String, Object>();
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

    switch (fileType) {
      case n5: {
        N5Writer n5 = new N5FSWriter(pyramidPath);
        n5.createGroup(seriesString);
        break;
      }
      case zarr: {
        N5Writer n5 = new N5ZarrWriter(pyramidPath);
        n5.createGroup(seriesString);
        break;
      }
      case tiledb: {
        break;
      }
      default:
        throw new IllegalArgumentException(
            "Unsupported file type: " + fileType);
    }
    setAttribute(pyramidPath, seriesString, "multiscales", multiscales);
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
