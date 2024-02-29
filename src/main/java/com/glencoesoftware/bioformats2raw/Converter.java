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
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
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
import java.util.stream.IntStream;

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
import loci.formats.Memoizer;
import loci.formats.MinMaxCalculator;
import loci.formats.MissingLibraryException;
import loci.formats.in.DynamicMetadataOptions;
import loci.formats.meta.IMetadata;
import loci.formats.ome.OMEXMLMetadata;
import loci.formats.services.OMEXMLService;
import loci.formats.services.OMEXMLServiceImpl;
import ome.units.quantity.Length;
import ome.units.quantity.Quantity;
import ome.units.quantity.Time;
import ome.xml.meta.OMEXMLMetadataRoot;
import ome.xml.model.Channel;
import ome.xml.model.Filter;
import ome.xml.model.FilterSet;
import ome.xml.model.Image;
import ome.xml.model.Laser;
import ome.xml.model.LightPath;
import ome.xml.model.LightSource;
import ome.xml.model.LightSourceSettings;
import ome.xml.model.TransmittanceRange;
import ome.xml.model.enums.DimensionOrder;
import ome.xml.model.enums.EnumerationException;
import ome.xml.model.enums.UnitsLength;
import ome.xml.model.enums.UnitsTime;
import ome.xml.model.primitives.Color;
import ome.xml.model.primitives.PositiveInteger;

import org.perf4j.slf4j.Slf4JStopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bc.zarr.ArrayParams;
import com.bc.zarr.CompressorFactory;
import com.bc.zarr.DataType;
import com.bc.zarr.DimensionSeparator;
import com.bc.zarr.ZarrArray;
import com.bc.zarr.ZarrGroup;
import com.glencoesoftware.bioformats2raw.MiraxReader.TilePointer;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.math.DoubleMath;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import ch.qos.logback.classic.Level;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import ucar.ma2.InvalidRangeException;

/**
 * Command line tool for converting whole slide imaging files to Zarr.
 */
public class Converter implements Callable<Integer> {

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
  public static final Integer LAYOUT = 3;

  /** NGFF specification version.*/
  public static final String NGFF_VERSION = "0.4";

  private volatile Path inputPath;
  private volatile String outputLocation;

  private Map<String, String> outputOptions = new HashMap<String, String>();
  private volatile Integer pyramidResolutions;
  private volatile List<Integer> seriesList;

  private volatile int tileWidth;
  private volatile int tileHeight;
  private volatile int chunkDepth;
  private volatile String logLevel;
  private volatile boolean progressBars = false;
  private volatile boolean printVersion = false;
  private volatile boolean help = false;

  private volatile int maxWorkers;
  private volatile int maxCachedTiles;
  private volatile ZarrCompression compressionType;
  private volatile Map<String, Object> compressionProperties;
  private volatile Class<?>[] extraReaders;
  private volatile boolean omeroMetadata = true;
  private volatile boolean nested = true;
  private volatile String pyramidName = null;
  private volatile String scaleFormatString;
  private volatile Path additionalScaleFormatStringArgsCsv;

  /** Additional scale format string arguments after parsing. */
  private volatile List<String[]> additionalScaleFormatStringArgs;

  private volatile DimensionOrder dimensionOrder;
  private volatile File memoDirectory;
  private volatile boolean keepMemoFiles = false;
  private volatile Downsampling downsampling;
  private volatile boolean overwrite = false;
  private volatile Short fillValue = null;
  private volatile List<String> readerOptions;
  private volatile boolean noHCS = false;
  private volatile boolean noOMEMeta = false;
  private volatile boolean noRootGroup = false;
  private volatile boolean reuseExistingResolutions = false;
  private volatile int minSize;

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

  /** Calculated from outputLocation. */
  private volatile Path outputPath;

  private IProgressListener progressListener;
  private Map<Integer, int[]> tileCounts = new HashMap<Integer, int[]>();

  // Option setters

  /**
   * @param input path to the input data
   */
  @Parameters(
    index = "0",
    arity = "1",
    description = "file to convert",
    defaultValue = Option.NULL_VALUE
  )
  public void setInputPath(String input) {
    if (input != null) {
      // this could be expanded later to support files not on disk
      inputPath = Paths.get(input);
    }
    else {
      inputPath = null;
    }
  }

  /**
   * @param output path or URI where output data should be written
   */
  @Parameters(
    index = "1",
    arity = "1",
    description = "path to the output pyramid directory. " +
      "The given path can also be a URI (containing ://) " +
      "which will activate **experimental** support for " +
      "Filesystems. For example, if the output path given " +
      "is 's3://my-bucket/some-path' *and* you have an "+
      "S3FileSystem implementation in your classpath, then " +
      "all files will be written to S3.",
    defaultValue = Option.NULL_VALUE
  )
  public void setOutputPath(String output) {
    outputLocation = output;
  }

  /**
   * Options to use when connecting to a remote filesystem, e.g. s3.
   *
   * @param options output filesystem options
   */
  @Option(
    names = "--output-options",
    split = "\\|",
    description = "|-separated list of key-value pairs " +
      "to be used as an additional argument to Filesystem " +
      "implementations if used. For example, " +
      "--output-options=s3fs_path_style_access=true|... " +
      "might be useful for connecting to minio.",
    defaultValue = Option.NULL_VALUE
  )
  public void setOutputOptions(Map<String, String> options) {
    outputOptions = options;
  }

  /**
   * Define the number of resolutions in the generated pyramid.
   * By default, the resolution count is calculated based upon
   * the input image size. The resolution count includes the
   * full-resolution image. If the resolution count is null,
   * then the number of resolutions will be calculated automatically.
   * If not null, it must be greater than 0.
   *
   * @param resolutions pyramid resolution count
   */
  @Option(
    names = {"-r", "--resolutions"},
    description = "Number of pyramid resolutions to generate",
    defaultValue = Option.NULL_VALUE
  )
  public void setResolutions(Integer resolutions) {
    if (resolutions == null || resolutions > 0) {
      pyramidResolutions = resolutions;
    }
    else {
      LOGGER.warn("Ignoring invalid resolution count: {}", resolutions);
    }
  }

  /**
   * Define a subset of input series indexes to convert.
   * Series indexes begin at 0 and are the Bio-Formats series/OME Image
   * index reported when resolution flattening has not been applied.
   *
   * @param seriesToConvert series index list
   */
  @Option(
    names = {"-s", "--series"},
    arity = "0..1",
    split = ",",
    description = "Comma-separated list of series indexes to convert",
    defaultValue = Option.NULL_VALUE
  )
  public void setSeriesList(List<Integer> seriesToConvert) {
    if (seriesToConvert != null) {
      seriesList = seriesToConvert;
    }
    else {
      seriesList = new ArrayList<Integer>();
    }
  }

  /**
   * Set the maximum tile width (X chunk size) for input and output.
   *
   * @param width tile width
   */
  @Option(
    names = {"-w", "--tile-width", "--tile_width"},
    description = "Maximum tile width (default: ${DEFAULT-VALUE}). " +
      "This is both the chunk size (in X) when writing Zarr and the tile " +
      "size used for reading from the original data. Changing the tile " +
      "size may have performance implications.",
    defaultValue = "1024"
  )
  public void setTileWidth(int width) {
    if (width > 0) {
      tileWidth = width;
    }
    else {
      LOGGER.warn("Ignoring invalid tile width: {}", width);
    }
  }

  /**
   * Set the maximum tile height (Y chunk size) for input and output.
   *
   * @param height tile height
   */
  @Option(
    names = {"-h", "--tile-height", "--tile_height"},
    description = "Maximum tile height (default: ${DEFAULT-VALUE}). " +
      "This is both the chunk size (in Y) when writing Zarr and the tile " +
      "size used for reading from the original data. Changing the tile " +
      "size may have performance implications.",
    defaultValue = "1024"
  )
  public void setTileHeight(int height) {
    if (height > 0) {
      tileHeight = height;
    }
    else {
      LOGGER.warn("Ignoring invalid tile height: {}", height);
    }
  }

  /**
   * Set the maximum depth (Z chunk size) for writing Zarr.
   *
   * @param depth Z chunk size
   */
  @Option(
    names = {"-z", "--chunk-depth", "--chunk_depth"},
    description = "Maximum chunk depth to read (default: ${DEFAULT-VALUE}) ",
    defaultValue = "1"
  )
  public void setChunkDepth(int depth) {
    if (depth > 0) {
      chunkDepth = depth;
    }
    else {
      LOGGER.warn("Ignoring invalid chunk depth: {}", depth);
    }
  }

  /**
   * Set the slf4j logging level. Defaults to "WARN".
   *
   * @param level logging level
   */
  @Option(
    names = {"--log-level", "--debug"},
    arity = "0..1",
    description = "Change logging level; valid values are " +
      "OFF, ERROR, WARN, INFO, DEBUG, TRACE and ALL. " +
      "(default: ${DEFAULT-VALUE})",
    defaultValue = "WARN",
    fallbackValue = "DEBUG"
  )
  public void setLogLevel(String level) {
    if (level != null) {
      logLevel = level;
    }
  }

  /**
   * Configure whether or not progress bars are shown during conversion.
   * Progress bars are turned off by default.
   *
   * @param useProgressBars whether or not to show progress bars
   */
  @Option(
    names = {"-p", "--progress"},
    description = "Print progress bars during conversion",
    defaultValue = "false"
  )
  public void setProgressBars(boolean useProgressBars) {
    progressBars = useProgressBars;
  }

  /**
   * Configure whether to print version information and exit
   * without converting.
   *
   * @param versionOnly whether or not to print version information and exit
   */
  @Option(
    names = "--version",
    description = "Print version information and exit",
    help = true,
    defaultValue = "false"
  )
  public void setPrintVersionOnly(boolean versionOnly) {
    printVersion = versionOnly;
  }

  /**
   * Configure whether to print help and exit without converting.
   *
   * @param helpOnly whether or not to print help and exit
   */
  @Option(
    names = "--help",
    description = "Print usage information and exit",
    usageHelp = true,
    defaultValue = "false"
  )
  public void setHelp(boolean helpOnly) {
    help = helpOnly;
  }

  /**
   * Set the maximum number of workers to use for converting tiles.
   * Defaults to 4 or the number of detected CPUs, whichever is smaller.
   *
   * @param workers maximum worker count
   */
  @Option(
    names = {"--max-workers", "--max_workers"},
    description = "Maximum number of workers (default: ${DEFAULT-VALUE})",
    defaultValue = "4"
  )
  public void setMaxWorkers(int workers) {
    int availableProcessors = Runtime.getRuntime().availableProcessors();
    if (workers > availableProcessors) {
      maxWorkers = availableProcessors;
    }
    else if (workers > 0) {
      maxWorkers = workers;
    }
    else {
      LOGGER.warn("Ignoring invalid worker count: {}", workers);
    }
  }

  /**
   * Set the maximum number of tiles that can be cached.
   * Depending upon the input format, this can improve conversion performance
   * by reducing the number of tile reads.
   *
   * @param maxTiles maximum number of cached tiles
   */
  @Option(
    names = {"--max-cached-tiles", "--max_cached_tiles"},
    description =
      "Maximum number of tiles that will be cached across all "
      + "workers (default: ${DEFAULT-VALUE})",
    defaultValue = "64"
  )
  public void setMaxCachedTiles(int maxTiles) {
    maxCachedTiles = maxTiles;
  }

  /**
   * Set the compression type for the output Zarr. Defaults to blosc.
   *
   * @param compression compression type
   */
  @Option(
          names = {"-c", "--compression"},
          description = "Compression type for Zarr " +
                  "(${COMPLETION-CANDIDATES}; default: ${DEFAULT-VALUE})",
          defaultValue = "blosc"
  )
  public void setCompression(ZarrCompression compression) {
    if (compression != null) {
      compressionType = compression;
    }
  }

  /**
   * Compression-specific options as defined by jzarr.
   *
   * @param properties compression properties
   */
  @Option(
          names = {"--compression-properties"},
          description = "Properties for the chosen compression (see " +
            "https://jzarr.readthedocs.io/en/latest/tutorial.html#compressors" +
            " )",
          defaultValue = Option.NULL_VALUE
  )
  public void setCompressionProperties(Map<String, Object> properties) {
    if (properties != null) {
      compressionProperties = properties;
    }
    else {
      compressionProperties = new HashMap<String, Object>();
    }
  }

  /**
   * List of extra readers to use. This can include any reader that is on
   * the classpath and not defined by Bio-Formats. By default this includes
   * every reader in the bioformats2raw repository.
   *
   * @param extraReaderList list of extra readers
   */
  @Option(
          names = "--extra-readers",
          arity = "0..1",
          split = ",",
          description = "Separate set of readers to include; " +
                  "(default: ${DEFAULT-VALUE})",
          defaultValue =
            "com.glencoesoftware.bioformats2raw.PyramidTiffReader," +
            "com.glencoesoftware.bioformats2raw.MiraxReader," +
            "com.glencoesoftware.bioformats2raw.BioTekReader," +
            "com.glencoesoftware.bioformats2raw.ND2PlateReader," +
            "com.glencoesoftware.bioformats2raw.MetaxpressReader," +
            "com.glencoesoftware.bioformats2raw.MCDReader"
  )
  public void setExtraReaders(Class<?>[] extraReaderList) {
    if (extraReaderList != null) {
      extraReaders = extraReaderList;
    }
  }

  /**
   * Configure whether or not to calculate min/max pixel values and
   * write OMERO rendering metadata. By default, min/max values are
   * calculated and rendering metadata is written.
   *
   * @param noMinMax false if OMERO rendering metadata should be written
   */
  @Option(
          names = "--no-minmax", negatable=true,
          description = "Whether to calculate minimum and maximum pixel " +
                        "values. Min/max calculation can result in slower " +
                        "conversions. If true, min/max values are saved as " +
                        "OMERO rendering metadata (true by default)",
          defaultValue = "false"
  )
  public void setCalculateOMEROMetadata(boolean noMinMax) {
    omeroMetadata = !noMinMax;
  }

  /**
   * Configure whether or not the output Zarr is written as nested,
   * using the '/' chunk separator.
   *
   * @param unnested false if nested chunk storage should be used
   */
  @Option(
          names = "--no-nested", negatable=true,
          description = "Whether to use '/' as the chunk path separator " +
                  "(true by default)",
          defaultValue = "false"
  )
  public void setUnnested(boolean unnested) {
    nested = !unnested;
  }

  /**
   * Set the name of the pyramid being converted.
   * If not null, this will be used to create a subdirectory
   * in the output location.
   * By default, null for compatibility with raw2ometiff.
   *
   * @param pyramid pyramid name
   */
  @Option(
          names = "--pyramid-name",
          description = "Name of pyramid (default: ${DEFAULT-VALUE}) " +
                  "[Can break compatibility with raw2ometiff]",
          defaultValue = Option.NULL_VALUE
  )
  public void setPyramidName(String pyramid) {
    pyramidName = pyramid;
  }

  /**
   * Set the format string for scale paths.
   *
   * @param formatString scale path format string
   */
  @Option(
          names = "--scale-format-string",
          description = "Format string for scale paths; the first two " +
                  "arguments will always be series and resolution followed " +
                  "by any additional arguments brought in from " +
                  "`--additional-scale-format-string-args` " +
                  "[Can break compatibility with raw2ometiff] " +
                  "(default: ${DEFAULT-VALUE})",
          defaultValue = "%d/%d"
  )
  public void setScaleFormat(String formatString) {
    if (formatString != null) {
      scaleFormatString = formatString;
    }
  }

  /**
   * Set path to a CSV file with additional scale formatting arguments.
   *
   * @param scaleFormatCSV path to CSV file
   */
  @Option(
          names = "--additional-scale-format-string-args",
          description = "Additional format string argument CSV file (without " +
                  "header row).  Arguments will be added to the end of the " +
                  "scale format string mapping the at the corresponding CSV " +
                  "row index.  It is expected that the CSV file contain " +
                  "exactly the same number of rows as the input file has " +
                  "series",
          defaultValue = Option.NULL_VALUE
  )
  public void setAdditionalScaleFormatCSV(Path scaleFormatCSV) {
    additionalScaleFormatStringArgsCsv = scaleFormatCSV;
  }

  /**
   * Set the directory for memo (metadata cache) files.
   *
   * @param memoDir memo file directory
   */
  @Option(
          names = "--memo-directory",
          description = "Directory used to store .bfmemo cache files",
          defaultValue = Option.NULL_VALUE
  )
  public void setMemoDirectory(File memoDir) {
    memoDirectory = memoDir;
  }

  /**
   * Set whether or not to keep memo (metadata cache) files after conversion.
   * By default, memo files are deleted after conversion.
   *
   * @param keepMemos whether or not to keep memo files
   */
  @Option(
          names = "--keep-memo-files",
          description = "Do not delete .bfmemo files created during conversion",
          defaultValue = "false"
  )
  public void setKeepMemoFiles(boolean keepMemos) {
    keepMemoFiles = keepMemos;
  }

  /**
   * Set downsampling algorithm.
   *
   * @param downsampleType downsampling type
   */
  @Option(
          names = "--downsample-type",
          description = "Tile downsampling algorithm " +
            "(${COMPLETION-CANDIDATES})",
          defaultValue = "SIMPLE"
  )
  public void setDownsampling(Downsampling downsampleType) {
    if (downsampleType != null) {
      downsampling = downsampleType;
    }
  }

  /**
   * Set whether or not to overwrite an existing output directory.
   *
   * @param canOverwrite whether or not overwriting is allowed
   */
  @Option(
          names = "--overwrite",
          description = "Overwrite the output directory if it exists",
          defaultValue = "false"
  )
  public void setOverwrite(boolean canOverwrite) {
    overwrite = canOverwrite;
  }

  /**
   * Set the fill value for missing tiles. Must be in the range 0-255.
   * Currently applies to .mrxs input data only.
   *
   * @param tileFill pixel value (0-255) to fill
   */
  @Option(
          names = "--fill-value",
          description = "Default value to fill in for missing tiles (0-255)" +
                        " (currently .mrxs only)",
          defaultValue = Option.NULL_VALUE
  )
  public void setFillValue(Short tileFill) {
    if (tileFill == null || (tileFill >= 0 && tileFill <= 255)) {
      fillValue = tileFill;
    }
    else {
      LOGGER.warn("Ignoring invalid fill value (must be 0-255): {}", tileFill);
    }
  }

  /**
   * Set any reader-specific options.
   * See https://docs.openmicroscopy.org/bio-formats/latest/formats/options.html
   *
   * @param readerOpts reader options
   */
  @Option(
          arity = "0..1",
          names = "--options",
          split = ",",
          description =
            "Reader-specific options, in format key=value[,key2=value2]",
          defaultValue = Option.NULL_VALUE
  )
  public void setReaderOptions(List<String> readerOpts) {
    if (readerOpts != null) {
      readerOptions = readerOpts;
    }
    else {
      readerOptions = new ArrayList<String>();
    }
  }

  /**
   * Set whether or not to write HCS (plate/well) metadata.
   * See https://ngff.openmicroscopy.org/latest/#plate-md.
   * HCS metadata is written by default when the input data
   * represents a plate.
   *
   * @param noHCSWriting true if HCS metadata should not be written
   */
  @Option(
          names = "--no-hcs",
          description = "Turn off HCS writing",
          defaultValue = "false"
  )
  public void setNoHCS(boolean noHCSWriting) {
    noHCS = noHCSWriting;
  }

  /**
   * Set whether or not to write OME-XML metadata.
   * See https://ngff.openmicroscopy.org/latest/#bf2raw
   * By default, OME-XML metadata is written.
   *
   * @param noOMEMetaWriting true if OME-XML should not be written
   */
  @Option(
          names = "--no-ome-meta-export",
          description = "Turn off OME metadata exporting " +
                        "[Will break compatibility with raw2ometiff]",
          defaultValue = "false"
  )
  public void setNoOMEMeta(boolean noOMEMetaWriting) {
    noOMEMeta = noOMEMetaWriting;
  }

  /**
   * Set whether or not to write a root Zarr group.
   * By default, the root group is written.
   *
   * @param noRootGroupWriting true if the root group should not be written
   */
  @Option(
          names = "--no-root-group",
          description = "Turn off creation of root group and corresponding " +
                        "metadata [Will break compatibility with raw2ometiff]",
          defaultValue = "false"
  )
  public void setNoRootGroup(boolean noRootGroupWriting) {
    noRootGroup = noRootGroupWriting;
  }

  /**
   * Set whether or not to keep pyramids from the input data.
   * By default, only the full-resolution images from the input data are kept.
   * Pyramid resolutions are generated by downsampling the full-resolution
   * image, and any pyramid resolutions in the input data are ignored.
   *
   * @param reuse whether or not to reuse pyramids in the input data, if present
   */
  @Option(
      names = "--use-existing-resolutions",
      description = "Use existing sub resolutions from original input format" +
          "[Will break compatibility with raw2ometiff]",
      defaultValue = "false"
  )
  public void setReuseExistingResolutions(boolean reuse) {
    reuseExistingResolutions = reuse;
  }

  /**
   * The target size of the largest XY dimension in the
   * smallest pyramid resolution. Defaults to 256.
   * Used to calculate the number of pyramid resolutions,
   * if the resolution count was not explicitly set.
   *
   * @param min target size of smallest pyramid resolution
   */
  @Option(
      names = "--target-min-size",
      description = "Specifies the desired size for the largest XY dimension " +
          "of the smallest resolution, when calculating the number " +
          "of resolutions generate. If the target size cannot be matched " +
          "exactly, the largest XY dimension of the smallest resolution " +
          "should be smaller than the target size.",
      defaultValue = "" + MIN_SIZE
  )
  public void setMinImageSize(int min) {
    if (min > 0) {
      minSize = min;
    }
    else {
      LOGGER.warn("Ignoring invalid minimum image size: {}", min);
    }
  }

  /**
   * Set the output dimension order. Defaults to XYZCT for compliance with
   * the OME-NGFF specification.
   *
   * @param order dimension order
   */
  @Option(
          names = "--dimension-order",
          description = "Override the input file dimension order in the " +
                  "output file [Can break compatibility with raw2ometiff] " +
                  "(${COMPLETION-CANDIDATES})",
          converter = DimensionOrderConverter.class,
          defaultValue = "XYZCT"
  )
  public void setDimensionOrder(DimensionOrder order) {
    dimensionOrder = order;
  }

  // Option getters

  /**
   * @return path to input data
   */
  public String getInputPath() {
    return inputPath == null ? null : inputPath.toString();
  }

  /**
   * @return path to output data
   */
  public String getOutputPath() {
    return outputLocation;
  }

  /**
   * @return current output filesystem options
   */
  public Map<String, String> getOutputOptions() {
    return outputOptions;
  }

  /**
   * @return number of pyramid resolutions
   */
  public Integer getResolutions() {
    return pyramidResolutions;
  }

  /**
   * @return list of series indexes to convert
   */
  public List<Integer> getSeriesList() {
    return seriesList;
  }

  /**
   * @return tile width (X chunk size)
   */
  public int getTileWidth() {
    return tileWidth;
  }

  /**
   * @return tile height (Y chunk size)
   */
  public int getTileHeight() {
    return tileHeight;
  }

  /**
   * @return tile depth (Z chunk size)
   */
  public int getChunkDepth() {
    return chunkDepth;
  }

  /**
   * @return slf4j logging level
   */
  public String getLogLevel() {
    return logLevel;
  }

  /**
   * @return true if progress bars are displayed
   */
  public boolean getProgressBars() {
    return progressBars;
  }

  /**
   * @return true if only version info is displayed
   */
  public boolean getPrintVersionOnly() {
    return printVersion;
  }

  /**
   * @return true if only usage info is displayed
   */
  public boolean getHelp() {
    return help;
  }

  /**
   * @return maximum number of worker threads
   */
  public int getMaxWorkers() {
    return maxWorkers;
  }

  /**
   * @return maximum number of cached input tiles
   */
  public int getMaxCachedTiles() {
    return maxCachedTiles;
  }

  /**
   * @return current compression type
   */
  public ZarrCompression getCompression() {
    return compressionType;
  }

  /**
   * @return compression options
   */
  public Map<String, Object> getCompressionProperties() {
    return compressionProperties;
  }

  /**
   * @return list of extra readers to use
   */
  public Class<?>[] getExtraReaders() {
    return extraReaders;
  }

  /**
   * @return true if min/max pixel values and OMERO rendering
   *         metadata are calculated
   */
  public boolean getCalculateOMEROMetadata() {
    return omeroMetadata;
  }

  /**
   * @return true if chunks are stored nested ('/' chunk separator)
   */
  public boolean getNested() {
    return nested;
  }

  /**
   * @return pyramid name
   */
  public String getPyramidName() {
    return pyramidName;
  }

  /**
   * @return scale formatting pattern
   */
  public String getScaleFormat() {
    return scaleFormatString;
  }

  /**
   * @return path to CSV file with additional scale formatting arguments
   */
  public Path getAdditionalScaleFormatCSV() {
    return additionalScaleFormatStringArgsCsv;
  }

  /**
   * @return directory containing memo (metadata cache) files
   */
  public File getMemoDirectory() {
    return memoDirectory;
  }

  /**
   * @return true if memo (metadata cache) files are retained after conversion
   */
  public boolean getKeepMemoFiles() {
    return keepMemoFiles;
  }

  /**
   * @return downsampling algorithm
   */
  public Downsampling getDownsampling() {
    return downsampling;
  }

  /**
   * @return true if existing Zarr data can be overwritten
   */
  public boolean getOverwrite() {
    return overwrite;
  }

  /**
   * @return fill value for missing tiles (.mrxs input only)
   */
  public Short getFillValue() {
    return fillValue;
  }

  /**
   * See:
   * https://docs.openmicroscopy.org/bio-formats/latest/formats/options.html.
   *
   * @return reader-specific options
   */
  public List<String> getReaderOptions() {
    return readerOptions;
  }

  /**
   * See https://ngff.openmicroscopy.org/latest/#plate-md.
   *
   * @return false if HCS (plate/well) metadata is written when the input data
   *         represents a plate
   */
  public boolean getNoHCS() {
    return noHCS;
  }

  /**
   * See https://ngff.openmicroscopy.org/latest/#bf2raw.
   *
   * @return false if OME-XML metadata is written
   */
  public boolean getNoOMEMeta() {
    return noOMEMeta;
  }

  /**
   * @return true if a root Zarr group will not be written
   */
  public boolean getNoRootGroup() {
    return noRootGroup;
  }

  /**
   * @return true if resolutions in the input data are converted
   */
  public boolean getReuseExistingResolutions() {
    return reuseExistingResolutions;
  }

  /**
   * @return target size of the largest XY dimension in the smallest
   *         pyramid resolution
   */
  public int getMinImageSize() {
    return minSize;
  }

  /**
   * @return current dimension roder
   */
  public DimensionOrder getDimensionOrder() {
    return dimensionOrder;
  }

  // Conversion methods

  /**
   * @return 0 if conversion completed without error,
   *         -1 if conversion was not performed
   * @throws Exception on most conversion errors
   */
  @Override
  public Integer call() throws Exception {
    ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger)
        LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    root.setLevel(Level.toLevel(logLevel));

    if (help) {
      return -1;
    }

    if (printVersion) {
      String version = Optional.ofNullable(
        this.getClass().getPackage().getImplementationVersion()
        ).orElse("development");
      System.out.println("Version = " + version);
      System.out.println("Bio-Formats version = " + FormatTools.VERSION);
      System.out.println("NGFF specification version = " + NGFF_VERSION);
      return -1;
    }

    if (inputPath == null) {
      throw new IllegalArgumentException("Input path not specified");
    }
    if (outputLocation == null) {
      throw new IllegalArgumentException("Output location not specified");
    }

    if (fillValue != null && (fillValue < 0 || fillValue > 255)) {
      throw new IllegalArgumentException("Invalid fill value: " + fillValue);
    }
    if (tileWidth != 1024 || tileHeight != 1024) {
      LOGGER.warn("Non-default tile size: {} x {}. " +
        "This may cause performance issues in some applications.",
        tileWidth, tileHeight);
    }

    OpenCVTools.loadOpenCV();

    if (progressBars) {
      setProgressListener(new ProgressBarListener(logLevel));
    }

    if (outputLocation.contains("://")) {

      LOGGER.info("*** experimental remote filesystem support ***");

      URI uri = URI.create(outputLocation);
      URI endpoint = new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(),
              uri.getPort(), "", "", "");
      String path = uri.getRawPath().substring(1); // drop initial "/"
      int first = path.indexOf("/");
      String bucket = "/" + path.substring(0, first);
      String rest = path.substring(first + 1);

      LOGGER.debug("endpoint: {}", endpoint);
      LOGGER.debug("bucket: {}", bucket);
      LOGGER.debug("path: {}", rest);
      LOGGER.debug("opts: {}", outputOptions);

      FileSystem fs = FileSystems.newFileSystem(endpoint, outputOptions);
      outputPath = fs.getPath(bucket, rest);
      if (Files.exists(outputPath)) {
        if (overwrite) {
          LOGGER.warn("overwriting on remote filesystem not yet supported");
        }
        throw new IllegalArgumentException(
                "Output path " + outputPath + " already exists.");
      }
    }
    else {
      outputPath = Paths.get(outputLocation);

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
    }

    readers = new ArrayBlockingQueue<IFormatReader>(maxWorkers);
    queue = new LimitedQueue<Runnable>(maxWorkers);
    executor = new ThreadPoolExecutor(
      maxWorkers, maxWorkers, 0L, TimeUnit.MILLISECONDS, queue);
    convert();
    return 0;
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
    boolean savedMemoFile = false;
    for (int i=0; i < maxWorkers; i++) {
      IFormatReader reader;
      Memoizer memoizer;
      try {
        reader = (IFormatReader) readerClass.getConstructor().newInstance();
        if (fillValue != null && reader instanceof MiraxReader) {
          ((MiraxReader) reader).setFillValue(fillValue.byteValue());
        }
        memoizer = createMemoizer(reader);
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

      memoizer.setOriginalMetadataPopulated(!noOMEMeta);
      memoizer.setFlattenedResolutions(false);
      memoizer.setMetadataFiltered(true);
      memoizer.setMetadataStore(createMetadata());
      ChannelSeparator separator = new ChannelSeparator(memoizer);
      separator.setId(inputPath.toString());
      separator.setResolution(0);
      if (reader instanceof MiraxReader) {
        ((MiraxReader) reader).setTileCache(tileCache);
      }
      if (omeroMetadata) {
        readers.add(new MinMaxCalculator(separator));
      }
      else {
        readers.add(separator);
      }
      savedMemoFile = savedMemoFile || memoizer.isSavedToMemo();
    }

    // Finally, perform conversion on all series
    try {
      IFormatReader v = readers.take();
      IMetadata meta = null;
      try {
        meta = (IMetadata) v.getMetadataStore();
        ((OMEXMLMetadata) meta).resolveReferences();

        if (!noHCS) {
          noHCS = !hasValidPlate(meta);
          int plateCount = meta.getPlateCount();
          if (!noHCS && plateCount > 1) {
            throw new IllegalArgumentException(
              "Found " + plateCount + " plates; only one can be converted. " +
              "Use --no-hcs to as a work-around.");
          }
        }
        else {
          ((OMEXMLMetadata) meta).resolveReferences();
          OMEXMLMetadataRoot root = (OMEXMLMetadataRoot) meta.getRoot();
          for (int i=0; i<meta.getPlateCount(); i++) {
            root.removePlate(root.getPlate(0));
          }
          meta.setRoot(root);
        }

        if (seriesList.size() > 0) {
          ((OMEXMLMetadata) meta).resolveReferences();
          OMEXMLMetadataRoot root = (OMEXMLMetadataRoot) meta.getRoot();
          int toRemove = meta.getImageCount();

          for (Integer index : seriesList) {
            if (index >= 0 && index < toRemove) {
              root.addImage(root.getImage(index));
            }
          }

          for (int i=0; i<toRemove; i++) {
            root.removeImage(root.getImage(0));
          }
          meta.setRoot(root);
        }
        else {
          for (int i=0; i<meta.getImageCount(); i++) {
            seriesList.add(i);
          }
        }

        for (int s=0; s<meta.getImageCount(); s++) {
          meta.setPixelsBigEndian(true, s);

          if (dimensionOrder != null) {
            meta.setPixelsDimensionOrder(dimensionOrder, s);
          }

          if (!noHCS) {
            HCSIndex index = new HCSIndex(meta, s);
            hcsIndexes.add(index);
          }
        }

        if (!noOMEMeta) {
          OMEXMLService service = getService();
          service.removeBinData((OMEXMLMetadata) meta);
          service.removeTiffData((OMEXMLMetadata) meta);
          for (int s=0; s<meta.getImageCount(); s++) {
            service.addMetadataOnly((OMEXMLMetadata) meta, s, s == 0);
          }
          String xml = service.getOMEXML(meta);

          // write the original OME-XML to a file
          Path metadataPath = getRootPath().resolve("OME");
          if (!Files.exists(metadataPath)) {
            Files.createDirectories(metadataPath);
          }
          Path omexmlFile = metadataPath.resolve(METADATA_FILE);
          Files.write(omexmlFile, xml.getBytes(Constants.ENCODING));
        }
      }
      catch (ServiceException se) {
        LOGGER.error("Could not retrieve OME-XML", se);
        return;
      }
      finally {
        readers.put(v);
      }

      if (!noHCS) {
        scaleFormatString = "%s/%s/%d/%d";
      }

      // fileset level metadata
      if (!noRootGroup) {
        final ZarrGroup root = ZarrGroup.create(getRootPath());
        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put("bioformats2raw.layout", LAYOUT);

        root.writeAttributes(attributes);
      }
      if (!noOMEMeta) {
        Path metadataPath = getRootPath().resolve("OME");
        final ZarrGroup root = ZarrGroup.create(metadataPath);
        Map<String, Object> attributes = new HashMap<String, Object>();

        // record the path to each series (multiscales) and the corresponding
        // series (OME-XML Image) index
        // using the index as the key would mean that the index is stored
        // as a string instead of an integer
        List<String> groups = new ArrayList<String>();
        for (Integer index : seriesList) {
          String resolutionString = String.format(
                  scaleFormatString, getScaleFormatStringArgs(index, 0));
          String seriesString = "";
          if (resolutionString.indexOf('/') >= 0) {
            seriesString = resolutionString.substring(0,
                resolutionString.lastIndexOf('/'));
          }
          groups.add(seriesString);
        }
        attributes.put("series", groups);

        root.writeAttributes(attributes);
      }

      // pre-calculate resolution and tile counts
      long totalTiles = 0;
      for (Integer index : seriesList) {
        int[] counts = calculateTileCounts(index);
        tileCounts.put(index, counts);
        for (int count : counts) {
          totalTiles += count;
        }
      }
      getProgressListener().notifyStart(seriesList.size(), totalTiles);

      for (Integer index : seriesList) {
        try {
          write(index);
        }
        catch (Throwable t) {
          LOGGER.error("Error while writing series {}", index, t);
          unwrapException(t);
          return;
        }
      }

      if (meta != null) {
        saveHCSMetadata(meta);
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

    // delete the memo file if it was saved and it's not explicitly kept
    // this should mean that memo files which existed before conversion
    // started will not be deleted
    if (savedMemoFile && !keepMemoFiles) {
      File memoFile = createMemoizer(null).getMemoFile(inputPath.toString());
      memoFile.delete();
    }
  }

  /**
   * Pre-calculate the number of resolutions and tiles for the
   * given series index. This is useful for accurate progress reporting
   * and failing faster for incompatible pixel/downsampling combinations.
   *
   * @param series series index
   * @return array of tile counts; the array length is the number of resolutions
   */
  private int[] calculateTileCounts(int series)
    throws FormatException, IOException, InterruptedException,
           EnumerationException
  {
    readers.forEach((reader) -> {
      reader.setSeries(series);
    });

    IFormatReader workingReader = readers.take();
    int resolutions = 1;
    int sizeX;
    int sizeY;
    int sizeZ;
    int imageCount;
    try {
      // calculate a reasonable pyramid depth if not specified as an argument
      sizeX = workingReader.getSizeX();
      sizeY = workingReader.getSizeY();
      if (pyramidResolutions == null) {
        if (workingReader.getResolutionCount() > 1
            && reuseExistingResolutions)
        {
          resolutions = workingReader.getResolutionCount();
        }
        else {
          resolutions = calculateResolutions(sizeX, sizeY);
        }
      }
      else {
        resolutions = pyramidResolutions;

        // check to make sure too many resolutions aren't being used
        if ((int) (sizeX / Math.pow(PYRAMID_SCALE, resolutions)) == 0 ||
          (int) (sizeY / Math.pow(PYRAMID_SCALE, resolutions)) == 0)
        {
          resolutions = calculateResolutions(sizeX, sizeY);
          LOGGER.warn("Too many resolutions specified; reducing to {}",
            resolutions);
        }
      }
      LOGGER.info("Using {} pyramid resolutions", resolutions);
      sizeZ = workingReader.getSizeZ();
      imageCount = workingReader.getImageCount();
      pixelType = workingReader.getPixelType();
    }
    finally {
      readers.put(workingReader);
    }

    int[] resTileCounts = new int[resolutions];

    if ((pixelType == FormatTools.INT8 || pixelType == FormatTools.INT32) &&
      getDownsampling() != Downsampling.SIMPLE && resolutions > 0)
    {
      String type = FormatTools.getPixelTypeString(pixelType);
      throw new UnsupportedOperationException(
        "OpenCV does not support downsampling " + type + " data. " +
        "See https://github.com/opencv/opencv/issues/7862");
    }

    for (int resCounter=0; resCounter<resolutions; resCounter++) {
      final int resolution = resCounter;
      int scale = (int) Math.pow(PYRAMID_SCALE, resolution);
      int scaledWidth = sizeX / scale;
      int scaledHeight = sizeY / scale;
      int scaledDepth = sizeZ;

      workingReader = readers.take();
      try {
        if (workingReader.getResolutionCount() > 1
            && reuseExistingResolutions)
        {
          workingReader.setResolution(resCounter);
          scaledWidth = workingReader.getSizeX();
          scaledHeight = workingReader.getSizeY();
          scaledDepth = workingReader.getSizeZ();
        }
      }
      finally {
        readers.put(workingReader);
      }

      resTileCounts[resCounter] =
          (int) Math.ceil((double) scaledWidth / tileWidth)
          * (int) Math.ceil((double) scaledHeight / tileHeight)
          * (int) Math.ceil((double) scaledDepth / chunkDepth)
          * (imageCount / sizeZ);
    }
    return resTileCounts;
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
   * Create a new Memoizer that wraps the given reader, based on the
   * user-specified caching options. The supplied reader may be null.
   *
   * @param reader existing reader to wrap, or null
   * @return new uninitialized Memoizer instance
   */
  private Memoizer createMemoizer(IFormatReader reader) {
    if (memoDirectory == null) {
      if (reader == null) {
        return new Memoizer();
      }
      return new Memoizer(reader);
    }
    else {
      if (reader == null) {
        return new Memoizer(
          Memoizer.DEFAULT_MINIMUM_ELAPSED, memoDirectory);
      }
      else {
        return new Memoizer(
          reader, Memoizer.DEFAULT_MINIMUM_ELAPSED, memoDirectory);
      }
    }
  }

  /**
   * Get the root path of the dataset, which will contain Zarr and OME-XML.
   * By default, this is the specified output directory.
   * If "--pyramid-name" was used, then this will be the pyramid name
   * as a subdirectory of the output directory.
   *
   * @return directory into which Zarr and OME-XML data is written
   */
  private Path getRootPath() {
    if (pyramidName == null) {
      return outputPath;
    }
    return outputPath.resolve(pyramidName);
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
      args.add(index.getRowPath());
      args.add(index.getColumnPath());
      args.add(index.getFieldIndex());
      args.add(resolution);
    }
    else {
      // if a single series is written,
      // the output series index should always be 0
      args.add(seriesList.indexOf(series));
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

  /**
   * Return the number of bytes per pixel for a JZarr data type.
   * @param dataType type to return number of bytes per pixel for
   * @return See above.
   */
  public static int bytesPerPixel(DataType dataType) {
    switch (dataType) {
      case i1:
      case u1:
        return 1;
      case i2:
      case u2:
        return 2;
      case i4:
      case u4:
      case f4:
        return 4;
      case f8:
        return 8;
      default:
        throw new IllegalArgumentException(
            "Unsupported data type: " + dataType);
    }
  }

  /**
   * Read tile as bytes from typed Zarr array.
   * @param zArray Zarr array to read from
   * @param shape array describing the number of elements in each dimension to
   * be read
   * @param offset array describing the offset in each dimension at which to
   * begin reading
   * @return tile data as bytes of size <code>shape * bytesPerPixel</code>
   * read from <code>offset</code>.
   * @throws IOException
   * @throws InvalidRangeException
   */
  public static byte[] readAsBytes(ZarrArray zArray, int[] shape, int[] offset)
      throws IOException, InvalidRangeException
  {
    DataType dataType = zArray.getDataType();
    int bytesPerPixel = bytesPerPixel(dataType);
    int size = IntStream.of(shape).reduce((a, b) -> a * b).orElse(0);
    byte[] tileAsBytes = new byte[size * bytesPerPixel];
    ByteBuffer tileAsByteBuffer = ByteBuffer.wrap(tileAsBytes);
    switch (dataType) {
      case i1:
      case u1: {
        zArray.read(tileAsBytes, shape, offset);
        break;
      }
      case i2:
      case u2: {
        short[] tileAsShorts = new short[size];
        zArray.read(tileAsShorts, shape, offset);
        tileAsByteBuffer.asShortBuffer().put(tileAsShorts);
        break;
      }
      case i4:
      case u4: {
        int[] tileAsInts = new int[size];
        zArray.read(tileAsInts, shape, offset);
        tileAsByteBuffer.asIntBuffer().put(tileAsInts);
        break;
      }
      case f4: {
        float[] tileAsFloats = new float[size];
        zArray.read(tileAsFloats, shape, offset);
        tileAsByteBuffer.asFloatBuffer().put(tileAsFloats);
        break;
      }
      case f8: {
        double[] tileAsDoubles = new double[size];
        zArray.read(tileAsDoubles, shape, offset);
        tileAsByteBuffer.asDoubleBuffer().put(tileAsDoubles);
        break;
      }
      default:
        throw new IllegalArgumentException(
            "Unsupported data type: " + dataType);
    }
    return tileAsBytes;
  }

  /**
   * Write tile as bytes to typed Zarr array.
   * @param zArray Zarr array to write to
   * @param shape array describing the number of elements in each dimension to
   * be written
   * @param offset array describing the offset in each dimension at which to
   * begin writing
   * @param tile data as bytes of size <code>shape * bytesPerPixel</code> to be
   * written at <code>offset</code>
   * @throws IOException
   * @throws InvalidRangeException
   */
  private static void writeBytes(
      ZarrArray zArray, int[] shape, int[] offset, ByteBuffer tile)
          throws IOException, InvalidRangeException
  {
    int size = IntStream.of(shape).reduce((a, b) -> a * b).orElse(0);
    DataType dataType = zArray.getDataType();
    Slf4JStopWatch t1 = stopWatch();
    try {
      switch (dataType) {
        case i1:
        case u1: {
          zArray.write(tile.array(), shape, offset);
          break;
        }
        case i2:
        case u2: {
          short[] tileAsShorts = new short[size];
          tile.asShortBuffer().get(tileAsShorts);
          zArray.write(tileAsShorts, shape, offset);
          break;
        }
        case i4:
        case u4: {
          int[] tileAsInts = new int[size];
          tile.asIntBuffer().get(tileAsInts);
          zArray.write(tileAsInts, shape, offset);
          break;
        }
        case f4: {
          float[] tileAsFloats = new float[size];
          tile.asFloatBuffer().get(tileAsFloats);
          zArray.write(tileAsFloats, shape, offset);
          break;
        }
        case f8: {
          double[] tileAsDoubles = new double[size];
          tile.asDoubleBuffer().put(tileAsDoubles);
          zArray.write(tileAsDoubles, shape, offset);
          break;
        }
        default:
          throw new IllegalArgumentException(
              "Unsupported data type: " + dataType);
      }
    }
    finally {
      t1.stop("writeBytes");
    }
  }

  private byte[] getTileDownsampled(
      int series, int resolution, int plane, int xx, int yy,
      int width, int height)
          throws FormatException, IOException, InterruptedException,
                 EnumerationException, InvalidRangeException
  {
    final String pathName =
        String.format(scaleFormatString,
            getScaleFormatStringArgs(series, resolution - 1));
    final ZarrArray zarr = ZarrArray.open(getRootPath().resolve(pathName));
    int[] dimensions = zarr.getShape();
    int[] blockSizes = zarr.getChunks();
    int activeTileWidth = blockSizes[blockSizes.length - 1];
    int activeTileHeight = blockSizes[blockSizes.length - 2];

    // Upscale our base X and Y offsets, and sizes to the previous resolution
    // based on the pyramid scaling factor
    xx *= PYRAMID_SCALE;
    yy *= PYRAMID_SCALE;
    width = (int) Math.min(
        activeTileWidth * PYRAMID_SCALE,
        dimensions[dimensions.length - 1] - xx);
    height = (int) Math.min(
        activeTileHeight * PYRAMID_SCALE,
        dimensions[dimensions.length - 2] - yy);

    IFormatReader reader = readers.take();
    int[] offset;
    try {
      offset = getOffset(reader, xx, yy, plane);
    }
    finally {
      readers.put(reader);
    }

    int bytesPerPixel = FormatTools.getBytesPerPixel(pixelType);
    int[] shape = new int[] {1, 1, 1, height, width};
    byte[] tileAsBytes = readAsBytes(zarr, shape, offset);

    if (downsampling == Downsampling.SIMPLE) {
      return scaler.downsample(tileAsBytes, width, height,
        PYRAMID_SCALE, bytesPerPixel, false,
        FormatTools.isFloatingPoint(pixelType),
        1, false);
    }

    return OpenCVTools.downsample(
      tileAsBytes, pixelType, width, height, PYRAMID_SCALE, downsampling);
  }

  private byte[] getTile(
      int series, int resolution, int plane, int xx, int yy,
      int width, int height)
          throws FormatException, IOException, InterruptedException,
                 EnumerationException, InvalidRangeException
  {
    IFormatReader reader = readers.take();
    try {
      if (reader.getResolutionCount() > 1 && reuseExistingResolutions) {
        reader.setResolution(resolution);
        return reader.openBytes(plane, xx, yy, width, height);
      }
    }
    finally {
      readers.put(reader);
    }
    if (resolution == 0) {
      reader = readers.take();
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
   * @param scaledDepth size of the Z dimension at the current resolution
   * @return dimension array ready for use with Zarr
   * @throws EnumerationException
   */
  private int[] getDimensions(
    IFormatReader reader, int scaledWidth, int scaledHeight, int scaledDepth)
      throws EnumerationException
  {
    int sizeZ = reader.getSizeZ();
    int sizeC = reader.getSizeC();
    int sizeT = reader.getSizeT();
    String o = new StringBuilder(
        dimensionOrder != null? dimensionOrder.toString()
        : reader.getDimensionOrder()).reverse().toString();
    int[] dimensions = new int[] {0, 0, scaledDepth, scaledHeight, scaledWidth};
    dimensions[o.indexOf("Z")] = sizeZ;
    dimensions[o.indexOf("C")] = sizeC;
    dimensions[o.indexOf("T")] = sizeT;
    return dimensions;
  }

  /**
   * Retrieve the offset based on either the configured or input file
   * dimension order at the current resolution.
   * @param reader initialized reader for the input file
   * @param x X position at the current resolution
   * @param y Y position at the current resolution
   * @param plane current plane being operated upon
   * @return offsets array ready to use
   * @throws EnumerationException
   */
  private int[] getOffset(
    IFormatReader reader, int x, int y, int plane) throws EnumerationException
  {
    String o = new StringBuilder(
        dimensionOrder != null? dimensionOrder.toString()
        : reader.getDimensionOrder()).reverse().toString();
    int[] zct = reader.getZCTCoords(plane);
    int[] offset = new int[] {0, 0, 0, y, x};
    offset[o.indexOf("Z")] = zct[0];
    offset[o.indexOf("C")] = zct[1];
    offset[o.indexOf("T")] = zct[2];
    return offset;
  }

  private void processChunk(int series, int resolution, int plane,
      int[] offset, int[] shape)
        throws EnumerationException, FormatException, IOException,
          InterruptedException, InvalidRangeException
  {
    String pathName =
        String.format(scaleFormatString,
            getScaleFormatStringArgs(series, resolution));
    final ZarrArray zarr = ZarrArray.open(getRootPath().resolve(pathName));
    IFormatReader reader = readers.take();
    boolean littleEndian = reader.isLittleEndian();
    int bpp = FormatTools.getBytesPerPixel(reader.getPixelType());
    int[] zct;
    ByteArrayOutputStream chunkAsBytes = new ByteArrayOutputStream();
    int zOffset;
    int zShape;
    try {
      LOGGER.info("requesting tile to write at {} to {}", offset, pathName);
      //Get coords of current series
      zct = reader.getZCTCoords(plane);
      String o = new StringBuilder(
          dimensionOrder != null? dimensionOrder.toString()
          : reader.getDimensionOrder()).reverse().toString();
      zOffset = offset[o.indexOf("Z")];
      zShape = shape[o.indexOf("Z")];
    }
    finally {
      readers.put(reader);
    }
    getProgressListener().notifyChunkStart(
      plane, offset[4], offset[3], zOffset);
    Slf4JStopWatch t0 = new Slf4JStopWatch("getChunk");
    try {
      for (int z = zOffset; z < zOffset + zShape; z++) {
        //Get plane index for current Z
        reader = readers.take();
        int planeIndex;
        try {
          planeIndex = FormatTools.getIndex(reader, z, zct[1], zct[2]);
        }
        finally {
          readers.put(reader);
        }
        byte[] tileAsBytes = getTile(series, resolution, planeIndex,
                                    offset[4], offset[3], shape[4], shape[3]);
        if (tileAsBytes == null) {
          return;
        }
        chunkAsBytes.write(tileAsBytes);
      }
    }
    finally {
      nTile.incrementAndGet();
      LOGGER.info("chunk read complete {}/{}", nTile.get(), tileCount);
      t0.stop();
    }
    ByteBuffer tileBuffer = ByteBuffer.wrap(chunkAsBytes.toByteArray());
    if (resolution == 0 && bpp > 1) {
      tileBuffer.order(
        littleEndian ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
    }
    writeBytes(zarr, shape, offset, tileBuffer);
    getProgressListener().notifyChunkEnd(plane, offset[4], offset[3], zOffset);
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
    int[] resTileCounts = tileCounts.get(series);
    int resolutions = resTileCounts.length;
    int seriesTiles = 0;
    for (int t : resTileCounts) {
      seriesTiles += t;
    }
    getProgressListener().notifySeriesStart(series, resolutions, seriesTiles);

    IFormatReader workingReader = readers.take();
    int sizeX;
    int sizeY;
    int sizeZ;
    int sizeT;
    int sizeC;
    int imageCount;
    String readerDimensionOrder;
    try {
      // calculate a reasonable pyramid depth if not specified as an argument
      sizeX = workingReader.getSizeX();
      sizeY = workingReader.getSizeY();
      sizeZ = workingReader.getSizeZ();
      sizeT = workingReader.getSizeT();
      sizeC = workingReader.getSizeC();
      readerDimensionOrder = workingReader.getDimensionOrder();
      imageCount = workingReader.getImageCount();
      pixelType = workingReader.getPixelType();
    }
    finally {
      readers.put(workingReader);
    }

    LOGGER.info(
      "Preparing to write pyramid sizeX {} (tileWidth: {}) " +
      "sizeY {} (tileWidth: {}) sizeZ {} (tileDepth: {}) imageCount {}",
        sizeX, tileWidth, sizeY, tileHeight, sizeZ, chunkDepth, imageCount
    );

    // series level metadata
    // do this after pixels are written, otherwise
    // min/max calculation won't have been performed
    setSeriesLevelMetadata(series, resolutions);

    for (int resCounter=0; resCounter<resolutions; resCounter++) {
      final int resolution = resCounter;
      int scale = (int) Math.pow(PYRAMID_SCALE, resolution);
      int scaledWidth = sizeX / scale;
      int scaledHeight = sizeY / scale;
      int scaledDepth = sizeZ;

      workingReader = readers.take();
      try {
        if (workingReader.getResolutionCount() > 1
            && reuseExistingResolutions)
        {
          workingReader.setResolution(resCounter);
          scaledWidth = workingReader.getSizeX();
          scaledHeight = workingReader.getSizeY();
          scaledDepth = workingReader.getSizeZ();
        }
      }
      finally {
        readers.put(workingReader);
      }

      int activeTileWidth = tileWidth;
      int activeTileHeight = tileHeight;
      int activeChunkDepth = chunkDepth;
      if (scaledWidth < activeTileWidth) {
        LOGGER.info("Reducing active tileWidth to {}", scaledWidth);
        activeTileWidth = scaledWidth;
      }

      if (scaledHeight < activeTileHeight) {
        LOGGER.info("Reducing active tileHeight to {}", scaledHeight);
        activeTileHeight = scaledHeight;
      }

      if (scaledDepth < activeChunkDepth) {
        LOGGER.warn("Reducing active chunkDepth to {}", scaledDepth);
        activeChunkDepth = scaledDepth;
      }

      DataType dataType = getZarrType(pixelType);
      String resolutionString = String.format(
              scaleFormatString, getScaleFormatStringArgs(series, resolution));
      ArrayParams arrayParams = new ArrayParams()
          .shape(getDimensions(
              workingReader, scaledWidth, scaledHeight, scaledDepth))
          .chunks(new int[] {1, 1, activeChunkDepth, activeTileHeight,
            activeTileWidth})
          .dataType(dataType)
          .dimensionSeparator(getDimensionSeparator())
          .compressor(CompressorFactory.create(
              compressionType.toString(), compressionProperties));
      ZarrArray.create(getRootPath().resolve(resolutionString), arrayParams);

      nTile = new AtomicInteger(0);
      tileCount = resTileCounts[resolution];

      List<CompletableFuture<Void>> futures =
        new ArrayList<CompletableFuture<Void>>();

      getProgressListener().notifyResolutionStart(resolution, tileCount);

      try {
        for (int j=0; j<scaledHeight; j+=tileHeight) {
          final int yy = j;
          int height = (int) Math.min(tileHeight, scaledHeight - yy);
          for (int k=0; k<scaledWidth; k+=tileWidth) {
            final int xx = k;
            int width = (int) Math.min(tileWidth, scaledWidth - xx);
            for (int l=0; l<scaledDepth; l+=chunkDepth) {
              final int zz = l;
              int depth = (int) Math.min(chunkDepth, scaledDepth - zz);
              for (int cc=0; cc<sizeC; cc++) {
                for (int tt=0; tt<sizeT; tt++) {
                  final int plane = FormatTools.getIndex(readerDimensionOrder,
                      sizeZ, sizeC, sizeT, imageCount, zz, cc, tt);

                  CompletableFuture<Void> future =
                       new CompletableFuture<Void>();
                  futures.add(future);
                  executor.execute(() -> {
                    try {
                      int[] shape = {1, 1, 1, height, width};
                      int[] offset;
                      IFormatReader reader = readers.take();
                      try {
                        String o = new StringBuilder(
                            dimensionOrder != null? dimensionOrder.toString()
                            : reader.getDimensionOrder()).reverse().toString();
                        shape[o.indexOf("Z")] = depth;
                        offset = getOffset(reader, xx, yy, plane);
                      }
                      finally {
                        readers.put(reader);
                      }
                      processChunk(series, resolution, plane, offset, shape);
                      LOGGER.info(
                          "Successfully processed chunk; resolution={} plane={}"
                          + " xx={} yy={} zz={} width={} height={} depth={}",
                          resolution, plane, xx, yy, zz, width, height, depth);
                      future.complete(null);
                    }
                    catch (Throwable t) {
                      future.completeExceptionally(t);
                      LOGGER.error(
                        "Failure processing chunk; resolution={} plane={} " +
                        "xx={} yy={} zz={} width={} height={} depth={}",
                        resolution, plane, xx, yy, zz, width, height, depth, t);
                    }
                  });
                }
              }
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
      finally {
        getProgressListener().notifyResolutionEnd(resolution);
      }
    }

    // series level metadata
    // do this after pixels are written, otherwise
    // min/max calculation won't have been performed
    setSeriesLevelMetadata(series, resolutions);
    getProgressListener().notifySeriesEnd(series);
  }

  private void saveHCSMetadata(IMetadata meta) throws IOException {
    if (noHCS) {
      LOGGER.debug("skipping HCS metadata");
      return;
    }
    LOGGER.debug("saving HCS metadata");

    // assumes only one plate defined
    Path rootPath = getRootPath();
    ZarrGroup root = ZarrGroup.open(rootPath);
    int plate = 0;
    Map<String, Object> plateMap = new HashMap<String, Object>();

    plateMap.put("name", meta.getPlateName(plate));

    List<Map<String, Object>> columns = new ArrayList<Map<String, Object>>();
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();

    // try to set plate dimensions based upon Plate.Rows/Plate.Columns
    // if not possible, use well data
    PositiveInteger plateRows =  meta.getPlateRows(plate);
    if (plateRows == null) {
      plateRows = new PositiveInteger(1);
      for (int wellIndex=0; wellIndex<meta.getWellCount(plate); wellIndex++) {
        plateRows = new PositiveInteger(Math.max(
            meta.getWellRow(plate, wellIndex).getNumberValue().intValue(),
            plateRows.getNumberValue().intValue()));
      }
    }
    for (int r=0; r<plateRows.getValue(); r++) {
      Map<String, Object> row = new HashMap<String, Object>();
      String rowName = HCSIndex.getRowName(r);
      row.put("name", rowName);
      rows.add(row);
    }
    PositiveInteger plateColumns = meta.getPlateColumns(plate);
    if (plateColumns == null) {
      plateColumns = new PositiveInteger(1);
      for (int wellIndex=0; wellIndex<meta.getWellCount(plate); wellIndex++) {
        plateColumns = new PositiveInteger(Math.max(
            meta.getWellColumn(plate, wellIndex).getNumberValue().intValue(),
            plateColumns.getNumberValue().intValue()));
      }
    }
    for (int c=0; c<plateColumns.getValue(); c++) {
      Map<String, Object> column = new HashMap<String, Object>();
      String columnName = HCSIndex.getColumnName(c);
      column.put("name", columnName);
      columns.add(column);
    }

    List<Map<String, Object>> acquisitions =
      new ArrayList<Map<String, Object>>();
    for (int pa=0; pa<meta.getPlateAcquisitionCount(plate); pa++) {
      Map<String, Object> acquisition = new HashMap<String, Object>();
      acquisition.put("id", pa);
      acquisitions.add(acquisition);
    }
    if (acquisitions.size() > 0) {
      plateMap.put("acquisitions", acquisitions);
    }

    List<Map<String, Object>> wells = new ArrayList<Map<String, Object>>();
    int maxField = Integer.MIN_VALUE;
    for (HCSIndex index : hcsIndexes) {
      if (index.getPlateIndex() == plate) {
        if (index.getFieldIndex() == 0) {
          String wellPath = index.getWellPath();

          Map<String, Object> well = new HashMap<String, Object>();
          well.put("path", wellPath);

          List<Map<String, Object>> imageList =
            new ArrayList<Map<String, Object>>();
          String rowPath = index.getRowPath();
          ZarrGroup rowGroup = root.createSubGroup(rowPath);
          String columnPath = index.getColumnPath();
          ZarrGroup columnGroup = rowGroup.createSubGroup(columnPath);
          for (HCSIndex field : hcsIndexes) {
            if (field.getPlateIndex() == index.getPlateIndex() &&
              field.getWellRowIndex() == index.getWellRowIndex() &&
              field.getWellColumnIndex() == index.getWellColumnIndex())
            {
              Map<String, Object> image = new HashMap<String, Object>();
              Integer plateAcq = field.getPlateAcquisitionIndex();
              if (plateAcq != null) {
                image.put("acquisition", plateAcq);
              }
              image.put("path", String.valueOf(field.getFieldIndex()));
              imageList.add(image);
            }
          }

          Map<String, Object> wellMap = new HashMap<String, Object>();
          wellMap.put("images", imageList);
          Map<String, Object> attributes = columnGroup.getAttributes();
          attributes.put("well", wellMap);
          columnGroup.writeAttributes(attributes);

          // make sure the row/column indexes are added to the plate attributes
          // this is necessary when Plate.Rows or Plate.Columns is not set
          String column = index.getColumnPath();
          String row = index.getRowPath();

          int columnIndex = -1;
          for (int c=0; c<columns.size(); c++) {
            if (columns.get(c).get("name").equals(column)) {
              columnIndex = c;
              break;
            }
          }
          if (columnIndex < 0) {
            Map<String, Object> colMap = new HashMap<String, Object>();
            colMap.put("name", column);
            columnIndex = columns.size();
            columns.add(colMap);
          }

          int rowIndex = -1;
          for (int r=0; r<rows.size(); r++) {
            if (rows.get(r).get("name").equals(row)) {
              rowIndex = r;
              break;
            }
          }
          if (rowIndex < 0) {
            Map<String, Object> rowMap = new HashMap<String, Object>();
            rowMap.put("name", row);
            rowIndex = rows.size();
            rows.add(rowMap);
          }

          well.put("rowIndex", rowIndex);
          well.put("columnIndex", columnIndex);
          wells.add(well);
        }

        maxField = (int) Math.max(maxField, index.getFieldIndex());
      }
    }
    plateMap.put("wells", wells);
    plateMap.put("columns", columns);
    plateMap.put("rows", rows);

    plateMap.put("field_count", maxField + 1);
    plateMap.put("version", NGFF_VERSION);

    Map<String, Object> attributes = root.getAttributes();
    attributes.put("plate", plateMap);
    root.writeAttributes(attributes);
  }

  /**
   * Use {@link ZarrArray#writeAttributes(Map)}
   * to attach the multiscales metadata to the group containing
   * the pyramids.
   *
   * @param series Series which is currently being written.
   * @param resolutions Total number of resolutions from which
   *                    names will be generated.
   * @throws IOException
   * @throws InterruptedException
   */
  private void setSeriesLevelMetadata(int series, int resolutions)
      throws IOException, InterruptedException
  {
    LOGGER.debug("setSeriesLevelMetadata({}, {})", series, resolutions);
    String resolutionString = String.format(
            scaleFormatString, getScaleFormatStringArgs(series, 0));
    if (resolutionString.endsWith("/")) {
      resolutionString = resolutionString.substring(
        0, resolutionString.length() - 1);
    }
    String seriesString = "";
    if (resolutionString.indexOf('/') >= 0) {
      seriesString = resolutionString.substring(0,
          resolutionString.lastIndexOf('/'));
    }
    LOGGER.debug("  seriesString = {}", seriesString);
    LOGGER.debug("  resolutionString = {}", resolutionString);
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
    multiscale.put("version", nested ? NGFF_VERSION : "0.1");
    multiscales.add(multiscale);

    IFormatReader v = null;
    IMetadata meta = null;
    String axisOrder = null;
    try {
      v = readers.take();
      meta = (IMetadata) v.getMetadataStore();

      if (dimensionOrder != null) {
        axisOrder = dimensionOrder.toString();
      }
      else {
        axisOrder = v.getDimensionOrder();
      }
    }
    finally {
      readers.put(v);
    }

    List<Map<String, Object>> datasets = new ArrayList<Map<String, Object>>();
    for (int r = 0; r < resolutions; r++) {
      resolutionString = String.format(
              scaleFormatString, getScaleFormatStringArgs(series, r));

      // calculate the relative path to this resolution
      String lastPath = resolutionString;
      int lastFileSeparator = lastPath.lastIndexOf('/');
      if (lastFileSeparator == lastPath.length() - 1) {
        // if there is a trailing slash, remove it and recalculate
        // the last file separator index
        lastPath = lastPath.substring(0, lastFileSeparator);
        lastFileSeparator = lastPath.lastIndexOf('/');
      }
      lastPath = lastPath.substring(lastFileSeparator + 1);

      List<Map<String, Object>> transforms =
        new ArrayList<Map<String, Object>>();
      Map<String, Object> scale = new HashMap<String, Object>();
      scale.put("type", "scale");
      List<Double> axisValues = new ArrayList<Double>();
      double resolutionScale = Math.pow(PYRAMID_SCALE, r);
      for (int i=axisOrder.length()-1; i>=0; i--) {
        Quantity axisScale = getScale(meta, series, axisOrder, i);
        String axisChar = axisOrder.substring(i, i + 1).toLowerCase();

        if (axisScale != null) {
          // if physical dimension information is defined,
          // use it directly for dimensions that aren't scaled (Z and T)
          // increase it according to the resolution number for dimensions that
          // are scaled (X and Y)
          if (axisChar.equals("x") || axisChar.equals("y")) {
            axisValues.add(axisScale.value().doubleValue() * resolutionScale);
          }
          else {
            axisValues.add(axisScale.value().doubleValue());
          }
        }
        else {
          // if physical dimension information is not defined,
          // store the scale factor for the dimension in the current resolution,
          // i.e. 1.0 for everything other than X and Y
          if (axisChar.equals("x") || axisChar.equals("y")) {
            axisValues.add(resolutionScale);
          }
          else {
            axisValues.add(1.0);
          }
        }
      }
      scale.put("scale", axisValues);

      transforms.add(scale);

      Map<String, Object> dataset = new HashMap<String, Object>();
      dataset.put("path", lastPath);
      dataset.put("coordinateTransformations", transforms);
      datasets.add(dataset);
    }
    multiscale.put("datasets", datasets);

    List<Map<String, String>> axes = new ArrayList<Map<String, String>>();
    for (int i=axisOrder.length()-1; i>=0; i--) {
      String axis = axisOrder.substring(i, i + 1).toLowerCase();
      String type = "space";
      Quantity scale = getScale(meta, series, axisOrder, i);
      if (axis.equals("t")) {
        type = "time";
      }
      else if (axis.equals("c")) {
        type = "channel";
      }
      Map<String, String> thisAxis = new HashMap<String, String>();
      thisAxis.put("name", axis);
      thisAxis.put("type", type);
      if (scale != null) {
        String symbol = scale.unit().getSymbol();
        String unitName = null;
        try {
          if (scale instanceof Length) {
            unitName = UnitsLength.fromString(symbol).name().toLowerCase();
          }
          else if (scale instanceof Time) {
            unitName = UnitsTime.fromString(symbol).name().toLowerCase();
          }
        }
        catch (EnumerationException e) {
          LOGGER.warn("Could not identify unit '{}'", symbol);
        }
        thisAxis.put("unit", unitName);
      }
      axes.add(thisAxis);
    }
    multiscale.put("axes", axes);

    int seriesIndex = seriesList.indexOf(series);
    String name = meta.getImageName(seriesIndex);
    if (name == null) {
      name = "Series " + seriesIndex;
    }
    multiscale.put("name", name);

    Path subGroupPath = getRootPath().resolve(seriesString);
    LOGGER.debug("  creating subgroup {}", subGroupPath);
    ZarrGroup subGroup = ZarrGroup.create(subGroupPath);
    Map<String, Object> attributes = new HashMap<String, Object>();
    attributes.put("multiscales", multiscales);

    if (omeroMetadata) {
      Map<String, Object> omero = new HashMap<String, Object>();

      int channelCount = meta.getChannelCount(seriesIndex);
      boolean colorRender = channelCount > 1 && channelCount < 8;

      Map<String, Object> rdefs = new HashMap<String, Object>();
      rdefs.put("defaultT", 0);
      rdefs.put("defaultZ", meta.getPixelsSizeZ(seriesIndex).getValue() / 2);
      rdefs.put("model", colorRender ? "color" : "greyscale");
      omero.put("rdefs", rdefs);

      double[] defaultMinMax =
        getRange(FormatTools.pixelTypeFromString(
          meta.getPixelsType(seriesIndex).toString()));

      OMEXMLMetadata omexml = (OMEXMLMetadata) meta;
      omexml.resolveReferences();

      List<Map<String, Object>> channels = new ArrayList<Map<String, Object>>();
      for (int c=0; c<channelCount; c++) {
        Map<String, Object> channel = new HashMap<String, Object>();
        channel.put("active", c < 3);
        channel.put("coefficient", 1);

        // set an RGB color (alpha removed)
        Color color = Colors.getColor(omexml, seriesIndex, c);
        Integer packedColor = (color.getValue() >> 8) & 0xffffff;
        String formattedColor = String.format("%06X", packedColor);
        channel.put("color", formattedColor);

        channel.put("family", "linear");
        channel.put("inverted", false);
        channel.put("label", getChannelName(omexml, seriesIndex, c));
        Map<String, Object> window = new HashMap<String, Object>();

        final int channelIndex = c;
        Double[] totalMin = new Double[1];
        Double[] totalMax = new Double[1];

        readers.forEach((minmax) -> {
          try {
            if (!(minmax instanceof MinMaxCalculator)) {
              LOGGER.error("Cannot set OMERO min/max data");
            }

            MinMaxCalculator calc = (MinMaxCalculator) minmax;
            Double min = calc.getChannelKnownMinimum(channelIndex);
            Double max = calc.getChannelKnownMaximum(channelIndex);

            if (min != null && min < Double.POSITIVE_INFINITY) {
              if (totalMin[0] == null || min < totalMin[0]) {
                totalMin[0] = min;
              }
            }
            if (max != null && max > Double.NEGATIVE_INFINITY) {
              if (totalMax[0] == null || max > totalMax[0]) {
                totalMax[0] = max;
              }
            }
          }
          catch (FormatException|IOException e) {
            LOGGER.error("Cannot set OMERO min/max data", e);
          }
        });

        if (totalMin[0] == null && defaultMinMax != null) {
          totalMin[0] = defaultMinMax[0];
        }
        if (totalMax[0] == null && defaultMinMax != null) {
          totalMax[0] = defaultMinMax[1];
        }

        window.put("start", totalMin[0]);
        window.put("end", totalMax[0]);
        window.put("min", totalMin[0]);
        window.put("max", totalMax[0]);
        channel.put("window", window);

        channels.add(channel);
      }

      omero.put("channels", channels);

      attributes.put("omero", omero);
    }

    subGroup.writeAttributes(attributes);
    LOGGER.debug("    finished writing subgroup attributes");
  }

  private Quantity getScale(
    IMetadata meta, int series, String axisOrder, int axis)
  {
    if (meta == null) {
      return null;
    }
    int seriesIndex = seriesList.indexOf(series);

    if (seriesIndex < 0 || seriesIndex >= meta.getImageCount()) {
      return null;
    }

    String axisChar = axisOrder.substring(axis, axis + 1).toLowerCase();
    switch (axisChar.charAt(0)) {
      case 'x':
        return meta.getPixelsPhysicalSizeX(seriesIndex);
      case 'y':
        return meta.getPixelsPhysicalSizeY(seriesIndex);
      case 'z':
        return meta.getPixelsPhysicalSizeZ(seriesIndex);
      case 't':
        return meta.getPixelsTimeIncrement(seriesIndex);
      default:
        return null;
    }
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
   * Get the minimum and maximum pixel values for the given pixel type.
   *
   * @param bfPixelType pixel type as defined in FormatTools
   * @return array of length 2 representing the minimum and maximum
   *         pixel values, or null if converting to the given type is
   *         not supported
   */
  private double[] getRange(int bfPixelType) {
    double[] range = new double[2];
    switch (bfPixelType) {
      case FormatTools.INT8:
        range[0] = -128.0;
        range[1] = 127.0;
        break;
      case FormatTools.UINT8:
        range[0] = 0.0;
        range[1] = 255.0;
        break;
      case FormatTools.INT16:
        range[0] = -32768.0;
        range[1] = 32767.0;
        break;
      case FormatTools.UINT16:
        range[0] = 0.0;
        range[1] = 65535.0;
        break;
      default:
        return null;
    }

    return range;
  }

  /**
   * Convert Bio-Formats pixel type to Zarr data type.
   *
   * @param type Bio-Formats pixel type
   * @return corresponding Zarr data type
   */
  public static DataType getZarrType(int type) {
    switch (type) {
      case FormatTools.INT8:
        return DataType.i1;
      case FormatTools.UINT8:
        return DataType.u1;
      case FormatTools.INT16:
        return DataType.i2;
      case FormatTools.UINT16:
        return DataType.u2;
      case FormatTools.INT32:
        return DataType.i4;
      case FormatTools.UINT32:
        return DataType.u4;
      case FormatTools.FLOAT:
        return DataType.f4;
      case FormatTools.DOUBLE:
        return DataType.f8;
      default:
        throw new IllegalArgumentException("Unsupported pixel type: "
            + FormatTools.getPixelTypeString(type));
    }
  }

  private DimensionSeparator getDimensionSeparator() {
    return nested ? DimensionSeparator.SLASH : DimensionSeparator.DOT;
  }

  private void checkOutputPaths() {
    if (pyramidName != null || !scaleFormatString.equals("%d/%d")) {
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

  /**
   * Check if the given metadata object contains at least one plate
   * with at least one well that links to an image.
   *
   * @param meta metadata object
   * @return true if a valid plate exists, false otherwise
   */
  private boolean hasValidPlate(IMetadata meta) {
    List<String> images = new ArrayList<String>();
    for (int i=0; i<meta.getImageCount(); i++) {
      images.add(meta.getImageID(i));
    }
    for (int p=0; p<meta.getPlateCount(); p++) {
      for (int w=0; w<meta.getWellCount(p); w++) {
        for (int ws=0; ws<meta.getWellSampleCount(p, w); ws++) {
          if (images.contains(meta.getWellSampleImageRef(p, w, ws))) {
            return true;
          }
        }
      }
      LOGGER.warn("Encountered invalid plate #{}", p);
    }
    return false;
  }

  private String getChannelName(OMEXMLMetadata meta,
    int series, int channelIndex)
  {
    String channelName = meta.getChannelName(series, channelIndex);
    if (channelName != null) {
      return channelName;
    }

    Length emission = meta.getChannelEmissionWavelength(series, channelIndex);
    Length excitation =
      meta.getChannelExcitationWavelength(series, channelIndex);

    OMEXMLMetadataRoot root = (OMEXMLMetadataRoot) meta.getRoot();
    Image img = root.getImage(series);
    Channel channel = img.getPixels().getChannel(channelIndex);
    LightPath path = channel.getLightPath();
    FilterSet filterSet = channel.getLinkedFilterSet();
    LightSourceSettings sourceSettings = channel.getLightSourceSettings();
    LightSource source = null;
    if (sourceSettings != null) {
      source = sourceSettings.getLightSource();
    }

    if (emission != null) {
      String name = getNameFromWavelength(emission);
      if (name != null) {
        return name;
      }
    }

    if (path != null) {
      List<Filter> emFilters = path.copyLinkedEmissionFilterList();
      for (Filter f : emFilters) {
        String name = getNameFromFilter(f);
        if (name != null) {
          return name;
        }
      }
    }

    if (filterSet != null) {
      List<Filter> emFilters = filterSet.copyLinkedEmissionFilterList();
      for (Filter f : emFilters) {
        String name = getNameFromFilter(f);
        if (name != null) {
          return name;
        }
      }
    }
    if (source != null && source instanceof Laser) {
      Laser laser = (Laser) source;
      if (laser.getWavelength() != null) {
        String name = getNameFromWavelength(laser.getWavelength());
        if (name != null) {
          return name;
        }
      }
    }
    if (excitation != null) {
      String name = getNameFromWavelength(excitation);
      if (name != null) {
        return name;
      }
    }

    if (path != null) {
      List<Filter> exFilters = path.copyLinkedExcitationFilterList();
      for (Filter f : exFilters) {
        String name = getNameFromFilter(f);
        if (name != null) {
          return name;
        }
      }
    }
    if (filterSet != null) {
      List<Filter> exFilters = filterSet.copyLinkedExcitationFilterList();
      for (Filter f : exFilters) {
        String name = getNameFromFilter(f);
        if (name != null) {
          return name;
        }
      }
    }
    return "Channel " + channelIndex;
  }

  private String getNameFromFilter(Filter f) {
    if (f == null) {
      return null;
    }
    TransmittanceRange range = f.getTransmittanceRange();
    if (range == null) {
      return null;
    }
    return String.valueOf(range.getCutIn().value());
  }

  private String getNameFromWavelength(Length v) {
    if (v == null) {
      return null;
    }
    double wave = v.value().doubleValue();
    if (DoubleMath.isMathematicalInteger(wave)) {
      return String.valueOf(wave);
    }
    return v.toString();
  }

  private int calculateResolutions(int width, int height) {
    int resolutions = 1;
    while (width > minSize || height > minSize) {
      resolutions++;
      width /= PYRAMID_SCALE;
      height /= PYRAMID_SCALE;
    }
    return resolutions;
  }

  private static Slf4JStopWatch stopWatch() {
    return new Slf4JStopWatch(LOGGER, Slf4JStopWatch.DEBUG_LEVEL);
  }

  /**
   * Set a listener for tile processing events.
   * Intended to be used to show a status bar.
   *
   * @param listener a progress event listener
   */
  public void setProgressListener(IProgressListener listener) {
    progressListener = listener;
  }

  /**
   * Get the currrent listener for tile processing events.
   * If no listener was set, a no-op listener is returned.
   *
   * @return the current progress listener
   */
  public IProgressListener getProgressListener() {
    if (progressListener == null) {
      setProgressListener(new NoOpProgressListener());
    }
    return progressListener;
  }

  /**
   * Perform file conversion as specified by command line arguments.
   * @param args command line arguments
   */
  public static void main(String[] args) {
    CommandLine.call(new Converter(), args);
  }

}
