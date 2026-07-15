/**
 * Copyright (c) 2026 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.bioformats2raw;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ConverterCliTest {

  @TempDir
  Path temporaryDirectory;

  @Test
  void helpIsSuccessfulAndUsesStdout() {
    Result result = execute("--help");

    assertEquals(0, result.exitCode);
    assertTrue(result.out.startsWith("Usage: bioformats2raw"));
    assertEquals("", result.err);
  }

  @Test
  void versionIsSuccessfulAndUsesStdout() {
    Result result = execute("--version");

    assertEquals(0, result.exitCode);
    assertTrue(result.out.startsWith("bioformats2raw "));
    assertTrue(result.out.contains("Bio-Formats version = "));
    assertTrue(result.out.contains("NGFF specification version = 0.4"));
    assertEquals("", result.err);
  }

  @Test
  void missingOperandsAreUsageErrors() {
    Result result = execute();

    assertEquals(2, result.exitCode);
    assertEquals("", result.out);
    assertTrue(result.err.contains("input path not specified"));
    assertTrue(result.err.contains("Try 'bioformats2raw --help'"));
    assertFalse(result.err.contains("\tat "));
  }

  @Test
  void unknownOptionIsAConciseUsageError() {
    Result result = execute("--not-an-option");

    assertEquals(2, result.exitCode);
    assertEquals("", result.out);
    assertTrue(result.err.contains("Unknown option: '--not-an-option'"));
    assertFalse(result.err.contains("Usage: bioformats2raw"));
  }

  @ParameterizedTest
  @MethodSource("invalidNumericOptions")
  void invalidNumericValuesAreUsageErrors(String option, String value) {
    Result result = execute(option, value, "input.fake", "output.zarr");

    assertEquals(2, result.exitCode);
    assertEquals("", result.out);
    assertTrue(result.err.contains("must be"));
    assertFalse(result.err.contains("\tat "));
  }

  static java.util.stream.Stream<Arguments> invalidNumericOptions() {
    return java.util.stream.Stream.of(
      Arguments.of("--resolutions", "0"),
      Arguments.of("--tile-width", "0"),
      Arguments.of("--tile-height", "-1"),
      Arguments.of("--chunk-depth", "0"),
      Arguments.of("--shard-width", "0"),
      Arguments.of("--shard-height", "0"),
      Arguments.of("--shard-depth", "0"),
      Arguments.of("--max-workers", "0"),
      Arguments.of("--max-cached-tiles", "-1"),
      Arguments.of("--target-min-size", "0"),
      Arguments.of("--fill-value", "256")
    );
  }

  @Test
  void invalidLogLevelAndReaderOptionAreUsageErrors() {
    Result logLevel = execute(
      "--log-level", "verbose", "input.fake", "output.zarr");
    Result readerOption = execute(
      "--options", "missing-value", "input.fake", "output.zarr");

    assertEquals(2, logLevel.exitCode);
    assertTrue(logLevel.err.contains("--log-level must be one of"));
    assertEquals(2, readerOption.exitCode);
    assertTrue(readerOption.err.contains("key=value"));
  }

  @Test
  void invalidExtraReaderIsAUsageError() {
    Result result = execute(
      "--extra-readers", "java.lang.String", "input.fake", "output.zarr");

    assertEquals(2, result.exitCode);
    assertTrue(result.err.contains("must implement IFormatReader"));
    assertFalse(result.err.contains("\tat "));
  }

  @Test
  void incompatibleCompressionIsAUsageError() {
    Result result = execute(
      "--compression", "gzip", "input.fake", "output.zarr");

    assertEquals(2, result.exitCode);
    assertTrue(result.err.contains("Zarr v2 does not support gzip"));
  }

  @ParameterizedTest
  @MethodSource("invalidCompressionProperties")
  void invalidCompressionPropertiesAreUsageErrors(
    String[] options, String expectedError)
  {
    String[] args = Arrays.copyOf(options, options.length + 2);
    args[options.length] = "input.fake";
    args[options.length + 1] = "output.zarr";

    Result result = execute(args);

    assertEquals(2, result.exitCode);
    assertEquals("", result.out);
    assertTrue(result.err.contains(expectedError));
    assertFalse(result.err.contains("\tat "));
  }

  static java.util.stream.Stream<Arguments> invalidCompressionProperties() {
    return java.util.stream.Stream.of(
      Arguments.of(
        new String[] {"-c", "blosc", "--compression-properties", "foo=1"},
        "unsupported blosc compression property: foo"),
      Arguments.of(
        new String[] {"-c", "blosc", "--compression-properties", "clevel=10"},
        "clevel must be between 0 and 9"),
      Arguments.of(
        new String[] {
          "-c", "blosc", "--compression-properties", "cname=snappy"},
        "invalid blosc cname: snappy"),
      Arguments.of(
        new String[] {
          "-c", "blosc", "--compression-properties", "shuffle=maybe"},
        "invalid blosc shuffle value: maybe"),
      Arguments.of(
        new String[] {"-c", "zlib", "--compression-properties", "level=fast"},
        "level must be an integer"),
      Arguments.of(
        new String[] {"-c", "zlib", "--compression-properties", "level=10"},
        "level must be between 0 and 9"),
      Arguments.of(
        new String[] {"--ngff-version", "0.5", "-c", "gzip",
          "--compression-properties", "level=10"},
        "level must be between 0 and 9"),
      Arguments.of(
        new String[] {"--ngff-version", "0.5", "-c", "zstd",
          "--compression-properties", "level=23"},
        "level must be between -7 and 22"),
      Arguments.of(
        new String[] {"--ngff-version", "0.5", "-c", "zstd",
          "--compression-properties", "checksum=yes"},
        "zstd checksum must be true or false")
    );
  }

  @Test
  void missingInputIsAnOperationalErrorWithoutStackTrace() {
    Path output = temporaryDirectory.resolve("missing-input-output");
    Result result = execute(
      temporaryDirectory.resolve("missing.tif").toString(), output.toString());

    assertEquals(1, result.exitCode);
    assertEquals("", result.out);
    assertTrue(result.err.contains("missing.tif"));
    assertFalse(result.err.contains("\tat "));
  }

  @Test
  void debugOperationalErrorIncludesStackTrace() {
    Path output = temporaryDirectory.resolve("debug-output");
    Result result = execute(
      temporaryDirectory.resolve("missing.tif").toString(), output.toString(),
      "--debug");

    assertEquals(1, result.exitCode);
    assertTrue(result.err.contains("java.io.FileNotFoundException"));
    assertTrue(result.err.contains("\tat "));
  }

  @Test
  void existingOutputIsAnOperationalError() throws Exception {
    Path output = Files.createDirectory(temporaryDirectory.resolve("existing"));
    Result result = execute("image.fake", output.toString());

    assertEquals(1, result.exitCode);
    assertTrue(result.err.contains("already exists"));
    assertFalse(result.err.contains("may be incomplete"));
  }

  @Test
  void overwritePreflightsInputBeforeDeletingOutput() throws Exception {
    Path output = Files.createDirectory(
      temporaryDirectory.resolve("preserved"));
    Path marker = Files.createFile(output.resolve("marker"));
    Result result = execute("--overwrite",
      temporaryDirectory.resolve("missing.tif").toString(), output.toString());

    assertEquals(1, result.exitCode);
    assertTrue(Files.exists(marker));
    assertFalse(result.err.contains("may be incomplete"));
  }

  @Test
  void overwritePreservesOutputWhenScaleFormatIsInvalid() throws Exception {
    Path output = Files.createDirectory(
      temporaryDirectory.resolve("invalid-format-preserved"));
    Path marker = Files.createFile(output.resolve("marker"));

    Result result = execute("--overwrite", "--scale-format-string", "%q",
      "image.fake", output.toString());

    assertEquals(2, result.exitCode);
    assertTrue(Files.exists(marker));
    assertFalse(Files.exists(output.resolve("OME")));
    assertFalse(result.err.contains("may be incomplete"));
  }

  @Test
  void overwriteStopsWhenAPathCannotBeDeleted() throws Exception {
    Assumptions.assumeTrue(
      FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
    Path output = Files.createDirectory(temporaryDirectory.resolve("readonly"));
    Path marker = Files.createFile(output.resolve("marker"));
    Set<PosixFilePermission> original =
      Files.getPosixFilePermissions(output);
    Files.setPosixFilePermissions(
      output, PosixFilePermissions.fromString("r-xr-xr-x"));
    try {
      Result result = execute("--overwrite", "image.fake", output.toString());

      assertEquals(1, result.exitCode);
      assertTrue(Files.exists(marker));
      assertTrue(result.err.contains("may be incomplete"));
    }
    finally {
      Files.setPosixFilePermissions(output, original);
    }
  }

  @Test
  void conversionFailureReportsPartialOutput() {
    Path output = temporaryDirectory.resolve("partial");
    Converter failing = new Converter() {
      @Override
      public void convert() throws java.io.IOException {
        prepareOutput();
        throw new java.io.IOException("simulated write failure");
      }
    };
    Result result = execute(failing, "image.fake", output.toString());

    assertEquals(1, result.exitCode);
    assertTrue(result.err.contains("simulated write failure"));
    assertTrue(result.err.contains(
      "output '" + output + "' may be incomplete"));
  }

  @Test
  void interruptionReturns130AndRestoresInterruptFlag() {
    Path output = temporaryDirectory.resolve("interrupted");
    Converter interrupted = new Converter() {
      @Override
      public void convert() throws InterruptedException {
        throw new InterruptedException("simulated interruption");
      }
    };
    try {
      Result result = execute(interrupted, "image.fake", output.toString());

      assertEquals(130, result.exitCode);
      assertTrue(Thread.currentThread().isInterrupted());
      assertTrue(result.err.contains("conversion interrupted"));
    }
    finally {
      Thread.interrupted();
    }
  }

  @Test
  void outOfRangeSeriesIsAUsageError() {
    Path output = temporaryDirectory.resolve("series-output");
    Result result = execute(
      "--series", "2", "image.fake", output.toString());

    assertEquals(2, result.exitCode);
    assertTrue(result.err.contains("outside the valid range"));
  }

  @Test
  void duplicateSeriesIsAUsageError() {
    Path output = temporaryDirectory.resolve("duplicate-series-output");
    Result result = execute(
      "--series", "0,0", "image.fake", output.toString());

    assertEquals(2, result.exitCode);
    assertTrue(result.err.contains(
      "series index 0 was specified more than once"));
  }

  @Test
  void incorrectAdditionalArgumentCsvRowCountIsAUsageError()
    throws Exception
  {
    Path csv = temporaryDirectory.resolve("additional-arguments.csv");
    Files.write(csv, Arrays.asList("first", "second"));
    Path output = temporaryDirectory.resolve("csv-row-count-output");

    Result result = execute(
      "--additional-scale-format-string-args", csv.toString(),
      "image.fake", output.toString());

    assertEquals(2, result.exitCode);
    assertTrue(result.err.contains("must contain exactly 1 rows"));
  }

  @Test
  void invalidScaleFormatIsAUsageError() {
    Path output = temporaryDirectory.resolve("format-output");
    Result result = execute(
      "--scale-format-string", "%q", "image.fake", output.toString());

    assertEquals(2, result.exitCode);
    assertTrue(result.err.contains("invalid --scale-format-string"));
  }

  @Test
  void publicSettersRejectInvalidValues() {
    Converter converter = new Converter();

    assertThrows(IllegalArgumentException.class,
      () -> converter.setTileWidth(0));
    assertThrows(IllegalArgumentException.class,
      () -> converter.setMaxWorkers(-1));
    assertThrows(IllegalArgumentException.class,
      () -> converter.setFillValue((short) 256));
    assertThrows(IllegalArgumentException.class,
      () -> converter.setSeriesList(Arrays.asList(0, -1)));
    assertThrows(IllegalArgumentException.class,
      () -> converter.setReaderOptions(Collections.singletonList("broken")));
    assertThrows(IllegalArgumentException.class,
      () -> converter.setExtraReaders(new Class<?>[] {String.class}));
  }

  private Result execute(String... args) {
    return execute(new Converter(), args);
  }

  private Result execute(Converter converter, String... args) {
    StringWriter out = new StringWriter();
    StringWriter err = new StringWriter();
    int exitCode = Converter.execute(converter, args,
      new PrintWriter(out, true), new PrintWriter(err, true));
    return new Result(exitCode, out.toString(), err.toString());
  }

  private static class Result {

    private final int exitCode;
    private final String out;
    private final String err;

    Result(int exitCode, String out, String err) {
      this.exitCode = exitCode;
      this.out = out;
      this.err = err;
    }
  }
}
