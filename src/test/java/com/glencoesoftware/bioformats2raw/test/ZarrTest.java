/**
 * Copyright (c) 2019 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.bioformats2raw.test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ShortBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.glencoesoftware.bioformats2raw.Converter;
import com.glencoesoftware.bioformats2raw.Downsampling;
import loci.common.LogbackTools;
import loci.common.services.ServiceFactory;
import loci.formats.FormatTools;
import loci.formats.in.FakeReader;
import loci.formats.ome.OMEXMLMetadata;
import loci.formats.services.OMEXMLService;
import picocli.CommandLine;
import picocli.CommandLine.ExecutionException;

import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.zarr.N5ZarrReader;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.opencv.core.Core;

public class ZarrTest {

  Path input;

  Path output;

  Converter converter;

  /**
   * Set logging to warn before all methods.
   *
   * @param tmp temporary directory for output file
   */
  @BeforeEach
  public void setup(@TempDir Path tmp) throws Exception {
    output = tmp.resolve("test");
    LogbackTools.setRootLevel("warn");
  }

  /**
   * Run the Converter main method and check for success or failure.
   *
   * @param additionalArgs CLI arguments as needed beyond "-o output input"
   */
  void assertTool(String...additionalArgs) throws IOException {
    List<String> args = new ArrayList<String>();
    for (String arg : additionalArgs) {
      args.add(arg);
    }
    args.add("--file_type=zarr");
    args.add(input.toString());
    args.add(output.toString());
    try {
      converter = new Converter();
      CommandLine.call(converter, args.toArray(new String[]{}));
      assertTrue(Files.exists(output.resolve("data.zarr")));
      assertTrue(Files.exists(output.resolve("METADATA.ome.xml")));
    }
    catch (RuntimeException rt) {
      throw rt;
    }
    catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  static Path fake(String...args) {
    assertTrue(args.length %2 == 0);
    Map<String, String> options = new HashMap<String, String>();
    for (int i = 0; i < args.length; i += 2) {
      options.put(args[i], args[i+1]);
    }
    return fake(options);
  }

  static Path fake(Map<String, String> options) {
    return fake(options, null);
  }

  /**
   * Create a Bio-Formats fake INI file to use for testing.
   * @param options map of the options to assign as part of the fake filename
   * from the allowed keys
   * @param series map of the integer series index and options map (same format
   * as <code>options</code> to add to the fake INI content
   * @see https://docs.openmicroscopy.org/bio-formats/6.4.0/developers/
   * generating-test-images.html#key-value-pairs
   * @return path to the fake INI file that has been created
   */
  static Path fake(Map<String, String> options,
          Map<Integer, Map<String, String>> series)
  {
    return fake(options, series, null);
  }

  static Path fake(Map<String, String> options,
          Map<Integer, Map<String, String>> series,
          Map<String, String> originalMetadata)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("image");
    if (options != null) {
      for (Map.Entry<String, String> kv : options.entrySet()) {
        sb.append("&");
        sb.append(kv.getKey());
        sb.append("=");
        sb.append(kv.getValue());
      }
    }
    sb.append("&");
    try {
      List<String> lines = new ArrayList<String>();
      if (originalMetadata != null) {
        lines.add("[GlobalMetadata]");
        for (String key : originalMetadata.keySet()) {
          lines.add(String.format("%s=%s", key, originalMetadata.get(key)));
        }
      }
      if (series != null) {
        for (int s : series.keySet()) {
          Map<String, String> seriesOptions = series.get(s);
          lines.add(String.format("[series_%d]", s));
          for (String key : seriesOptions.keySet()) {
            lines.add(String.format("%s=%s", key, seriesOptions.get(key)));
          }
        }
      }
      Path ini = Files.createTempFile(sb.toString(), ".fake.ini");
      File iniAsFile = ini.toFile();
      String iniPath = iniAsFile.getAbsolutePath();
      String fakePath = iniPath.substring(0, iniPath.length() - 4);
      Path fake = Paths.get(fakePath);
      File fakeAsFile = fake.toFile();
      Files.write(fake, new byte[]{});
      Files.write(ini, lines);
      iniAsFile.deleteOnExit();
      fakeAsFile.deleteOnExit();
      return ini;
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Test a fake file with default values smaller than
   * the default tile size (512 vs 1024).
   */
  @Test
  public void testDefaultIsTooBig() throws Exception {
    input = fake();
    assertTool();
  }

  /**
   * Test additional format string args.
   */
  @Test
  public void testAdditionalScaleFormatStringArgs() throws Exception {
    input = fake("series", "2");
    Path csv = Files.createTempFile(null, ".csv");
    Files.write(csv, Arrays.asList((new String[] {
      "abc,888,def",
      "ghi,999,jkl"
    })));
    csv.toFile().deleteOnExit();
    assertTool(
        "--scale-format-string", "%3$s/%4$s/%1$s/%2$s",
        "--additional-scale-format-string-args", csv.toString()
    );
    N5ZarrReader z =
            new N5ZarrReader(output.resolve("data.zarr").toString());
    assertTrue(z.exists("/abc/888/0/0"));
    assertTrue(z.exists("/ghi/999/1/0"));
  }

  /**
   * Test a fake file conversion and ensure the layout is set.
   */
  @Test
  public void testDefaultLayoutIsSet() throws Exception {
    input = fake();
    assertTool();
    N5ZarrReader z =
        new N5ZarrReader(output.resolve("data.zarr").toString());
    Integer layout =
        z.getAttribute("/", "bioformats2raw.layout", Integer.class);
    assertEquals(Converter.LAYOUT, layout);
  }

  /**
   * Test that multiscales metadata is present.
   */
  @Test
  public void testMultiscalesMetadata() throws Exception {
    input = fake();
    assertTool();
    N5ZarrReader z =
            new N5ZarrReader(output.resolve("data.zarr").toString());
    //
    List<Map<String, Object>> multiscales =
            z.getAttribute("/0", "multiscales", List.class);
    assertEquals(1, multiscales.size());
    Map<String, Object> multiscale = multiscales.get(0);
    assertEquals("0.1", multiscale.get("version"));
    List<Map<String, Object>> datasets =
            (List<Map<String, Object>>) multiscale.get("datasets");
    assertTrue(datasets.size() > 0);
    assertEquals("0", datasets.get(0).get("path"));
  }

  /**
   * Test alternative dimension order.
   */
  @Test
  public void testXYCZTDimensionOrder() throws Exception {
    input = fake("sizeC", "2", "dimOrder", "XYCZT");
    assertTool();
    N5ZarrReader z =
      new N5ZarrReader(output.resolve("data.zarr").toString());
    DatasetAttributes da = z.getDatasetAttributes("/0/0");
    assertArrayEquals(new long[] {512, 512, 2, 1, 1}, da.getDimensions());
  }

  /**
   * Test using a forced dimension order.
   */
  @Test
  public void testSetXYCZTDimensionOrder() throws Exception {
    input = fake("sizeC", "2");
    assertTool("--dimension-order", "XYCZT");
    N5ZarrReader z =
      new N5ZarrReader(output.resolve("data.zarr").toString());
    DatasetAttributes da = z.getDatasetAttributes("/0/0");
    assertArrayEquals(new long[] {512, 512, 2, 1, 1}, da.getDimensions());
  }

  /**
   * Test using a different tile size from the default (1024).
   */
  @Test
  public void testSetSmallerDefault() throws Exception {
    input = fake();
    assertTool("-h", "128", "-w", "128");
    N5ZarrReader z =
      new N5ZarrReader(output.resolve("data.zarr").toString());
    DatasetAttributes da = z.getDatasetAttributes("/0/0");
    assertArrayEquals(new long[] {512, 512, 1, 1, 1}, da.getDimensions());
    assertArrayEquals(new int[] {128, 128, 1, 1, 1}, da.getBlockSize());
  }

  /**
   * Test using a different tile size from the default (1024) the does not
   * divide evenly.
   */
  @Test
  public void testSetSmallerDefaultWithRemainder() throws Exception {
    input = fake();
    assertTool("-h", "384", "-w", "384");
    N5ZarrReader z =
      new N5ZarrReader(output.resolve("data.zarr").toString());
    DatasetAttributes da = z.getDatasetAttributes("/0/0");
    assertArrayEquals(new long[] {512, 512, 1, 1, 1}, da.getDimensions());
    assertArrayEquals(new int[] {384, 384, 1, 1, 1}, da.getBlockSize());
  }

  /**
   * Test more than one series.
   */
  @Test
  public void testMultiSeries() throws Exception {
    input = fake("series", "2");
    assertTool();
    N5ZarrReader z =
      new N5ZarrReader(output.resolve("data.zarr").toString());

    // Check series 0 dimensions and special pixels
    DatasetAttributes da = z.getDatasetAttributes("/0/0");
    assertArrayEquals(new long[] {512, 512, 1, 1, 1}, da.getDimensions());
    assertArrayEquals(new int[] {512, 512, 1, 1, 1}, da.getBlockSize());
    ByteBuffer tile = z.readBlock("/0/0", da, new long[] {0, 0, 0, 0, 0})
        .toByteBuffer();
    int[] seriesPlaneNumberZCT = FakeReader.readSpecialPixels(tile.array());
    assertArrayEquals(new int[] {0, 0, 0, 0, 0}, seriesPlaneNumberZCT);

    // Check series 1 dimensions and special pixels
    da = z.getDatasetAttributes("/1/0");
    assertArrayEquals(new long[] {512, 512, 1, 1, 1}, da.getDimensions());
    assertArrayEquals(new int[] {512, 512, 1, 1, 1}, da.getBlockSize());
    tile = z.readBlock("/1/0", da, new long[] {0, 0, 0, 0, 0})
            .toByteBuffer();
    seriesPlaneNumberZCT = FakeReader.readSpecialPixels(tile.array());
    assertArrayEquals(new int[] {1, 0, 0, 0, 0}, seriesPlaneNumberZCT);
  }

  /**
   * Test single-series conversion.
   */
  @Test
  public void testSingleSeries() throws Exception {
    input = fake("series", "2");
    assertTool("-s", "1");
    N5ZarrReader z =
            new N5ZarrReader(output.resolve("data.zarr").toString());

    // Check series 1 dimensions and special pixels
    DatasetAttributes da = z.getDatasetAttributes("/0/0");
    assertArrayEquals(new long[] {512, 512, 1, 1, 1}, da.getDimensions());
    assertArrayEquals(new int[] {512, 512, 1, 1, 1}, da.getBlockSize());
    ByteBuffer tile = z.readBlock("/0/0", da, new long[] {0, 0, 0, 0, 0})
            .toByteBuffer();
    int[] seriesPlaneNumberZCT = FakeReader.readSpecialPixels(tile.array());
    assertArrayEquals(new int[] {1, 0, 0, 0, 0}, seriesPlaneNumberZCT);
  }

  /**
   * Test more than one Z-section.
   */
  @Test
  public void testMultiZ() throws Exception {
    input = fake("sizeZ", "2");
    assertTool();
    N5ZarrReader z =
      new N5ZarrReader(output.resolve("data.zarr").toString());

    // Check dimensions and block size
    DatasetAttributes da = z.getDatasetAttributes("/0/0");
    assertArrayEquals(new long[] {512, 512, 2, 1, 1}, da.getDimensions());
    assertArrayEquals(new int[] {512, 512, 1, 1, 1}, da.getBlockSize());

    // Check Z 0 special pixels
    ByteBuffer tile = z.readBlock("/0/0", da, new long[] {0, 0, 0, 0, 0})
        .toByteBuffer();
    int[] seriesPlaneNumberZCT = FakeReader.readSpecialPixels(tile.array());
    assertArrayEquals(new int[] {0, 0, 0, 0, 0}, seriesPlaneNumberZCT);
    // Check Z 1 special pixels
    tile = z.readBlock("/0/0", da, new long[] {0, 0, 1, 0, 0})
            .toByteBuffer();
    seriesPlaneNumberZCT = FakeReader.readSpecialPixels(tile.array());
    assertArrayEquals(new int[] {0, 1, 1, 0, 0}, seriesPlaneNumberZCT);
  }

  /**
   * Test more than one channel.
   */
  @Test
  public void testMultiC() throws Exception {
    input = fake("sizeC", "2");
    assertTool();
    N5ZarrReader z =
      new N5ZarrReader(output.resolve("data.zarr").toString());

    // Check dimensions and block size
    DatasetAttributes da = z.getDatasetAttributes("/0/0");
    assertArrayEquals(new long[] {512, 512, 1, 2, 1}, da.getDimensions());
    assertArrayEquals(new int[] {512, 512, 1, 1, 1}, da.getBlockSize());

    // Check C 0 special pixels
    ByteBuffer tile = z.readBlock("/0/0", da, new long[] {0, 0, 0, 0, 0})
        .toByteBuffer();
    int[] seriesPlaneNumberZCT = FakeReader.readSpecialPixels(tile.array());
    assertArrayEquals(new int[] {0, 0, 0, 0, 0}, seriesPlaneNumberZCT);
    // Check C 1 special pixels
    tile = z.readBlock("/0/0", da, new long[] {0, 0, 0, 1, 0})
            .toByteBuffer();
    seriesPlaneNumberZCT = FakeReader.readSpecialPixels(tile.array());
    assertArrayEquals(new int[] {0, 1, 0, 1, 0}, seriesPlaneNumberZCT);
  }

  /**
   * Test more than one timepoint.
   */
  @Test
  public void testMultiT() throws Exception {
    input = fake("sizeT", "2");
    assertTool();
    N5ZarrReader z =
      new N5ZarrReader(output.resolve("data.zarr").toString());

    // Check dimensions and block size
    DatasetAttributes da = z.getDatasetAttributes("/0/0");
    assertArrayEquals(new long[] {512, 512, 1, 1, 2}, da.getDimensions());
    assertArrayEquals(new int[] {512, 512, 1, 1, 1}, da.getBlockSize());

    // Check T 0 special pixels
    ByteBuffer tile = z.readBlock("/0/0", da, new long[] {0, 0, 0, 0, 0})
        .toByteBuffer();
    int[] seriesPlaneNumberZCT = FakeReader.readSpecialPixels(tile.array());
    assertArrayEquals(new int[] {0, 0, 0, 0, 0}, seriesPlaneNumberZCT);
    // Check T 1 special pixels
    tile = z.readBlock("/0/0", da, new long[] {0, 0, 0, 0, 1})
            .toByteBuffer();
    seriesPlaneNumberZCT = FakeReader.readSpecialPixels(tile.array());
    assertArrayEquals(new int[] {0, 1, 0, 0, 1}, seriesPlaneNumberZCT);
  }

  /**
   * Test pixel type preservation.
   *
   * @param type string representation of Bio-Formats pixel type
   * @param n5Type expected corresponding N5 data type
   */
  @ParameterizedTest
  @MethodSource("getPixelTypes")
  public void testPixelType(String type, DataType n5Type) throws Exception {
    input = fake("pixelType", type);
    assertTool();
    N5ZarrReader z =
            new N5ZarrReader(output.resolve("data.zarr").toString());

    // Check series dimensions and special pixels
    DatasetAttributes da = z.getDatasetAttributes("/0/0");
    assertEquals(n5Type, da.getDataType());
    assertArrayEquals(new long[] {512, 512, 1, 1, 1}, da.getDimensions());
    assertArrayEquals(new int[] {512, 512, 1, 1, 1}, da.getBlockSize());
    ByteBuffer tile = z.readBlock("/0/0", da, new long[] {0, 0, 0, 0, 0})
        .toByteBuffer();
    int[] seriesPlaneNumberZCT = FakeReader.readSpecialPixels(tile.array());
    assertArrayEquals(new int[] {0, 0, 0, 0, 0}, seriesPlaneNumberZCT);
  }

  /**
   * @return pairs of pixel type strings and N5 data types
   */
  static Stream<Arguments> getPixelTypes() {
    return Stream.of(
      Arguments.of("float", DataType.FLOAT32),
      Arguments.of("double", DataType.FLOAT64),
      Arguments.of("uint32", DataType.UINT32),
      Arguments.of("int32", DataType.INT32)
    );
  }

  /**
   * Test that there are no edge effects when tiles do not divide evenly
   * and downsampling.
   */
  @Test
  public void testDownsampleEdgeEffectsUInt8() throws Exception {
    input = fake("sizeX", "60", "sizeY", "300");
    assertTool("-w", "25", "-h", "75");
    N5ZarrReader z =
        new N5ZarrReader(output.resolve("data.zarr").toString());

    // Check series dimensions
    DatasetAttributes da = z.getDatasetAttributes("/0/1");
    assertArrayEquals(new long[] {30, 150, 1, 1, 1}, da.getDimensions());
    assertArrayEquals(new int[] {25, 75, 1, 1, 1}, da.getBlockSize());
    ByteBuffer tile = z.readBlock("/0/1", da, new long[] {1, 0, 0, 0, 0})
        .toByteBuffer();
    // Last row first pixel should be the 2x2 downsampled value;
    // test will break if the downsampling algorithm changes
    assertEquals(50, tile.get(75 * 24));
  }

  /**
   * Test that there are no edge effects when tiles do not divide evenly
   * and downsampling.
   */
  @Test
  public void testDownsampleEdgeEffectsUInt16() throws Exception {
    input = fake("sizeX", "60", "sizeY", "300", "pixelType", "uint16");
    assertTool("-w", "25", "-h", "75");
    N5ZarrReader z =
        new N5ZarrReader(output.resolve("data.zarr").toString());

    // Check series dimensions
    DatasetAttributes da = z.getDatasetAttributes("/0/1");
    assertEquals(DataType.UINT16, da.getDataType());
    assertArrayEquals(new long[] {30, 150, 1, 1, 1}, da.getDimensions());
    assertArrayEquals(new int[] {25, 75, 1, 1, 1}, da.getBlockSize());
    ShortBuffer tile = z.readBlock("/0/1", da, new long[] {1, 0, 0, 0, 0})
        .toByteBuffer().asShortBuffer();
    // Last row first pixel should be the 2x2 downsampled value;
    // test will break if the downsampling algorithm changes
    assertEquals(50, tile.get(75 * 24));
  }

  /**
   * Test that original metadata is saved.
   */
  @Test
  public void testOriginalMetadata() throws Exception {
    Map<String, String> originalMetadata = new HashMap<String, String>();
    originalMetadata.put("key1", "value1");
    originalMetadata.put("key2", "value2");

    input = fake(null, null, originalMetadata);
    assertTool();
    Path omexml = output.resolve("METADATA.ome.xml");
    StringBuilder xml = new StringBuilder();
    Files.lines(omexml).forEach(v -> xml.append(v));

    OMEXMLService service =
      new ServiceFactory().getInstance(OMEXMLService.class);
    OMEXMLMetadata retrieve =
      (OMEXMLMetadata) service.createOMEXMLMetadata(xml.toString());
    Hashtable convertedMetadata = service.getOriginalMetadata(retrieve);
    assertEquals(originalMetadata.size(), convertedMetadata.size());
    for (String key : originalMetadata.keySet()) {
      assertEquals(originalMetadata.get(key), convertedMetadata.get(key));
    }
  }

  /**
   * Test that execution fails if the output directory already exists and the
   * <code>--overwrite</code> option has not been supplied.
   */
  @Test
  public void testFailIfNoOverwrite() throws IOException {
    input = fake();
    Files.createDirectory(output);
    assertThrows(ExecutionException.class, () -> {
      assertTool();
    });
  }

  /**
   * Test that execution succeeds if the output directory already exists and
   * the <code>--overwrite</code> option has been supplied.
   */
  @Test
  public void testOverwrite() throws IOException {
    input = fake();
    Files.createDirectory(output);
    assertTool("--overwrite");
  }

  /**
   * Test that appropriate metadata is written for each downsampling type.
   *
   * @param type downsampling type
   */
  @ParameterizedTest
  @EnumSource(Downsampling.class)
  public void testDownsampleTypes(Downsampling type) throws IOException {
    input = fake();
    assertTool("--downsample-type", type.toString());

    N5ZarrReader z =
          new N5ZarrReader(output.resolve("data.zarr").toString());
    List<Map<String, Object>> multiscales =
          z.getAttribute("/0", "multiscales", List.class);
    assertEquals(1, multiscales.size());
    Map<String, Object> multiscale = multiscales.get(0);
    assertEquals("0.1", multiscale.get("version"));

    Map<String, String> metadata =
      (Map<String, String>) multiscale.get("metadata");
    assertNotNull(metadata);

    String version = metadata.get("version");
    String method = metadata.get("method");
    assertNotNull(version);
    assertNotNull(method);

    if (type != Downsampling.SIMPLE) {
      assertEquals(type.getName(), multiscale.get("type"));
      assertEquals(Core.VERSION, version);
      assertEquals("org.opencv.imgproc.Imgproc." +
        (type == Downsampling.GAUSSIAN ? "pyrDown" : "resize"), method);
    }
    else {
      assertEquals("Bio-Formats " + FormatTools.VERSION, version);
      assertEquals("loci.common.image.SimpleImageScaler", method);
    }
  }

}
