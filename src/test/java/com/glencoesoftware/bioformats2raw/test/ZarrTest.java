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
import java.nio.charset.StandardCharsets;
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

import com.bc.zarr.DataType;
import com.bc.zarr.ZarrArray;
import com.bc.zarr.ZarrGroup;
import com.glencoesoftware.bioformats2raw.Converter;
import com.glencoesoftware.bioformats2raw.Downsampling;
import loci.common.LogbackTools;
import loci.common.services.ServiceFactory;
import loci.formats.FormatTools;
import loci.formats.in.FakeReader;
import loci.formats.ome.OMEXMLMetadata;
import loci.formats.services.OMEXMLService;
import ome.xml.model.OME;
import ome.xml.model.Pixels;
import picocli.CommandLine;
import picocli.CommandLine.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
    args.add(input.toString());
    args.add(output.toString());
    try {
      converter = new Converter();
      CommandLine.call(converter, args.toArray(new String[]{}));
      Path zarr = output.resolve("data.zarr");
      assertTrue(Files.exists(zarr));
      assertTrue(Files.exists(zarr.resolve("METADATA.ome.xml")));
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
    Path root = output.resolve("data.zarr");
    ZarrGroup series0 = ZarrGroup.open(root.resolve("abc/888/0").toString());
    series0.openArray("0");
    series0 = ZarrGroup.open(root.resolve("ghi/999/1").toString());
    series0.openArray("0");
  }

  /**
   * Test a fake file conversion and ensure the layout is set.
   */
  @Test
  public void testDefaultLayoutIsSet() throws Exception {
    input = fake();
    assertTool();
    ZarrGroup z =
        ZarrGroup.open(output.resolve("data.zarr").toString());
    Integer layout = (Integer)
        z.getAttributes().get("bioformats2raw.layout");
    assertEquals(Converter.LAYOUT, layout);
  }

  /**
   * Test that multiscales metadata is present.
   */
  @Test
  public void testMultiscalesMetadata() throws Exception {
    input = fake();
    assertTool();
    ZarrGroup z =
        ZarrGroup.open(output.resolve("data.zarr").resolve("0").toString());
    List<Map<String, Object>> multiscales = (List<Map<String, Object>>)
            z.getAttributes().get("multiscales");
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
    ZarrGroup z =
        ZarrGroup.open(output.resolve("data.zarr").toString());
    ZarrArray array = z.openArray("0/0");
    assertArrayEquals(new int[] {1, 2, 1, 512, 512}, array.getShape());
  }

  /**
   * Test using a forced dimension order.
   */
  @Test
  public void testSetXYCZTDimensionOrder() throws Exception {
    input = fake("sizeC", "2");
    assertTool("--dimension-order", "XYCZT");
    ZarrGroup z =
        ZarrGroup.open(output.resolve("data.zarr").toString());
    ZarrArray array = z.openArray("0/0");
    assertArrayEquals(new int[] {1, 1, 2, 512, 512}, array.getShape());
  }

  /**
   * Test setting original (source file) dimension order.
   */
  @Test
  public void testSetOriginalDimensionOrder() throws Exception {
    input = fake("sizeC", "2", "dimOrder", "XYCZT");
    assertTool("--dimension-order", "original");
    ZarrGroup z =
        ZarrGroup.open(output.resolve("data.zarr").toString());
    ZarrArray array = z.openArray("0/0");
    assertArrayEquals(new int[] {1, 1, 2, 512, 512}, array.getShape());
  }

  /**
   * Test using a different tile size from the default (1024).
   */
  @Test
  public void testSetSmallerDefault() throws Exception {
    input = fake();
    assertTool("-h", "128", "-w", "128");
    ZarrGroup z =
        ZarrGroup.open(output.resolve("data.zarr").toString());
    ZarrArray array = z.openArray("0/0");
    assertArrayEquals(new int[] {1, 1, 1, 512, 512}, array.getShape());
    assertArrayEquals(new int[] {1, 1, 1, 128, 128}, array.getChunks());
  }

  /**
   * Test using a different tile size from the default (1024) the does not
   * divide evenly.
   */
  @Test
  public void testSetSmallerDefaultWithRemainder() throws Exception {
    input = fake();
    assertTool("-h", "384", "-w", "384");
    ZarrGroup z =
        ZarrGroup.open(output.resolve("data.zarr").toString());
    ZarrArray array = z.openArray("0/0");
    assertArrayEquals(new int[] {1, 1, 1, 512, 512}, array.getShape());
    assertArrayEquals(new int[] {1, 1, 1, 384, 384}, array.getChunks());
  }

  /**
   * Test more than one series.
   */
  @Test
  public void testMultiSeries() throws Exception {
    input = fake("series", "2");
    assertTool();
    ZarrGroup z =
        ZarrGroup.open(output.resolve("data.zarr").toString());

    // Check series 0 dimensions and special pixels
    ZarrArray series0 = z.openArray("0/0");
    assertArrayEquals(new int[] {1, 1, 1, 512, 512}, series0.getShape());
    assertArrayEquals(new int[] {1, 1, 1, 512, 512}, series0.getChunks());
    int[] shape = new int[] {1, 1, 1, 512, 512};
    byte[] tile = new byte[512 * 512];
    series0.read(tile, shape);
    int[] seriesPlaneNumberZCT = FakeReader.readSpecialPixels(tile);
    assertArrayEquals(new int[] {0, 0, 0, 0, 0}, seriesPlaneNumberZCT);

    // Check series 1 dimensions and special pixels
    ZarrArray series1 = z.openArray("1/0");
    assertArrayEquals(new int[] {1, 1, 1, 512, 512}, series1.getShape());
    assertArrayEquals(new int[] {1, 1, 1, 512, 512}, series1.getChunks());
    series1.read(tile, shape);
    seriesPlaneNumberZCT = FakeReader.readSpecialPixels(tile);
    assertArrayEquals(new int[] {1, 0, 0, 0, 0}, seriesPlaneNumberZCT);
  }

  /**
   * Test single beginning -series conversion.
   */
  @Test
  public void testSingleBeginningSeries() throws Exception {
    input = fake("series", "2");
    assertTool("-s", "0");
    ZarrGroup z =
        ZarrGroup.open(output.resolve("data.zarr").toString());

    // Check series 0 dimensions and special pixels
    ZarrArray series0 = z.openArray("0/0");
    assertArrayEquals(new int[] {1, 1, 1, 512, 512}, series0.getShape());
    assertArrayEquals(new int[] {1, 1, 1, 512, 512}, series0.getChunks());
    int[] shape = new int[] {1, 1, 1, 512, 512};
    byte[] tile = new byte[512 * 512];
    series0.read(tile, shape);
    int[] seriesPlaneNumberZCT = FakeReader.readSpecialPixels(tile);
    assertArrayEquals(new int[] {0, 0, 0, 0, 0}, seriesPlaneNumberZCT);
    try {
      z.openArray("1/0");
      fail("Array exists!");
    }
    catch (IOException e) {
      // Pass
    }
  }

  /**
   * Test single end series conversion.
   */
  @Test
  public void testSingleEndSeries() throws Exception {
    input = fake("series", "2");
    assertTool("-s", "1");
    ZarrGroup z =
        ZarrGroup.open(output.resolve("data.zarr").toString());

    // Check series 1 dimensions and special pixels
    ZarrArray series0 = z.openArray("0/0");
    assertArrayEquals(new int[] {1, 1, 1, 512, 512}, series0.getShape());
    assertArrayEquals(new int[] {1, 1, 1, 512, 512}, series0.getChunks());
    int[] shape = new int[] {1, 1, 1, 512, 512};
    byte[] tile = new byte[512 * 512];
    series0.read(tile, shape);
    int[] seriesPlaneNumberZCT = FakeReader.readSpecialPixels(tile);
    assertArrayEquals(new int[] {1, 0, 0, 0, 0}, seriesPlaneNumberZCT);
    try {
      z.openArray("1/0");
      fail("Array exists!");
    }
    catch (IOException e) {
      // Pass
    }
  }

  /**
   * Test single middle series conversion.
   */
  @Test
  public void testSingleMiddleSeries() throws Exception {
    input = fake("series", "3");
    assertTool("-s", "1");
    ZarrGroup z =
        ZarrGroup.open(output.resolve("data.zarr").toString());

    // Check series 1 dimensions and special pixels
    ZarrArray series0 = z.openArray("0/0");
    assertArrayEquals(new int[] {1, 1, 1, 512, 512}, series0.getShape());
    assertArrayEquals(new int[] {1, 1, 1, 512, 512}, series0.getChunks());
    int[] shape = new int[] {1, 1, 1, 512, 512};
    byte[] tile = new byte[512 * 512];
    series0.read(tile, shape);
    int[] seriesPlaneNumberZCT = FakeReader.readSpecialPixels(tile);
    assertArrayEquals(new int[] {1, 0, 0, 0, 0}, seriesPlaneNumberZCT);
    try {
      z.openArray("1/0");
      fail("Array exists!");
    }
    catch (IOException e) {
      // Pass
    }
    try {
      z.openArray("2/0");
      fail("Array exists!");
    }
    catch (IOException e) {
      // Pass
    }
  }

  /**
   * Test more than one Z-section.
   */
  @Test
  public void testMultiZ() throws Exception {
    input = fake("sizeZ", "2");
    assertTool();
    ZarrGroup z =
        ZarrGroup.open(output.resolve("data.zarr").toString());

    // Check dimensions and block size
    ZarrArray series0 = z.openArray("0/0");
    assertArrayEquals(new int[] {1, 1, 2, 512, 512}, series0.getShape());
    assertArrayEquals(new int[] {1, 1, 1, 512, 512}, series0.getChunks());

    // Check Z 0 special pixels
    int[] shape = new int[] {1, 1, 1, 512, 512};
    byte[] tile = new byte[512 * 512];
    series0.read(tile, shape);
    int[] seriesPlaneNumberZCT = FakeReader.readSpecialPixels(tile);
    assertArrayEquals(new int[] {0, 0, 0, 0, 0}, seriesPlaneNumberZCT);
    // Check Z 1 special pixels
    int[] offset = new int[] {0, 0, 1, 0, 0};
    series0.read(tile, shape, offset);
    seriesPlaneNumberZCT = FakeReader.readSpecialPixels(tile);
    assertArrayEquals(new int[] {0, 1, 1, 0, 0}, seriesPlaneNumberZCT);
  }

  /**
   * Test more than one channel.
   */
  @Test
  public void testMultiC() throws Exception {
    input = fake("sizeC", "2");
    assertTool();
    ZarrGroup z =
        ZarrGroup.open(output.resolve("data.zarr").toString());

    // Check dimensions and block size
    ZarrArray series0 = z.openArray("0/0");
    assertArrayEquals(new int[] {1, 2, 1, 512, 512}, series0.getShape());
    assertArrayEquals(new int[] {1, 1, 1, 512, 512}, series0.getChunks());

    // Check C 0 special pixels
    int[] shape = new int[] {1, 1, 1, 512, 512};
    byte[] tile = new byte[512 * 512];
    series0.read(tile, shape);
    int[] seriesPlaneNumberZCT = FakeReader.readSpecialPixels(tile);
    assertArrayEquals(new int[] {0, 0, 0, 0, 0}, seriesPlaneNumberZCT);
    // Check C 1 special pixels
    int[] offset = new int[] {0, 1, 0, 0, 0};
    series0.read(tile, shape, offset);
    seriesPlaneNumberZCT = FakeReader.readSpecialPixels(tile);
    assertArrayEquals(new int[] {0, 1, 0, 1, 0}, seriesPlaneNumberZCT);
  }

  /**
   * Test more than one timepoint.
   */
  @Test
  public void testMultiT() throws Exception {
    input = fake("sizeT", "2");
    assertTool();
    ZarrGroup z =
        ZarrGroup.open(output.resolve("data.zarr").toString());

    // Check dimensions and block size
    ZarrArray series0 = z.openArray("0/0");
    assertArrayEquals(new int[] {2, 1, 1, 512, 512}, series0.getShape());
    assertArrayEquals(new int[] {1, 1, 1, 512, 512}, series0.getChunks());

    // Check T 0 special pixels
    int[] shape = new int[] {1, 1, 1, 512, 512};
    byte[] tile = new byte[512 * 512];
    series0.read(tile, shape);
    int[] seriesPlaneNumberZCT = FakeReader.readSpecialPixels(tile);
    assertArrayEquals(new int[] {0, 0, 0, 0, 0}, seriesPlaneNumberZCT);
    // Check T 1 special pixels
    int[] offset = new int[] {1, 0, 0, 0, 0};
    series0.read(tile, shape, offset);
    seriesPlaneNumberZCT = FakeReader.readSpecialPixels(tile);
    assertArrayEquals(new int[] {0, 1, 0, 0, 1}, seriesPlaneNumberZCT);
  }

  private int bytesPerPixel(DataType dataType) {
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
        throw new IllegalArgumentException("Unsupported data type: "
            + dataType.toString());
    }
  }

  /**
   * Test pixel type preservation.
   *
   * @param type string representation of Bio-Formats pixel type
   * @param dataType expected corresponding Zarr data type
   */
  @ParameterizedTest
  @MethodSource("getPixelTypes")
  public void testPixelType(String type, DataType dataType) throws Exception {
    input = fake("pixelType", type);
    assertTool();
    ZarrGroup z =
        ZarrGroup.open(output.resolve("data.zarr").toString());

    // Check series dimensions and special pixels
    ZarrArray series0 = z.openArray("0/0");
    assertEquals(dataType, series0.getDataType());
    assertArrayEquals(new int[] {1, 1, 1, 512, 512}, series0.getShape());
    assertArrayEquals(new int[] {1, 1, 1, 512, 512}, series0.getChunks());
    int bytesPerPixel = bytesPerPixel(dataType);
    int[] shape = new int[] {1, 1, 1, 512, 512};

    int pixelType = FormatTools.pixelTypeFromString(type);
    byte[] tileAsBytes = new byte[512 * 512 * bytesPerPixel];
    ByteBuffer tileAsByteBuffer = ByteBuffer.wrap(tileAsBytes);
    switch (pixelType) {
      case FormatTools.INT8:
      case FormatTools.UINT8: {
        series0.read(tileAsBytes, shape);
        break;
      }
      case FormatTools.INT16:
      case FormatTools.UINT16: {
        short[] tileAsShorts = new short[512 * 512];
        series0.read(tileAsShorts, shape);
        tileAsByteBuffer.asShortBuffer().put(tileAsShorts);
        break;
      }
      case FormatTools.INT32:
      case FormatTools.UINT32: {
        int[] tileAsInts = new int[512 * 512];
        series0.read(tileAsInts, shape);
        tileAsByteBuffer.asIntBuffer().put(tileAsInts);
        break;
      }
      case FormatTools.FLOAT: {
        float[] tileAsFloats = new float[512 * 512];
        series0.read(tileAsFloats, shape);
        tileAsByteBuffer.asFloatBuffer().put(tileAsFloats);
        break;
      }
      case FormatTools.DOUBLE: {
        double[] tileAsDoubles = new double[512 * 512];
        series0.read(tileAsDoubles, shape);
        tileAsByteBuffer.asDoubleBuffer().put(tileAsDoubles);
        break;
      }
      default:
        throw new IllegalArgumentException("Unsupported pixel type: "
            + FormatTools.getPixelTypeString(pixelType));
    }
    int[] seriesPlaneNumberZCT = FakeReader.readSpecialPixels(
        tileAsBytes, pixelType, false);
    assertArrayEquals(new int[] {0, 0, 0, 0, 0}, seriesPlaneNumberZCT);
  }

  /**
   * @return pairs of pixel type strings and Zarr data types
   */
  static Stream<Arguments> getPixelTypes() {
    return Stream.of(
      Arguments.of("float", DataType.f4),
      Arguments.of("double", DataType.f8),
      Arguments.of("uint32", DataType.u4),
      Arguments.of("int32", DataType.i4)
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
    ZarrGroup z =
        ZarrGroup.open(output.resolve("data.zarr").toString());

    // Check series dimensions
    ZarrArray series1 = z.openArray("0/1");
    assertArrayEquals(new int[] {1, 1, 1, 150, 30}, series1.getShape());
    assertArrayEquals(new int[] {1, 1, 1, 75, 25}, series1.getChunks());
    int[] shape = new int[] {1, 1, 1, 75, 5};
    int[] offset = new int[] {0, 0, 0, 75, 25};
    byte[] tile = new byte[75 * 5];
    series1.read(tile, shape, offset);
    // Last row first pixel should be the 2x2 downsampled value;
    // test will break if the downsampling algorithm changes
    assertEquals(50, tile[75 * 4]);
  }

  /**
   * Test that there are no edge effects when tiles do not divide evenly
   * and downsampling.
   */
  @Test
  public void testDownsampleEdgeEffectsUInt16() throws Exception {
    input = fake("sizeX", "60", "sizeY", "300", "pixelType", "uint16");
    assertTool("-w", "25", "-h", "75");
    ZarrGroup z =
        ZarrGroup.open(output.resolve("data.zarr").toString());

    ZarrArray series0 = z.openArray("0/0");
    assertEquals(DataType.u2, series0.getDataType());
    assertArrayEquals(new int[] {1, 1, 1, 300, 60}, series0.getShape());
    int[] shape = new int[] {1, 1, 1, 10, 10};
    int[] offset = new int[] {0, 0, 0, 290, 0};
    short[] tile = new short[10 * 10];
    series0.read(tile, shape, offset);
    for (int y=0; y<10; y++) {
      for (int x=0; x<10; x++) {
        assertEquals(x, tile[y * 10 + x]);
      }
    }

    // Check series dimensions
    ZarrArray series1 = z.openArray("0/1");
    assertEquals(DataType.u2, series1.getDataType());
    assertArrayEquals(new int[] {1, 1, 1, 150, 30}, series1.getShape());
    assertArrayEquals(new int[] {1, 1, 1, 75, 25}, series1.getChunks());
    shape = new int[] {1, 1, 1, 75, 5};
    offset = new int[] {0, 0, 0, 75, 25};
    tile = new short[75 * 5];
    series1.read(tile, shape, offset);
    // Last row first pixel should be the 2x2 downsampled value;
    // test will break if the downsampling algorithm changes
    assertEquals(50, tile[75 * 4]);
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
    Path omexml = output.resolve("data.zarr").resolve("METADATA.ome.xml");
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

    ZarrGroup z =
        ZarrGroup.open(output.resolve("data.zarr").resolve("0").toString());
    List<Map<String, Object>> multiscales =
          (List<Map<String, Object>>) z.getAttributes().get("multiscales");
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

  /**
   * Convert a plate with the --no-hcs option.
   * The output should not be compliant with OME Zarr HCS.
   */
  @Test
  public void testNoHCSOption() throws IOException {
    input = fake(
      "plates", "1", "plateAcqs", "1",
      "plateRows", "2", "plateCols", "3", "fields", "2");
    assertTool("--no-hcs");

    ZarrGroup z = ZarrGroup.open(output.resolve("data.zarr"));

    // Check dimensions and block size
    ZarrArray series0 = z.openArray("0/0");
    assertArrayEquals(new int[] {1, 1, 1, 512, 512}, series0.getShape());
    assertArrayEquals(new int[] {1, 1, 1, 512, 512}, series0.getChunks());
    assertEquals(12, z.getGroupKeys().size());
  }

  /**
   * Convert a plate with default options.
   * The output should be compliant with OME Zarr HCS.
   */
  @Test
  public void testHCSMetadata() throws IOException {
    input = fake(
      "plates", "1", "plateAcqs", "1",
      "plateRows", "2", "plateCols", "3", "fields", "2");
    assertTool();

    Path root = output.resolve("data.zarr");
    ZarrGroup z = ZarrGroup.open(root);

    // check valid group layout
    // METADATA.ome.xml, .zattrs (Plate), .zgroup (Plate) and 2 rows
    assertEquals(5, Files.list(root).toArray().length);
    for (int row=0; row<2; row++) {
      Path rowPath = root.resolve(Integer.toString(row));
      // .zgroup (Row) and 3 columns
      assertEquals(4, Files.list(rowPath).toArray().length);
      for (int col=0; col<3; col++) {
        Path colPath = rowPath.resolve(Integer.toString(col));
        ZarrGroup colGroup = ZarrGroup.open(colPath);
        // .zattrs (Column/Image), .zgroup (Column/Image) and 2 fields
        assertEquals(4, Files.list(colPath).toArray().length);
        for (int field=0; field<2; field++) {
          // append resolution index
          ZarrArray series0 = colGroup.openArray(field + "/0");
          assertArrayEquals(new int[] {1, 1, 1, 512, 512}, series0.getShape());
          assertArrayEquals(new int[] {1, 1, 1, 512, 512}, series0.getChunks());
        }
      }
    }

    // check plate/well level metadata
    Map<String, Object> plate =
        (Map<String, Object>) z.getAttributes().get("plate");
    assertEquals(2, ((Number) plate.get("field_count")).intValue());

    List<Map<String, Object>> acquisitions =
      (List<Map<String, Object>>) plate.get("acquisitions");
    List<Map<String, Object>> rows =
      (List<Map<String, Object>>) plate.get("rows");
    List<Map<String, Object>> columns =
      (List<Map<String, Object>>) plate.get("columns");
    List<Map<String, Object>> wells =
      (List<Map<String, Object>>) plate.get("wells");

    assertEquals(1, acquisitions.size());
    assertEquals("0", acquisitions.get(0).get("id"));

    assertEquals(2, rows.size());
    for (int row=0; row<rows.size(); row++) {
      assertEquals(String.valueOf(row), rows.get(row).get("name"));
    }

    assertEquals(3, columns.size());
    for (int col=0; col<columns.size(); col++) {
      assertEquals(String.valueOf(col), columns.get(col).get("name"));
    }

    assertEquals(rows.size() * columns.size(), wells.size());
    for (int row=0; row<rows.size(); row++) {
      for (int col=0; col<columns.size(); col++) {
        int well = row * columns.size() + col;
        assertEquals(row + "/" + col, wells.get(well).get("path"));
      }
    }

    // check well metadata
    for (Map<String, Object> row : rows) {
      String rowName = (String) row.get("name");
      for (Map<String, Object> column : columns) {
        String columnName = (String) column.get("name");
        ZarrGroup wellGroup = ZarrGroup.open(
            root.resolve(rowName).resolve(columnName));
        Map<String, Object> well =
            (Map<String, Object>) wellGroup.getAttributes().get("well");
        List<Map<String, Object>> images =
            (List<Map<String, Object>>) well.get("images");
        assertEquals(2, images.size());
        Map<String, Object> field1 = images.get(0);  // Field 1
        assertEquals(field1.get("path"), "0");
        assertEquals(0, field1.get("acquisition"));
        Map<String, Object> field2 = images.get(1);  // Field 2
        assertEquals(field2.get("path"), "1");
        assertEquals(0, field2.get("acquisition"));
      }
    }
  }

  /**
   * Convert an RGB image.  Ensure that the Channels are correctly split.
   */
  @Test
  public void testRGBChannelSeparator() throws Exception {
    input = fake("sizeC", "3", "rgb", "3");
    assertTool();

    Path xml = output.resolve("METADATA.ome.xml");
    ServiceFactory sf = new ServiceFactory();
    OMEXMLService xmlService = sf.getInstance(OMEXMLService.class);
    OME ome = (OME) xmlService.createOMEXMLRoot(
        new String(Files.readAllBytes(xml), StandardCharsets.UTF_8));
    assertEquals(1, ome.sizeOfImageList());
    Pixels pixels = ome.getImage(0).getPixels();
    assertEquals(3, pixels.sizeOfChannelList());
  }

}
