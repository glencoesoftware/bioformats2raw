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
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.bc.zarr.DataType;
import com.bc.zarr.DimensionSeparator;
import com.bc.zarr.ZarrArray;
import com.bc.zarr.ZarrGroup;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.glencoesoftware.bioformats2raw.Converter;
import com.glencoesoftware.bioformats2raw.Downsampling;
import loci.common.LogbackTools;
import loci.common.services.ServiceFactory;
import loci.formats.FormatTools;
import loci.formats.Memoizer;
import loci.formats.in.FakeReader;
import loci.formats.ome.OMEXMLMetadata;
import loci.formats.services.OMEXMLService;
import ome.xml.model.OME;
import ome.xml.model.Pixels;
import picocli.CommandLine;
import picocli.CommandLine.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import org.junit.jupiter.params.provider.ValueSource;
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
    ZarrGroup series0 = ZarrGroup.open(output.resolve("abc/888/0").toString());
    series0.openArray("0");
    series0 = ZarrGroup.open(output.resolve("ghi/999/1").toString());
    series0.openArray("0");

    Path omePath = output.resolve("OME");
    ZarrGroup z = ZarrGroup.open(omePath.toString());
    List<String> groupMap = (List<String>) z.getAttributes().get("series");
    assertEquals(groupMap.size(), 2);
    assertEquals(groupMap.get(0), "abc/888/0");
    assertEquals(groupMap.get(1), "ghi/999/1");
  }

  /**
   * Test single directory scale format string.
   *
   * @param format scale format string
   */
  @ParameterizedTest
  @ValueSource(strings = {"%2$d", "%2$d/"})
  public void testSingleDirectoryScaleFormat(String format) throws Exception {
    input = fake();
    assertTool("--scale-format-string", format);
    ZarrGroup series0 = ZarrGroup.open(output.toString());
    series0.openArray("0");
    List<Map<String, Object>> multiscales = (List<Map<String, Object>>)
            series0.getAttributes().get("multiscales");
    assertEquals(1, multiscales.size());

    Map<String, Object> multiscale = multiscales.get(0);
    List<Map<String, Object>> datasets =
            (List<Map<String, Object>>) multiscale.get("datasets");
    assertTrue(datasets.size() > 0);
    for (int i=0; i<datasets.size(); i++) {
      assertEquals(String.valueOf(i), datasets.get(i).get("path"));
    }
  }

  /**
   * Test a fake file conversion and ensure the layout is set and that the
   * output is nested.
   */
  @Test
  public void testDefaultLayoutIsSetAndIsNested() throws Exception {
    input = fake();
    assertTool();
    ZarrGroup z = ZarrGroup.open(output.toString());
    Integer layout = (Integer)
        z.getAttributes().get("bioformats2raw.layout");

    Path omePath = output.resolve("OME");
    ZarrGroup omeGroup = ZarrGroup.open(omePath.toString());
    List<String> groupMap =
      (List<String>) omeGroup.getAttributes().get("series");
    assertEquals(groupMap.size(), 1);
    assertEquals(groupMap.get(0), "0");

    ZarrArray series0 = ZarrGroup.open(output.resolve("0")).openArray("0");

    // check that the correct separator was used
    assertEquals(series0.getDimensionSeparator(), DimensionSeparator.SLASH);

    // Also ensure we're using the latest .zarray metadata
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode root = objectMapper.readTree(
        output.resolve("0/0/.zarray").toFile());
    assertEquals("/", root.path("dimension_separator").asText());
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
        ZarrGroup.open(output.resolve("0").toString());
    List<Map<String, Object>> multiscales = (List<Map<String, Object>>)
            z.getAttributes().get("multiscales");
    assertEquals(1, multiscales.size());
    Map<String, Object> multiscale = multiscales.get(0);
    assertEquals("0.4", multiscale.get("version"));
    assertEquals("image", multiscale.get("name"));
    List<Map<String, Object>> datasets =
            (List<Map<String, Object>>) multiscale.get("datasets");
    assertTrue(datasets.size() > 0);
    assertEquals("0", datasets.get(0).get("path"));

    List<Map<String, Object>> axes =
      (List<Map<String, Object>>) multiscale.get("axes");
    checkAxes(axes, "TCZYX", null);

    for (int r=0; r<datasets.size(); r++) {
      Map<String, Object> dataset = datasets.get(r);
      List<Map<String, Object>> transforms =
        (List<Map<String, Object>>) dataset.get("coordinateTransformations");
      assertEquals(1, transforms.size());
      Map<String, Object> scale = transforms.get(0);
      assertEquals("scale", scale.get("type"));
      List<Double> axisValues = (List<Double>) scale.get("scale");

      assertEquals(5, axisValues.size());
      double factor = Math.pow(2, r);
      // X and Y are the only dimensions that are downsampled,
      // so the TCZ physical scales remain the same across all resolutions
      assertEquals(axisValues, Arrays.asList(new Double[] {
        1.0, 1.0, 1.0, factor, factor}));
    }
  }

  /**
   * Test alternative dimension order.
   */
  @Test
  public void testXYCZTDimensionOrder() throws Exception {
    input = fake("sizeC", "2", "dimOrder", "XYCZT");
    assertTool();
    ZarrGroup z = ZarrGroup.open(output.toString());
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
    ZarrGroup z = ZarrGroup.open(output.toString());
    ZarrArray array = z.openArray("0/0");
    assertArrayEquals(new int[] {1, 1, 2, 512, 512}, array.getShape());

    z = ZarrGroup.open(output.resolve("0").toString());
    List<Map<String, Object>> multiscales = (List<Map<String, Object>>)
            z.getAttributes().get("multiscales");
    assertEquals(1, multiscales.size());
    Map<String, Object> multiscale = multiscales.get(0);
    List<Map<String, Object>> axes =
      (List<Map<String, Object>>) multiscale.get("axes");
    checkAxes(axes, "TZCYX", null);
  }

  /**
   * Test setting original (source file) dimension order.
   */
  @Test
  public void testSetOriginalDimensionOrder() throws Exception {
    input = fake("sizeC", "2", "dimOrder", "XYCZT");
    assertTool("--dimension-order", "original");
    ZarrGroup z = ZarrGroup.open(output.toString());
    ZarrArray array = z.openArray("0/0");
    assertArrayEquals(new int[] {1, 1, 2, 512, 512}, array.getShape());

    z = ZarrGroup.open(output.resolve("0").toString());
    List<Map<String, Object>> multiscales = (List<Map<String, Object>>)
            z.getAttributes().get("multiscales");
    assertEquals(1, multiscales.size());
    Map<String, Object> multiscale = multiscales.get(0);
    List<Map<String, Object>> axes =
      (List<Map<String, Object>>) multiscale.get("axes");
    checkAxes(axes, "TZCYX", null);
  }

  /**
   * Test that physical sizes are saved in axes/transformations metadata.
   */
  @Test
  public void testPhysicalSizes() throws Exception {
    input = fake("physicalSizeX", "1.0Å",
      "physicalSizeY", "0.5mm",
      "physicalSizeZ", "2cm");
    assertTool();

    ZarrGroup z = ZarrGroup.open(output.resolve("0").toString());
    List<Map<String, Object>> multiscales = (List<Map<String, Object>>)
            z.getAttributes().get("multiscales");
    assertEquals(1, multiscales.size());
    Map<String, Object> multiscale = multiscales.get(0);
    List<Map<String, Object>> axes =
      (List<Map<String, Object>>) multiscale.get("axes");
    checkAxes(axes, "TCZYX",
      new String[] {null, null, "centimeter", "millimeter", "angstrom"});

    List<Map<String, Object>> datasets =
      (List<Map<String, Object>>) multiscale.get("datasets");
    assertEquals(2, datasets.size());

    for (int r=0; r<datasets.size(); r++) {
      Map<String, Object> dataset = datasets.get(r);
      List<Map<String, Object>> transforms =
        (List<Map<String, Object>>) dataset.get("coordinateTransformations");
      assertEquals(1, transforms.size());
      Map<String, Object> scale = transforms.get(0);
      assertEquals("scale", scale.get("type"));
      List<Double> axisValues = (List<Double>) scale.get("scale");

      assertEquals(5, axisValues.size());
      double factor = Math.pow(2, r);
      // X and Y are the only dimensions that are downsampled,
      // so the TCZ physical scales remain the same across all resolutions
      assertEquals(axisValues, Arrays.asList(new Double[] {
        1.0, 1.0, 2.0, 0.5 * factor, factor}));
    }
  }

  /**
   * Test using a different tile size from the default (1024).
   */
  @Test
  public void testSetSmallerDefault() throws Exception {
    input = fake();
    assertTool("-h", "128", "-w", "128");
    ZarrGroup z = ZarrGroup.open(output.toString());
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
    ZarrGroup z = ZarrGroup.open(output.toString());
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
    ZarrGroup z = ZarrGroup.open(output.toString());

    Path omePath = output.resolve("OME");
    ZarrGroup omeGroup = ZarrGroup.open(omePath.toString());
    List<String> groupMap =
      (List<String>) omeGroup.getAttributes().get("series");
    assertEquals(groupMap.size(), 2);
    assertEquals(groupMap.get(0), "0");
    assertEquals(groupMap.get(1), "1");

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

    OME ome = getOMEMetadata();
    assertEquals(2, ome.sizeOfImageList());
    for (int i=0; i<ome.sizeOfImageList(); i++) {
      assertNotNull(ome.getImage(i).getPixels().getMetadataOnly());
    }
  }

  /**
   * Test single beginning -series conversion.
   */
  @Test
  public void testSingleBeginningSeries() throws Exception {
    input = fake("series", "2");
    assertTool("-s", "0");
    ZarrGroup z = ZarrGroup.open(output.toString());

    Path omePath = output.resolve("OME");
    ZarrGroup omeGroup = ZarrGroup.open(omePath.toString());
    List<String> groupMap =
      (List<String>) omeGroup.getAttributes().get("series");
    assertEquals(groupMap.size(), 1);
    assertEquals(groupMap.get(0), "0");

    OME ome = getOMEMetadata();
    assertEquals(1, ome.sizeOfImageList());

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
    ZarrGroup z = ZarrGroup.open(output.toString());

    Path omePath = output.resolve("OME");
    ZarrGroup omeGroup = ZarrGroup.open(omePath.toString());
    List<String> groupMap =
      (List<String>) omeGroup.getAttributes().get("series");
    assertEquals(groupMap.size(), 1);
    assertEquals(groupMap.get(0), "0");

    OME ome = getOMEMetadata();
    assertEquals(1, ome.sizeOfImageList());

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
    ZarrGroup z = ZarrGroup.open(output.toString());

    Path omePath = output.resolve("OME");
    ZarrGroup omeGroup = ZarrGroup.open(omePath.toString());
    List<String> groupMap =
      (List<String>) omeGroup.getAttributes().get("series");
    assertEquals(groupMap.size(), 1);
    assertEquals(groupMap.get(0), "0");

    OME ome = getOMEMetadata();
    assertEquals(1, ome.sizeOfImageList());

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
    ZarrGroup z = ZarrGroup.open(output.toString());

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
    ZarrGroup z = ZarrGroup.open(output.toString());

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
    ZarrGroup z = ZarrGroup.open(output.toString());

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

  /**
   * Test the progress listener API.
   */
  @Test
  public void testProgressListener() throws Exception {
    input = fake("sizeX", "8192", "sizeY", "8192", "sizeZ", "5");

    Converter progressConverter = new Converter();
    TestProgressListener listener = new TestProgressListener();
    progressConverter.setProgressListener(listener);

    try {
      CommandLine.call(progressConverter,
        new String[] {input.toString(), output.toString()});
    }
    catch (RuntimeException rt) {
      throw rt;
    }
    catch (Throwable t) {
      throw new RuntimeException(t);
    }

    Integer[] expectedChunkCounts = new Integer[] {320, 80, 20, 5, 5, 5};
    Integer[] chunkCounts = listener.getChunkCounts();
    assertArrayEquals(expectedChunkCounts, chunkCounts);
    long totalChunkCount = 0;
    for (Integer t : expectedChunkCounts) {
      totalChunkCount += t;
    }
    assertEquals(totalChunkCount, listener.getTotalChunkCount());
    assertEquals(totalChunkCount, listener.getSeriesChunkCount());
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
    ZarrGroup z = ZarrGroup.open(output.toString());

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
   * @return an array of dimensions ordered XYZCT
   * and the dimensionOrder to be used
   */
  static Stream<Arguments> getDimensions() {
    return Stream.of(
      Arguments.of(new int[]{512, 512, 64, 1, 1}, "XYZCT"),
      Arguments.of(new int[]{512, 512, 32, 3, 100}, "XYZCT"),
      Arguments.of(new int[]{512, 512, 16, 1, 1}, "XYCTZ"),
      Arguments.of(new int[]{512, 512, 32, 3, 100}, "XYCTZ")
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
    ZarrGroup z = ZarrGroup.open(output.toString());

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
    ZarrGroup z = ZarrGroup.open(output.toString());

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
    Path omexml = output.resolve("OME").resolve("METADATA.ome.xml");
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

    ZarrGroup z = ZarrGroup.open(output.resolve("0").toString());
    List<Map<String, Object>> multiscales =
          (List<Map<String, Object>>) z.getAttributes().get("multiscales");
    assertEquals(1, multiscales.size());
    Map<String, Object> multiscale = multiscales.get(0);
    assertEquals("0.4", multiscale.get("version"));

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
   * Make sure an informative exception is thrown when trying to use
   * OpenCV to downsample int8 data.
   * See https://github.com/opencv/opencv/issues/7862
   */
  @Test
  public void testUnsupportedOpenCVType() throws Exception {
    input = fake("pixelType", "int8");
    assertThrows(ExecutionException.class, () -> {
      assertTool("--downsample-type", "LINEAR");
    });
  }

  /**
   * Make sure an informative exception is thrown when trying to use
   * OpenCV to downsample int32 data. Uses the API instead of
   * command line arguments.
   * See https://github.com/opencv/opencv/issues/7862
   */
  @Test
  public void testUnsupportedOpenCVTypeAPI() throws Exception {
    input = fake("pixelType", "int32");
    Converter apiConverter = new Converter();
    CommandLine cmd = new CommandLine(apiConverter);
    cmd.parseArgs(); // this sets default values for all options

    apiConverter.setInputPath(input.toString());
    apiConverter.setOutputPath(output.toString());
    apiConverter.setDownsampling(Downsampling.AREA);

    assertThrows(UnsupportedOperationException.class, () -> {
      apiConverter.call();
    });
  }

  /**
   * Test that nested storage works equivalently.
   *
   * @param nested whether to use "/" or "." as the chunk separator.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testNestedStorage(boolean nested) throws IOException {
    input = fake();
    assertTool(nested ? "--nested" : "--no-nested");
    if (nested) {
      assertTrue(output.resolve("0/0/0/0/0/0/0").toFile().exists());
    }
    else {
      assertTrue(output.resolve("0/0/0.0.0.0.0").toFile().exists());
    }
  }

  /**
   * Convert a plate with the --no-hcs option.
   * The output should not be compliant with OME Zarr HCS.
   */
  @Test
  public void testNoHCSOption() throws Exception {
    input = fake(
      "plates", "1", "plateAcqs", "1",
      "plateRows", "2", "plateCols", "3", "fields", "2");
    assertTool("--no-hcs");

    ZarrGroup z = ZarrGroup.open(output);

    Path omePath = output.resolve("OME");
    ZarrGroup omeGroup = ZarrGroup.open(omePath.toString());
    List<String> groupMap =
      (List<String>) omeGroup.getAttributes().get("series");
    assertEquals(groupMap.size(), 12);
    for (int i=0; i<12; i++) {
      assertEquals(groupMap.get(i), String.valueOf(i));
    }

    // Check dimensions and block size
    ZarrArray series0 = z.openArray("0/0");
    assertArrayEquals(new int[] {1, 1, 1, 512, 512}, series0.getShape());
    assertArrayEquals(new int[] {1, 1, 1, 512, 512}, series0.getChunks());
    // 12 series + OME group
    assertEquals(13, z.getGroupKeys().size());

    // Check OME metadata
    OME ome = getOMEMetadata();
    assertEquals(0, ome.sizeOfPlateList());
  }

  /**
   * Make sure conversion fails when multiple plates are present.
   */
  @Test
  public void testMultiPlates() throws Exception {
    input = fake(
      "plates", "2", "plateAcqs", "1",
      "plateRows", "2", "plateCols", "3", "fields", "2");
    assertThrows(ExecutionException.class, () -> {
      assertTool();
    });
  }

  /**
   * Convert a plate with default options.
   * The output should be compliant with OME Zarr HCS.
   */
  @Test
  public void testHCSMetadata() throws Exception {
    input = fake(
      "plates", "1", "plateAcqs", "1",
      "plateRows", "2", "plateCols", "3", "fields", "2");
    assertTool();

    ZarrGroup z = ZarrGroup.open(output);

    int rowCount = 2;
    int colCount = 3;
    int fieldCount = 2;

    Path omePath = output.resolve("OME");
    ZarrGroup omeGroup = ZarrGroup.open(omePath.toString());
    List<String> groupMap =
      (List<String>) omeGroup.getAttributes().get("series");
    assertEquals(groupMap.size(), 12);
    int index = 0;
    for (int r=0; r<rowCount; r++) {
      for (int c=0; c<colCount; c++) {
        for (int f=0; f<fieldCount; f++) {
          String groupPath = (char) (r + 'A') + "/" + (c + 1) + "/" + f;
          assertEquals(groupMap.get(index++), groupPath);
        }
      }
    }

    Map<String, List<String>> plateMap = new HashMap<String, List<String>>();
    plateMap.put("A", Arrays.asList("1", "2", "3"));
    plateMap.put("B", Arrays.asList("1", "2", "3"));
    checkPlateGroupLayout(output, rowCount, colCount,
      plateMap, fieldCount, 512, 512);

    // check plate/well level metadata
    Map<String, Object> plate =
        (Map<String, Object>) z.getAttributes().get("plate");
    assertEquals(fieldCount, ((Number) plate.get("field_count")).intValue());
    assertEquals("0.4", plate.get("version"));

    List<Map<String, Object>> acquisitions =
      (List<Map<String, Object>>) plate.get("acquisitions");
    List<Map<String, Object>> rows =
      (List<Map<String, Object>>) plate.get("rows");
    List<Map<String, Object>> columns =
      (List<Map<String, Object>>) plate.get("columns");
    List<Map<String, Object>> wells =
      (List<Map<String, Object>>) plate.get("wells");

    assertEquals(1, acquisitions.size());
    assertEquals(0, acquisitions.get(0).get("id"));

    assertEquals(rows.size(), rowCount);
    assertEquals(columns.size(), colCount);

    assertEquals(rows.size() * columns.size(), wells.size());
    for (int row=0; row<rows.size(); row++) {
      for (int col=0; col<columns.size(); col++) {
        int well = row * columns.size() + col;
        String rowName = rows.get(row).get("name").toString();
        String colName = columns.get(col).get("name").toString();
        assertEquals(rowName + "/" + colName, wells.get(well).get("path"));
      }
    }

    // check well metadata
    for (Map<String, Object> row : rows) {
      String rowName = (String) row.get("name");
      for (Map<String, Object> column : columns) {
        String columnName = (String) column.get("name");
        ZarrGroup wellGroup = ZarrGroup.open(
            output.resolve(rowName).resolve(columnName));
        Map<Integer, Integer> wellAcquisitions =
            new HashMap<Integer, Integer>();
        wellAcquisitions.put(0, 0);
        wellAcquisitions.put(1, 0);
        checkWell(wellGroup, fieldCount, wellAcquisitions);
      }
    }

    // check OME metadata
    OME ome = getOMEMetadata();
    assertEquals(1, ome.sizeOfPlateList());
  }

  /**
   * Check for OMERO rendering metadata.
   */
  @Test
  public void testOMERO() throws Exception {
    input = getTestFile("colors.ome.xml");
    assertTool();

    String[][] names = {{"orange"}, {"green", "blue"}, {"blue"}};
    String[][] colors = {{"FF7F00"}, {"00FF00", "0000FF"}, {"808080"}};

    for (int i=0; i<names.length; i++) {
      ZarrGroup z =
        ZarrGroup.open(output.resolve(String.valueOf(i)).toString());
      Map<String, Object> omero =
            (Map<String, Object>) z.getAttributes().get("omero");

      Map<String, Object> rdefs = (Map<String, Object>) omero.get("rdefs");
      assertEquals(
        names[i].length == 1 ? "greyscale" : "color", rdefs.get("model"));

      List<Map<String, Object>> channels =
            (List<Map<String, Object>>) omero.get("channels");
      assertEquals(names[i].length, channels.size());

      for (int c=0; c<names[i].length; c++) {
        Map<String, Object> channel = channels.get(c);
        assertEquals(names[i][c], channel.get("label"));
        assertEquals(colors[i][c], channel.get("color"));
        assertEquals(true, channel.get("active"));
      }
    }
  }

  /**
   * Check for OMERO rendering metadata on a 4 channel image.
   */
  @Test
  public void testOMEROMultiC() throws Exception {
    input = getTestFile("multichannel-colors.ome.xml");
    assertTool();

    String[] names = {"orange", "green", "blue", "red"};
    String[] colors = {"FF7F00", "00FF00", "0000FF", "FF0000"};

    ZarrGroup z = ZarrGroup.open(output.resolve("0").toString());
    Map<String, Object> omero =
          (Map<String, Object>) z.getAttributes().get("omero");

    Map<String, Object> rdefs = (Map<String, Object>) omero.get("rdefs");
    assertEquals("color", rdefs.get("model"));

    List<Map<String, Object>> channels =
          (List<Map<String, Object>>) omero.get("channels");
    assertEquals(names.length, channels.size());

    for (int c=0; c<channels.size(); c++) {
      Map<String, Object> channel = channels.get(c);
      assertEquals(names[c], channel.get("label"));
      assertEquals(colors[c], channel.get("color"));
      assertEquals(c < 3, channel.get("active"));
    }
  }

  /**
   * Check for OMERO rendering metadata on a 3 channel image with
   * no channel names.
   */
  @Test
  public void testOMEROChannelNames() throws Exception {
    input = getTestFile("multichannel-no-names.ome.xml");
    assertTool();

    String[] names = {"300.0", "600.0", "350.0"};

    ZarrGroup z = ZarrGroup.open(output.resolve("0").toString());
    Map<String, Object> omero =
          (Map<String, Object>) z.getAttributes().get("omero");

    Map<String, Object> rdefs = (Map<String, Object>) omero.get("rdefs");

    List<Map<String, Object>> channels =
          (List<Map<String, Object>>) omero.get("channels");
    assertEquals(names.length, channels.size());

    for (int c=0; c<channels.size(); c++) {
      Map<String, Object> channel = channels.get(c);
      assertEquals(names[c], channel.get("label"));
    }
  }

  /**
   * Make sure OMERO metadata is not written when the
   * "--no-minmax" flag is used.
   */
  @Test
  public void testNoOMERO() throws Exception {
    input = getTestFile("colors.ome.xml");
    assertTool("--no-minmax");

    for (int i=0; i<3; i++) {
      ZarrGroup z =
        ZarrGroup.open(output.resolve(String.valueOf(i)).toString());
      assertNull(z.getAttributes().get("omero"));
    }
  }

  /**
   * Convert a plate with default options.
   * The output should be compliant with OME Zarr HCS.
   */
  @Test
  public void testHCSMetadataNoAcquisitions() throws Exception {
    input = getTestFile("A1-B1-only-no-acqs.xml");
    assertTool();

    ZarrGroup z = ZarrGroup.open(output);

    int rowCount = 8;
    int colCount = 12;
    int fieldCount = 1;

    // Only two rows are filled out with one column each (two Wells total)
    Map<String, List<String>> plateMap = new HashMap<String, List<String>>();
    plateMap.put("A", Arrays.asList("1"));
    plateMap.put("B", Arrays.asList("1"));
    checkPlateGroupLayout(output, rowCount, colCount,
      plateMap, fieldCount, 2, 2);

    // check plate/well level metadata
    Map<String, Object> plate =
        (Map<String, Object>) z.getAttributes().get("plate");
    assertEquals(fieldCount, ((Number) plate.get("field_count")).intValue());
    assertEquals("0.4", plate.get("version"));

    List<Map<String, Object>> rows =
      (List<Map<String, Object>>) plate.get("rows");
    List<Map<String, Object>> columns =
      (List<Map<String, Object>>) plate.get("columns");
    List<Map<String, Object>> wells =
      (List<Map<String, Object>>) plate.get("wells");

    assertFalse(plate.containsKey("acquisitions"));

    assertEquals(rows.size(), rowCount);
    assertEquals(columns.size(), colCount);

    assertEquals(2, wells.size());
    for (Map<String, Object> well : wells) {
      int row = ((Number) well.get("rowIndex")).intValue();
      int col = ((Number) well.get("columnIndex")).intValue();
      String rowName = rows.get(row).get("name").toString();
      String colName = columns.get(col).get("name").toString();

      String wellPath = well.get("path").toString();
      assertEquals(rowName + "/" + colName, wellPath);

      ZarrGroup wellGroup = ZarrGroup.open(output.resolve(wellPath));
      Map<Integer, Integer> wellAcquisitions = new HashMap<Integer, Integer>();
      checkWell(wellGroup, fieldCount, wellAcquisitions);
    }

    // check OME metadata
    OME ome = getOMEMetadata();
    assertEquals(1, ome.sizeOfPlateList());
  }

  /**
   * Plate defined with no linked images.
   */
  @Test
  public void testEmptyPlate() throws Exception {
    input = getTestFile("empty-plate.ome.xml");
    assertTool();

    ZarrGroup z = ZarrGroup.open(output);

    // Check dimensions and block size for consistency
    // with non-HCS layout
    ZarrArray series0 = z.openArray("0/0");
    assertArrayEquals(new int[] {1, 1, 1, 4, 6}, series0.getShape());
    assertArrayEquals(new int[] {1, 1, 1, 4, 6}, series0.getChunks());

    // Check OME metadata to make sure the empty plate was preserved
    OME ome = getOMEMetadata();
    assertEquals(1, ome.sizeOfPlateList());
  }

  /**
   * 96 well plate with only well E6.
   */
  @Test
  public void testSingleWell() throws IOException {
    input = getTestFile("E6-only.ome.xml");
    assertTool();

    ZarrGroup z = ZarrGroup.open(output);

    int rowCount = 8;
    int colCount = 12;
    int fieldCount = 1;

    Map<String, List<String>> plateMap = new HashMap<String, List<String>>();
    plateMap.put("E", Arrays.asList("6"));

    checkPlateGroupLayout(output, rowCount, colCount,
      plateMap, fieldCount, 2, 2);

    // check plate/well level metadata
    Map<String, Object> plate =
        (Map<String, Object>) z.getAttributes().get("plate");
    assertEquals(fieldCount, ((Number) plate.get("field_count")).intValue());
    assertEquals("0.4", plate.get("version"));

    List<Map<String, Object>> acquisitions =
      (List<Map<String, Object>>) plate.get("acquisitions");
    List<Map<String, Object>> rows =
      (List<Map<String, Object>>) plate.get("rows");
    List<Map<String, Object>> columns =
      (List<Map<String, Object>>) plate.get("columns");
    List<Map<String, Object>> wells =
      (List<Map<String, Object>>) plate.get("wells");

    assertEquals(1, acquisitions.size());
    assertEquals(0, acquisitions.get(0).get("id"));

    assertEquals(rows.size(), rowCount);
    assertEquals(columns.size(), colCount);

    assertEquals(1, wells.size());
    Map<String, Object> well = wells.get(0);
    String wellPath = (String) well.get("path");
    assertEquals("E/6", wellPath);
    assertEquals(4, ((Number) well.get("rowIndex")).intValue());
    assertEquals(5, ((Number) well.get("columnIndex")).intValue());

    // check well metadata
    ZarrGroup wellGroup = ZarrGroup.open(output.resolve(wellPath));
    Map<Integer, Integer> wellAcquisitions = new HashMap<Integer, Integer>();
    wellAcquisitions.put(0, 0);
    checkWell(wellGroup, fieldCount, wellAcquisitions);
  }

  /**
   * 96 well plate with only wells C4 and H2 with and without the "Rows"
   * attribute populated on the plate, as well as with and without the "Columns"
   * attribute populated on the plate.
   * @param resourceName parameterized XML file to use for the test case
   */
  @ParameterizedTest
  @ValueSource(strings = {
    "C12-H2-only.ome.xml",
    "C12-H2-only-no-rows.ome.xml",
    "C12-H2-only-no-columns.ome.xml"
  })
  public void testTwoWells(String resourceName) throws IOException {
    input = getTestFile(resourceName);
    assertTool();

    ZarrGroup z = ZarrGroup.open(output);

    int rowCount = 8;
    int colCount = 12;
    int fieldCount = 1;

    Map<String, List<String>> plateMap = new HashMap<String, List<String>>();
    plateMap.put("C", Arrays.asList("12"));
    plateMap.put("H", Arrays.asList("2"));
    checkPlateGroupLayout(output, rowCount, colCount,
      plateMap, fieldCount, 2, 2);

    // check plate/well level metadata
    Map<String, Object> plate =
        (Map<String, Object>) z.getAttributes().get("plate");
    assertEquals(fieldCount, ((Number) plate.get("field_count")).intValue());
    assertEquals("0.4", plate.get("version"));

    List<Map<String, Object>> acquisitions =
      (List<Map<String, Object>>) plate.get("acquisitions");
    List<Map<String, Object>> rows =
      (List<Map<String, Object>>) plate.get("rows");
    List<Map<String, Object>> columns =
      (List<Map<String, Object>>) plate.get("columns");
    List<Map<String, Object>> wells =
      (List<Map<String, Object>>) plate.get("wells");

    assertEquals(1, acquisitions.size());
    assertEquals(0, acquisitions.get(0).get("id"));

    assertEquals(rows.size(), rowCount);
    assertEquals(columns.size(), colCount);

    assertEquals(2, wells.size());
    Map<String, Object> well = wells.get(0);
    String wellPath = (String) well.get("path");
    assertEquals("C/12", wellPath);
    assertEquals(2, ((Number) well.get("rowIndex")).intValue());
    assertEquals(11, ((Number) well.get("columnIndex")).intValue());
    ZarrGroup wellGroup = ZarrGroup.open(output.resolve(wellPath));
    Map<Integer, Integer> wellAcquisitions = new HashMap<Integer, Integer>();
    wellAcquisitions.put(0, 0);
    checkWell(wellGroup, fieldCount, wellAcquisitions);

    well = wells.get(1);
    wellPath = (String) well.get("path");
    assertEquals("H/2", wellPath);
    assertEquals(7, ((Number) well.get("rowIndex")).intValue());
    assertEquals(1, ((Number) well.get("columnIndex")).intValue());
    wellGroup = ZarrGroup.open(output.resolve(wellPath));
    checkWell(wellGroup, fieldCount, wellAcquisitions);
  }

  /**
   * 96 well plate with all wells in row F.
   */
  @Test
  public void testOnePlateRow() throws IOException {
    input = getTestFile("row-F-only.ome.xml");
    assertTool();

    ZarrGroup z = ZarrGroup.open(output);

    int rowCount = 8;
    int colCount = 12;
    int fieldCount = 1;

    Map<String, List<String>> plateMap = new HashMap<String, List<String>>();
    plateMap.put("F", Arrays.asList(
      "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"));
    checkPlateGroupLayout(output, rowCount, colCount,
      plateMap, fieldCount, 2, 2);

    // check plate/well level metadata
    Map<String, Object> plate =
        (Map<String, Object>) z.getAttributes().get("plate");
    assertEquals(fieldCount, ((Number) plate.get("field_count")).intValue());
    assertEquals("0.4", plate.get("version"));

    List<Map<String, Object>> acquisitions =
      (List<Map<String, Object>>) plate.get("acquisitions");
    List<Map<String, Object>> rows =
      (List<Map<String, Object>>) plate.get("rows");
    List<Map<String, Object>> columns =
      (List<Map<String, Object>>) plate.get("columns");
    List<Map<String, Object>> wells =
      (List<Map<String, Object>>) plate.get("wells");

    assertEquals(1, acquisitions.size());
    assertEquals(0, acquisitions.get(0).get("id"));

    assertEquals(rows.size(), rowCount);
    assertEquals(columns.size(), colCount);

    assertEquals(colCount, wells.size());
    for (int col=0; col<wells.size(); col++) {
      Map<String, Object> well = wells.get(col);
      String wellPath = (String) well.get("path");
      String colName = columns.get(col).get("name").toString();
      assertEquals("F/" + colName, wellPath);
      assertEquals(5, ((Number) well.get("rowIndex")).intValue());
      assertEquals(col, ((Number) well.get("columnIndex")).intValue());
      ZarrGroup wellGroup = ZarrGroup.open(output.resolve(wellPath));
      Map<Integer, Integer> wellAcquisitions = new HashMap<Integer, Integer>();
      wellAcquisitions.put(0, 0);
      checkWell(wellGroup, fieldCount, wellAcquisitions);
    }
  }

  /**
   * 96 well plate with only well E6 with two acquisitions.
   */
  @Test
  public void testSingleWellTwoAcquisitions() throws IOException {
    input = getTestFile("E6-only-two-acqs.xml");
    assertTool();

    ZarrGroup z = ZarrGroup.open(output);

    int rowCount = 8;
    int colCount = 12;
    int fieldCount = 3;

    Map<String, List<String>> plateMap = new HashMap<String, List<String>>();
    plateMap.put("E", Arrays.asList("6"));

    checkPlateGroupLayout(output, rowCount, colCount,
      plateMap, fieldCount, 2, 2);

    // check plate/well level metadata
    Map<String, Object> plate =
        (Map<String, Object>) z.getAttributes().get("plate");
    assertEquals(fieldCount, ((Number) plate.get("field_count")).intValue());
    assertEquals("0.4", plate.get("version"));

    List<Map<String, Object>> acquisitions =
      (List<Map<String, Object>>) plate.get("acquisitions");
    List<Map<String, Object>> rows =
      (List<Map<String, Object>>) plate.get("rows");
    List<Map<String, Object>> columns =
      (List<Map<String, Object>>) plate.get("columns");
    List<Map<String, Object>> wells =
      (List<Map<String, Object>>) plate.get("wells");

    assertEquals(2, acquisitions.size());
    assertEquals(0, acquisitions.get(0).get("id"));
    assertEquals(1, acquisitions.get(1).get("id"));

    assertEquals(rows.size(), rowCount);
    assertEquals(columns.size(), colCount);

    assertEquals(1, wells.size());
    Map<String, Object> well = wells.get(0);
    String wellPath = (String) well.get("path");
    assertEquals("E/6", wellPath);
    assertEquals(4, ((Number) well.get("rowIndex")).intValue());
    assertEquals(5, ((Number) well.get("columnIndex")).intValue());

    // check well metadata
    ZarrGroup wellGroup = ZarrGroup.open(output.resolve(wellPath));
    Map<Integer, Integer> wellAcquisitions = new HashMap<Integer, Integer>();
    wellAcquisitions.put(0, 0);
    wellAcquisitions.put(1, 1);
    wellAcquisitions.put(2, null);
    checkWell(wellGroup, fieldCount, wellAcquisitions);
  }

  /**
   * Convert an RGB image.  Ensure that the Channels are correctly split.
   */
  @Test
  public void testRGBChannelSeparator() throws Exception {
    input = fake("sizeC", "3", "rgb", "3");
    assertTool();

    OME ome = getOMEMetadata();
    assertEquals(1, ome.sizeOfImageList());
    Pixels pixels = ome.getImage(0).getPixels();
    assertEquals(3, pixels.sizeOfChannelList());
  }

  /**
   * Check that a root group and attributes are created and populated.
   */
  @Test
  public void testRootGroup() throws Exception {
    input = fake();
    assertTool();

    assertTrue(Files.exists(output.resolve(".zattrs")));
    assertTrue(Files.exists(output.resolve(".zgroup")));
  }

  /**
   * Convert with the --no-root-group option.  Conversion should succeed but
   * no root group or attributes should be created or populated.
   */
  @Test
  public void testNoRootGroupOption() throws Exception {
    input = fake();
    assertTool("--no-root-group");

    assertTrue(!Files.exists(output.resolve(".zattrs")));
    assertTrue(!Files.exists(output.resolve(".zgroup")));
  }

  /**
   * Check that a OME metadata is exported.
   */
  @Test
  public void testOmeMetaExportOption() throws Exception {
    input = fake();
    assertTool();

    assertTrue(Files.exists(
      output.resolve("OME").resolve("METADATA.ome.xml")));
  }

  /**
   * Convert with the --no-ome-meta-export option.  Conversion should succeed,
   * but no OME metadata should be exported.
   */
  @Test
  public void testNoOmeMetaExportOption() throws Exception {
    input = fake();
    assertTool("--no-ome-meta-export");

    assertTrue(!Files.exists(
      output.resolve("OME").resolve("METADATA.ome.xml")));
    assertTrue(!Files.exists(
      output.resolve("OME").resolve(".zattrs")));
  }

  /**
   * Convert with the --use-existing-resolutions option.  Conversion should
   * produce multiscales matching the input resolution numbers and scale.
   */
  @Test
  public void testUseExistingResolutions() throws Exception {
    int resolutionCount = 3;
    int resolutionScale = 4;
    int sizeX = 2048;
    int sizeY = 1024;
    input = fake("sizeX", ""+sizeX+"", "sizeY", ""+sizeY+"",
        "resolutions", ""+resolutionCount+"",
        "resolutionScale", ""+resolutionScale+"");
    assertTool("--use-existing-resolutions");
    ZarrGroup z =
        ZarrGroup.open(output.resolve("0").toString());
    List<Map<String, Object>> multiscales = (List<Map<String, Object>>)
            z.getAttributes().get("multiscales");

    Map<String, Object> multiscale = multiscales.get(0);
    List<Map<String, Object>> datasets =
              (List<Map<String, Object>>) multiscale.get("datasets");
    assertEquals(resolutionCount, datasets.size());
    for (int i = 0; i < resolutionCount; i++) {
      String path = (String) datasets.get(i).get("path");
      ZarrArray series = z.openArray(path);
      assertArrayEquals(new int[] {1, 1, 1, sizeY, sizeX}, series.getShape());
      sizeY /= resolutionScale;
      sizeX /= resolutionScale;
    }
  }

  /**
   * Convert without the --use-existing-resolutions option.  Conversion should
   * ignore the input resolution numbers and scale.
   */
  @Test
  public void testIgnoreExistingResolutions() throws Exception {
    int resolutionCount = 3;
    int resolutionScale = 4;
    int sizeX = 2048;
    int sizeY = 1024;
    input = fake("sizeX", ""+sizeX+"", "sizeY", ""+sizeY+"",
        "resolutions", ""+resolutionCount+"",
        "resolutionScale", ""+resolutionScale+"");
    assertTool();
    ZarrGroup z =
        ZarrGroup.open(output.resolve("0").toString());
    List<Map<String, Object>> multiscales = (List<Map<String, Object>>)
            z.getAttributes().get("multiscales");

    Map<String, Object> multiscale = multiscales.get(0);
    List<Map<String, Object>> datasets =
              (List<Map<String, Object>>) multiscale.get("datasets");
    assertEquals(4, datasets.size());
    for (int i = 0; i < 4; i++) {
      String path = (String) datasets.get(i).get("path");
      ZarrArray series = z.openArray(path);
      assertArrayEquals(new int[] {1, 1, 1, sizeY, sizeX}, series.getShape());
      sizeY /= 2;
      sizeX /= 2;
    }
  }

  /**
   * Convert with too many resolutions for the input XY size.
   * The resolution count should automatically be reduced.
   */
  @Test
  public void testTooManyResolutions() throws Exception {
    int resolutionCount = 20;
    int expectedResolutionCount = 6;
    int sizeX = 8192;
    int sizeY = 8192;
    input = fake("sizeX", String.valueOf(sizeX),
      "sizeY", String.valueOf(sizeY));
    assertTool("--resolutions", String.valueOf(resolutionCount));

    ZarrGroup z =
        ZarrGroup.open(output.resolve("0").toString());
    List<Map<String, Object>> multiscales = (List<Map<String, Object>>)
            z.getAttributes().get("multiscales");

    Map<String, Object> multiscale = multiscales.get(0);
    List<Map<String, Object>> datasets =
              (List<Map<String, Object>>) multiscale.get("datasets");
    assertEquals(expectedResolutionCount, datasets.size());
    for (int i = 0; i < datasets.size(); i++) {
      String path = (String) datasets.get(i).get("path");
      ZarrArray series = z.openArray(path);
      assertArrayEquals(new int[] {1, 1, 1, sizeY, sizeX}, series.getShape());
      sizeY /= 2;
      sizeX /= 2;
    }
  }

  /**
   * Convert with the --chunk_depth option. Conversion should produce
   * chunk sizes matching the provided input
   *
   * @param xyzct array of dimensions to be used for the input file
   * @param dimOrder the dimensionOrder to be used for the input file
   */
  @ParameterizedTest
  @MethodSource("getDimensions")
  public void testChunkWriting(int[] xyzct, String dimOrder) throws Exception {
    int chunkDepth = 16;
    input = fake("sizeX", ""+xyzct[0]+"", "sizeY", ""+xyzct[1]+"",
        "sizeZ", ""+xyzct[2]+"", "sizeC", ""+xyzct[3]+"",
        "sizeT", ""+xyzct[4]+"", "dimOrder", dimOrder);
    assertTool("--chunk_depth", ""+chunkDepth+"");
    ZarrGroup z = ZarrGroup.open(output.resolve("0").toString());
    List<Map<String, Object>> multiscales = (List<Map<String, Object>>)
            z.getAttributes().get("multiscales");

    Map<String, Object> multiscale = multiscales.get(0);
    List<Map<String, Object>> datasets =
              (List<Map<String, Object>>) multiscale.get("datasets");

    for (int i = 0; i < datasets.size(); i++) {
      String path = (String) datasets.get(i).get("path");
      ZarrArray series = z.openArray(path);

      assertArrayEquals(new int[] {1, 1, chunkDepth, xyzct[0], xyzct[1]},
          series.getChunks());
      xyzct[0] /= 2;
      xyzct[1] /= 2;
    }
  }

  /**
   * Convert with the --chunk_depth option larger than sizeZ. Conversion
   * should produce chunk sizes matching the sizeZ
   */
  @Test
  public void testChunkSizeToBig() throws Exception {
    int sizeZ = 8;
    int chunkDepth = 16;
    int sizeX = 512;
    int sizeY = 512;
    input = fake("sizeX", ""+sizeX+"", "sizeY", ""+sizeY+"",
        "sizeZ", ""+sizeZ+"");
    assertTool("--chunk_depth", ""+chunkDepth+"");
    ZarrGroup z = ZarrGroup.open(output.resolve("0").toString());
    List<Map<String, Object>> multiscales = (List<Map<String, Object>>)
            z.getAttributes().get("multiscales");

    Map<String, Object> multiscale = multiscales.get(0);
    List<Map<String, Object>> datasets =
              (List<Map<String, Object>>) multiscale.get("datasets");

    for (int i = 0; i < datasets.size(); i++) {
      String path = (String) datasets.get(i).get("path");
      ZarrArray series = z.openArray(path);

      assertArrayEquals(new int[] {1, 1, sizeZ, sizeX, sizeY},
          series.getChunks());
      sizeX /= 2;
      sizeY /= 2;
    }
  }

  /**
   * Convert an image to produce  a smallest resolution of dimensions 32x32.
   */
  @Test
  public void testMinSizeExact() throws Exception {
    input = fake();
    assertTool("--target-min-size", "32");

    ZarrGroup z = ZarrGroup.open(output.resolve("0").toString());
    List<Map<String, Object>> multiscales = (List<Map<String, Object>>)
            z.getAttributes().get("multiscales");

    Map<String, Object> multiscale = multiscales.get(0);
    List<Map<String, Object>> datasets =
              (List<Map<String, Object>>) multiscale.get("datasets");
    assertEquals(datasets.size(), 5);

    ZarrArray array = z.openArray("0");
    assertArrayEquals(new int[] {1, 1, 1, 512, 512}, array.getShape());
    array = z.openArray("4");
    assertArrayEquals(new int[] {1, 1, 1, 32, 32}, array.getShape());
  }

  /**
   * Convert an image to produce a smallest resolution of dimensions 16x16.
   */
  @Test
  public void testMinSizeThreshold() throws Exception {
    input = fake();
    assertTool("--target-min-size", "30");

    ZarrGroup z = ZarrGroup.open(output.resolve("0").toString());
    List<Map<String, Object>> multiscales = (List<Map<String, Object>>)
            z.getAttributes().get("multiscales");

    Map<String, Object> multiscale = multiscales.get(0);
    List<Map<String, Object>> datasets =
              (List<Map<String, Object>>) multiscale.get("datasets");
    assertEquals(datasets.size(), 6);

    ZarrArray array = z.openArray("0");
    assertArrayEquals(new int[] {1, 1, 1, 512, 512}, array.getShape());
    array = z.openArray("5");
    assertArrayEquals(new int[] {1, 1, 1, 16, 16}, array.getShape());
  }

  /**
   * Convert an image to produce a smallest resolution of dimensions 32x16.
   */
  @Test
  public void testMinSizeAsymmetricExact() throws Exception {
    input = fake("sizeX", "1024");
    assertTool("--target-min-size", "32");

    ZarrGroup z = ZarrGroup.open(output.resolve("0").toString());
    List<Map<String, Object>> multiscales = (List<Map<String, Object>>)
            z.getAttributes().get("multiscales");

    Map<String, Object> multiscale = multiscales.get(0);
    List<Map<String, Object>> datasets =
              (List<Map<String, Object>>) multiscale.get("datasets");
    assertEquals(datasets.size(), 6);

    ZarrArray array = z.openArray("0");
    assertArrayEquals(new int[] {1, 1, 1, 512, 1024}, array.getShape());
    array = z.openArray("5");
    assertArrayEquals(new int[] {1, 1, 1, 16, 32}, array.getShape());
  }

  /**
   * Convert an image to produce a smallest resolution of dimensions 8x16.
   */
  @Test
  public void testMinSizeAsymmetricThreshold() throws Exception {
    input = fake("sizeY", "1024");
    assertTool("--target-min-size", "30");

    ZarrGroup z = ZarrGroup.open(output.resolve("0").toString());
    List<Map<String, Object>> multiscales = (List<Map<String, Object>>)
            z.getAttributes().get("multiscales");

    Map<String, Object> multiscale = multiscales.get(0);
    List<Map<String, Object>> datasets =
              (List<Map<String, Object>>) multiscale.get("datasets");
    assertEquals(datasets.size(), 7);

    ZarrArray array = z.openArray("0");
    assertArrayEquals(new int[] {1, 1, 1, 1024, 512}, array.getShape());
    array = z.openArray("6");
    assertArrayEquals(new int[] {1, 1, 1, 16, 8}, array.getShape());
  }

  /**
   * Check that --keep-memo-files works as expected.
   */
  @Test
  public void testMemoFiles() throws Exception {
    // make sure a memo file is created
    // by default the fake init time is too small
    input = fake("sleepInitFile", "1000");
    assertTool("--debug", "--keep-memo-files");

    Memoizer m = new Memoizer();
    File memoFile = m.getMemoFile(input.toString());
    assertTrue(memoFile.exists());

    // make sure the existing memo file is not deleted
    assertTool("--overwrite");
    assertTrue(memoFile.exists());

    // now delete the memo file and make sure that
    // a clean conversion doesn't leave a memo file around
    memoFile.delete();
    assertTool("--overwrite");
    assertFalse(memoFile.exists());
  }

  /**
   * Check that setting options via API instead of command line arguments
   * works as expected.
   */
  @Test
  public void testOptionsAPI() throws Exception {
    input = fake("series", "2", "sizeX", "4096", "sizeY", "4096");

    Converter apiConverter = new Converter();
    CommandLine cmd = new CommandLine(apiConverter);
    cmd.parseArgs(); // this sets default values for all options

    apiConverter.setInputPath(input.toString());
    apiConverter.setOutputPath(output.toString());
    apiConverter.setSeriesList(Collections.singletonList(1));
    apiConverter.setTileWidth(128);
    apiConverter.setTileHeight(128);

    apiConverter.call();

    ZarrGroup z = ZarrGroup.open(output.toString());

    Path omePath = output.resolve("OME");
    ZarrGroup omeGroup = ZarrGroup.open(omePath.toString());
    List<String> groupMap =
      (List<String>) omeGroup.getAttributes().get("series");
    assertEquals(groupMap.size(), 1);
    assertEquals(groupMap.get(0), "0");

    OME ome = getOMEMetadata();
    assertEquals(1, ome.sizeOfImageList());

    // Check series 1 dimensions and special pixels
    ZarrArray series0 = z.openArray("0/0");
    int[] shape = new int[] {1, 1, 1, 4096, 4096};
    assertArrayEquals(shape, series0.getShape());
    assertArrayEquals(
      new int[] {1, 1, 1,
      apiConverter.getTileHeight(),
      apiConverter.getTileWidth()},
      series0.getChunks());
    byte[] tile =
      new byte[apiConverter.getTileWidth() * apiConverter.getTileHeight()];
    shape[3] = 128;
    shape[4] = 128;
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
   * Check that setting and resetting options via API will reset options to
   * their default values.
   */
  @Test
  public void testResetOptions() throws Exception {
    input = fake("series", "2", "sizeX", "4096", "sizeY", "4096");

    Converter apiConverter = new Converter();
    CommandLine cmd = new CommandLine(apiConverter);
    cmd.parseArgs(); // this sets default values for all options

    apiConverter.setInputPath(input.toString());
    apiConverter.setOutputPath(output.toString());
    apiConverter.setSeriesList(Collections.singletonList(1));
    apiConverter.setTileWidth(128);
    apiConverter.setTileHeight(128);

    assertEquals(apiConverter.getInputPath(), input.toString());
    assertEquals(apiConverter.getOutputPath(), output.toString());
    assertEquals(apiConverter.getSeriesList(), Collections.singletonList(1));
    assertEquals(apiConverter.getTileWidth(), 128);
    assertEquals(apiConverter.getTileHeight(), 128);

    apiConverter.call(); // do a conversion

    cmd.parseArgs(); // this should reset default values for all options

    assertEquals(apiConverter.getInputPath(), null);

    apiConverter.setInputPath(input.toString());

    assertEquals(apiConverter.getInputPath(), input.toString());
    assertEquals(apiConverter.getOutputPath(), null);
    assertEquals(apiConverter.getSeriesList().size(), 0);
    assertEquals(apiConverter.getTileWidth(), 1024);
    assertEquals(apiConverter.getTileHeight(), 1024);
  }

  /**
   * @param root dataset root path
   * @param rowCount total rows the plate could contain
   * @param colCount total columns the plate could contain
   * @param validPaths map of row paths vs. list of column paths containing data
   * @param fieldCount number of fields per well
   * @param x image width
   * @param y image height
   */
  private void checkPlateGroupLayout(Path root, int rowCount, int colCount,
    Map<String, List<String>> validPaths, int fieldCount, int x, int y)
    throws IOException
  {
    // check valid group layout
    // OME (OME-XML metadata) folder, .zattrs (Plate), .zgroup (Plate) and rows
    assertEquals(validPaths.size() + 3, Files.list(root).count());

    // for every row in the overall plate,
    // if there are any wells with data in the row, the row path must exist
    // if there are no wells with data in the row, the row path cannot exist
    for (int row=0; row<rowCount; row++) {
      String rowString = String.format("%s", 'A' + row);
      if (validPaths.containsKey(rowString)) {
        Path rowPath = root.resolve(rowString);
        // .zgroup (Row) and columns
        List<String> validColumns = validPaths.get(rowString);
        assertEquals(validColumns.size() + 1, Files.list(rowPath).count());

        // for every column in the overall plate,
        // if this row/column is a well with data, the column path must exist
        // otherwise, the column path cannot exist
        for (int col=0; col<colCount; col++) {
          String colString = Integer.toString(col + 1);
          if (validColumns.contains(colString)) {
            Path colPath = rowPath.resolve(colString);
            ZarrGroup colGroup = ZarrGroup.open(colPath);
            // .zattrs (Column/Image), .zgroup (Column/Image) and fields
            assertEquals(fieldCount + 2, Files.list(colPath).count());
            for (int field=0; field<fieldCount; field++) {
              // append resolution index
              ZarrArray series0 = colGroup.openArray(field + "/0");
              assertArrayEquals(new int[] {1, 1, 1, y, x}, series0.getShape());
              assertArrayEquals(new int[] {1, 1, 1, y, x}, series0.getChunks());
            }
          }
          else {
            assertFalse(rowPath.resolve(colString).toFile().exists());
          }
        }
      }
      else {
        assertFalse(root.resolve(rowString).toFile().exists());
      }
    }
  }

  private void checkWell(
      ZarrGroup wellGroup, int fieldCount,
      Map<Integer, Integer> wellAcquisitions) throws IOException
  {
    Map<String, Object> well =
        (Map<String, Object>) wellGroup.getAttributes().get("well");
    List<Map<String, Object>> images =
        (List<Map<String, Object>>) well.get("images");
    assertEquals(fieldCount, images.size());

    for (int i=0; i<fieldCount; i++) {
      Map<String, Object> field = images.get(i);
      assertEquals(field.get("path"), String.valueOf(i));
      Integer acquisition = wellAcquisitions.getOrDefault(i, null);
      if (acquisition == null) {
        assertFalse(field.containsKey("acquisition"));
      }
      else {
        assertEquals(acquisition, field.get("acquisition"));
      }
    }
  }

  private void checkAxes(List<Map<String, Object>> axes, String order,
    String[] units)
  {
    assertEquals(axes.size(), order.length());
    for (int i=0; i<axes.size(); i++) {
      String name = axes.get(i).get("name").toString();
      assertEquals(name, order.toLowerCase().substring(i, i + 1));
      assertTrue(axes.get(i).containsKey("type"));
      if (units != null) {
        assertEquals(axes.get(i).get("unit"), units[i]);
      }
      else {
        assertTrue(!axes.get(i).containsKey("unit"));
      }
    }
  }

  private Path getTestFile(String resourceName) throws IOException {
    try {
      return Paths.get(this.getClass().getResource(resourceName).toURI());
    }
    catch (Exception e) {
      throw new IOException(e);
    }
  }

  private OME getOMEMetadata() throws Exception {
    Path xml = output.resolve("OME").resolve("METADATA.ome.xml");
    ServiceFactory sf = new ServiceFactory();
    OMEXMLService xmlService = sf.getInstance(OMEXMLService.class);
    String omexml = new String(Files.readAllBytes(xml), StandardCharsets.UTF_8);
    assertTrue(xmlService.validateOMEXML(omexml));
    return (OME) xmlService.createOMEXMLRoot(omexml);
  }
}
