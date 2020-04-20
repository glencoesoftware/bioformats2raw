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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.glencoesoftware.bioformats2raw.Converter;
import loci.common.LogbackTools;
import loci.formats.in.FakeReader;

import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.zarr.N5ZarrReader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Disabled;
import org.junit.rules.TemporaryFolder;

public class ZarrTest {

  Path input;

  Path output;

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  /**
   * Set logging to warn before all methods.
   */
  @Before
  public void setup() throws Exception {
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
    output = tmp.newFolder().toPath().resolve("test");
    args.add(output.toString());
    try {
      Converter.main(args.toArray(new String[]{}));
      Assert.assertTrue(Files.exists(output.resolve("data.zarr")));
      Assert.assertTrue(Files.exists(output.resolve("METADATA.ome.xml")));
    }
    catch (RuntimeException rt) {
      throw rt;
    }
    catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  static Path fake(String...args) {
    Assert.assertTrue(args.length %2 == 0);
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
   * Test using a different tile size from the default (1024).
   */
  @Test
  public void testSetSmallerDefault() throws Exception {
    input = fake();
    assertTool("-h", "128", "-w", "128");
    N5ZarrReader z =
      new N5ZarrReader(output.resolve("data.zarr").toString());
    DatasetAttributes da = z.getDatasetAttributes("/0/0");
    Assert.assertArrayEquals(
        new long[] {512, 512, 1, 1, 1}, da.getDimensions());
    Assert.assertArrayEquals(
        new int[] {128, 128, 1, 1, 1}, da.getBlockSize());
  }

  /**
   * Test using a different tile size from the default (1024) the does not
   * divide evenly.
   */
  @Disabled("n5-zarr padCrop() is broken")
  @Test
  public void testSetSmallerDefaultWithRemainder() throws Exception {
    input = fake();
    assertTool("-h", "384", "-w", "384");
    N5ZarrReader z =
      new N5ZarrReader(output.resolve("data.zarr").toString());
    DatasetAttributes da = z.getDatasetAttributes("/0/0");
    Assert.assertArrayEquals(
        new long[] {512, 512, 1, 1, 1}, da.getDimensions());
    Assert.assertArrayEquals(
        new int[] {384, 384, 1, 1, 1}, da.getBlockSize());
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
    Assert.assertArrayEquals(
        new long[] {512, 512, 1, 1, 1}, da.getDimensions());
    Assert.assertArrayEquals(
        new int[] {512, 512, 1, 1, 1}, da.getBlockSize());
    ByteBuffer tile = z.readBlock("/0/0", da, new long[] {0, 0, 0, 0, 0})
        .toByteBuffer();
    int[] seriesPlaneNumberZCT = FakeReader.readSpecialPixels(tile.array());
    Assert.assertArrayEquals(new int[] {0, 0, 0, 0, 0}, seriesPlaneNumberZCT);

    // Check series 1 dimensions and special pixels
    da = z.getDatasetAttributes("/1/0");
    Assert.assertArrayEquals(
        new long[] {512, 512, 1, 1, 1}, da.getDimensions());
    Assert.assertArrayEquals(
        new int[] {512, 512, 1, 1, 1}, da.getBlockSize());
    tile = z.readBlock("/1/0", da, new long[] {0, 0, 0, 0, 0})
            .toByteBuffer();
    seriesPlaneNumberZCT = FakeReader.readSpecialPixels(tile.array());
    Assert.assertArrayEquals(new int[] {1, 0, 0, 0, 0}, seriesPlaneNumberZCT);
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
    Assert.assertArrayEquals(
        new long[] {512, 512, 2, 1, 1}, da.getDimensions());
    Assert.assertArrayEquals(
        new int[] {512, 512, 1, 1, 1}, da.getBlockSize());

    // Check Z 0 special pixels
    ByteBuffer tile = z.readBlock("/0/0", da, new long[] {0, 0, 0, 0, 0})
        .toByteBuffer();
    int[] seriesPlaneNumberZCT = FakeReader.readSpecialPixels(tile.array());
    Assert.assertArrayEquals(new int[] {0, 0, 0, 0, 0}, seriesPlaneNumberZCT);
    // Check Z 1 special pixels
    tile = z.readBlock("/0/0", da, new long[] {0, 0, 1, 0, 0})
            .toByteBuffer();
    seriesPlaneNumberZCT = FakeReader.readSpecialPixels(tile.array());
    Assert.assertArrayEquals(new int[] {0, 1, 1, 0, 0}, seriesPlaneNumberZCT);
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
    Assert.assertArrayEquals(
        new long[] {512, 512, 1, 2, 1}, da.getDimensions());
    Assert.assertArrayEquals(
        new int[] {512, 512, 1, 1, 1}, da.getBlockSize());

    // Check C 0 special pixels
    ByteBuffer tile = z.readBlock("/0/0", da, new long[] {0, 0, 0, 0, 0})
        .toByteBuffer();
    int[] seriesPlaneNumberZCT = FakeReader.readSpecialPixels(tile.array());
    Assert.assertArrayEquals(new int[] {0, 0, 0, 0, 0}, seriesPlaneNumberZCT);
    // Check C 1 special pixels
    tile = z.readBlock("/0/0", da, new long[] {0, 0, 0, 1, 0})
            .toByteBuffer();
    seriesPlaneNumberZCT = FakeReader.readSpecialPixels(tile.array());
    Assert.assertArrayEquals(new int[] {0, 1, 0, 1, 0}, seriesPlaneNumberZCT);
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
    Assert.assertArrayEquals(
        new long[] {512, 512, 1, 1, 2}, da.getDimensions());
    Assert.assertArrayEquals(
        new int[] {512, 512, 1, 1, 1}, da.getBlockSize());

    // Check T 0 special pixels
    ByteBuffer tile = z.readBlock("/0/0", da, new long[] {0, 0, 0, 0, 0})
        .toByteBuffer();
    int[] seriesPlaneNumberZCT = FakeReader.readSpecialPixels(tile.array());
    Assert.assertArrayEquals(new int[] {0, 0, 0, 0, 0}, seriesPlaneNumberZCT);
    // Check T 1 special pixels
    tile = z.readBlock("/0/0", da, new long[] {0, 0, 0, 0, 1})
            .toByteBuffer();
    seriesPlaneNumberZCT = FakeReader.readSpecialPixels(tile.array());
    Assert.assertArrayEquals(new int[] {0, 1, 0, 0, 1}, seriesPlaneNumberZCT);
  }

}
