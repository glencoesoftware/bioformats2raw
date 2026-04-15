/**
 * Copyright (c) 2025 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.bioformats2raw.test;

import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import com.scalableminds.bloscjava.Blosc;
import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.ArrayMetadata;
import dev.zarr.zarrjava.v3.Group;
import dev.zarr.zarrjava.v3.codec.Codec;
import dev.zarr.zarrjava.v3.codec.core.BloscCodec;
import dev.zarr.zarrjava.v3.codec.core.GzipCodec;
import dev.zarr.zarrjava.v3.codec.core.ShardingIndexedCodec;
import dev.zarr.zarrjava.v3.codec.core.ZstdCodec;

import ome.xml.model.OME;

import picocli.CommandLine.ExecutionException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ZarrV3Test extends AbstractZarrTest {

  @Override
  String getNGFFVersion() {
    return "0.5";
  }

  @Override
  void checkPlateDimensions(Map<String, Object> plate,
    int rowCount, int colCount, int fieldCount)
  {
    checkPlateDimensions(plate, rowCount, colCount, fieldCount, false);
  }

  @Override
  void checkMultiscale(Map<String, Object> multiscale, String name) {
    assertEquals(name, multiscale.get("name"));
  }

  /**
   * Test basic v3 conversion.
   */
  @Test
  public void testDefault() throws Exception {
    input = fake();
    assertTool("--ngff-version", getNGFFVersion());

    Group rootGroup = Group.open(store.resolve(""));
    Attributes attrs = rootGroup.metadata().attributes;
    Attributes omeAttrs = attrs.getAttributes("ome");
    assertEquals(getNGFFVersion(), omeAttrs.get("version"));
    assertEquals(3, omeAttrs.get("bioformats2raw.layout"));

    Array array = Array.open(store.resolve("0", "0"));
    assertArrayEquals(new long[] {1, 1, 1, 512, 512}, array.metadata().shape);

    rootGroup = Group.open(store.resolve("0"));
    attrs = rootGroup.metadata().attributes;
    omeAttrs = attrs.getAttributes("ome");
    assertEquals("0.5", omeAttrs.get("version"));

    List<Map<String, Object>> multiscales =
      (List<Map<String, Object>>) omeAttrs.get("multiscales");
    assertEquals(1, multiscales.size());
    Map<String, Object> multiscale = multiscales.get(0);
    checkMultiscale(multiscale, "image");

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
   * Test HCS v3 conversion.
   */
  @Test
  public void testHCS() throws Exception {
    int rowCount = 2;
    int colCount = 3;
    int fieldCount = 2;
    input = fake(
      "plates", "1", "plateAcqs", "1",
      "plateRows", String.valueOf(rowCount),
      "plateCols", String.valueOf(colCount),
      "fields", String.valueOf(fieldCount));
    assertTool("--ngff-version", getNGFFVersion());

    Group rootGroup = Group.open(store.resolve(""));
    Group omeGroup = Group.open(store.resolve("OME"));
    Attributes omeAttrs = omeGroup.metadata().attributes.getAttributes("ome");
    List<String> groupMap = (List<String>) omeAttrs.get("series");
    assertEquals(getNGFFVersion(), omeAttrs.get("version"));

    checkPlateSeriesMetadata(groupMap, rowCount, colCount, fieldCount);

    omeAttrs = rootGroup.metadata().attributes.getAttributes("ome");
    assertEquals(getNGFFVersion(), omeAttrs.get("version"));
    assertEquals(3, omeAttrs.get("bioformats2raw.layout"));

    Attributes plate = omeAttrs.getAttributes("plate");
    checkPlateDimensions(plate, rowCount, colCount, fieldCount);

    long[] arrayShape = new long[] {1, 1, 1, 512, 512};
    for (int r=0; r<rowCount; r++) {
      for (int c=0; c<colCount; c++) {
        Group wellGroup = Group.open(store.resolve(
          String.valueOf((char) ('A' + r)),
          String.valueOf(c + 1)));
        omeAttrs = wellGroup.metadata().attributes.getAttributes("ome");
        assertEquals(getNGFFVersion(), omeAttrs.get("version"));
        Map<String, Object> well = (Map<String, Object>) omeAttrs.get("well");
        List<Map<String, Object>> images =
          (List<Map<String, Object>>) well.get("images");
        assertEquals(fieldCount, images.size());
        for (int f=0; f<fieldCount; f++) {
          Map<String, Object> image = (Map<String, Object>) images.get(f);
          assertEquals(image.get("path"), String.valueOf(f));
          Array array = Array.open(store.resolve(
            String.valueOf((char) ('A' + r)),
            String.valueOf(c + 1),
            String.valueOf(f), "0"));
          assertArrayEquals(arrayShape, array.metadata().shape);
        }
      }
    }

    OME ome = getOMEMetadata();
    assertEquals(1, ome.sizeOfPlateList());
  }

  /**
   * Check for OMERO rendering metadata.
   */
  @Test
  public void testOMEROMetadata() throws Exception {
    input = getTestFile("colors.ome.xml");
    assertTool("--ngff-version", getNGFFVersion());

    String[][] names = {{"orange"}, {"green", "blue"}, {"blue"}};
    String[][] colors = {{"FF7F00"}, {"00FF00", "0000FF"}, {"808080"}};

    for (int i=0; i<names.length; i++) {
      Group z = Group.open(store.resolve(String.valueOf(i)));
      Attributes attrs = z.metadata().attributes;
      Attributes omeAttrs = attrs.getAttributes("ome");
      Attributes omero = omeAttrs.getAttributes("omero");

      Attributes rdefs = omero.getAttributes("rdefs");
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
   * Convert with the --no-root-group option and make sure
   * no root group is present.
   */
  @Test
  public void testNoRootGroupOption() throws Exception {
    input = fake();
    assertTool("--no-root-group", "--ngff-version", getNGFFVersion());

    assertFalse(Files.exists(output.resolve("zarr.json")));
  }

  /**
   * Convert with the --no-ome-meta-export option and make sure
   * no OME metadata is present.
   */
  @Test
  public void testNoOMEOption() throws Exception {
    input = fake();
    assertTool("--no-ome-meta-export", "--ngff-version", getNGFFVersion());

    assertTrue(
      !Files.exists(output.resolve("OME").resolve("METADATA.ome.xml")));
  }

  /**
   * Test special pixels across several series and planes.
   */
  @Test
  public void testSpecialPixels() throws Exception {
    int seriesCount = 3;
    int sizeC = 2;
    int sizeZ = 4;
    int sizeT = 5;
    input = fake("series", String.valueOf(seriesCount),
      "sizeC", String.valueOf(sizeC),
      "sizeZ", String.valueOf(sizeZ),
      "sizeT", String.valueOf(sizeT));
    assertTool("--ngff-version", getNGFFVersion());

    int[] shape = new int[] {1, 1, 1, 512, 512};
    long[] arrayShape = new long[] {sizeT, sizeC, sizeZ, 512, 512};
    for (int s=0; s<seriesCount; s++) {
      Array array = Array.open(store.resolve(String.valueOf(s), "0"));
      assertArrayEquals(arrayShape, array.metadata().shape);

      checkSpecialPixels(s, sizeZ, sizeC, sizeT, shape, array);
    }
  }

  static Stream<Arguments> getShardSizes() {
    return Stream.of(
      Arguments.of(512, 512, 1),
      Arguments.of(1024, 1024, 2),
      Arguments.of(1024, 512, 4),
      Arguments.of(2048, 3072, 4)
    );
  }

  /**
   * Test a few different shard sizes.
   *
   * @param x shard width
   * @param y shard height
   * @param z shard depth
   */
  @ParameterizedTest
  @MethodSource("getShardSizes")
  public void testShardSizes(int x, int y, int z) throws Exception {
    int sizeX = 2048;
    int sizeY = 3072;
    int sizeC = 2;
    int sizeZ = 4;
    int sizeT = 5;
    input = fake("sizeX", String.valueOf(sizeX),
      "sizeY", String.valueOf(sizeY),
      "sizeC", String.valueOf(sizeC),
      "sizeZ", String.valueOf(sizeZ),
      "sizeT", String.valueOf(sizeT));
    assertTool("--ngff-version", getNGFFVersion(),
      "--tile-width", "512",
      "--tile-height", "512",
      "--shard-width", String.valueOf(x),
      "--shard-height", String.valueOf(y),
      "--shard-depth", String.valueOf(z));

    long[] arrayShape = new long[] {sizeT, sizeC, sizeZ, sizeY, sizeX};
    Array array = Array.open(store.resolve("0", "0"));
    assertArrayEquals(arrayShape, array.metadata().shape);

    int[] shardShape = new int[] {1, 1, z, y, x};
    int[] chunkShape = new int[] {1, 1, 1, 512, 512};
    assertArrayEquals(shardShape, array.metadata().chunkShape());
    Optional<Codec> shardingCodec =
      ArrayMetadata.getShardingIndexedCodec(array.metadata().codecs);
    assertTrue(shardingCodec.isPresent());
    assertTrue(shardingCodec.get() instanceof ShardingIndexedCodec);
    ShardingIndexedCodec shardIndex =
      (ShardingIndexedCodec) shardingCodec.get();
    assertArrayEquals(chunkShape, shardIndex.configuration.chunkShape);

    int[] shape = new int[] {1, 1, 1, sizeY, sizeX};
    checkSpecialPixels(0, sizeZ, sizeC, sizeT, shape, array);
  }

  /**
   * Test invalid shard size.
   */
  @Test
  public void testInvalidShardSizes() throws Exception {
    int sizeX = 2048;
    int sizeY = 3192;
    input = fake("sizeX", String.valueOf(sizeX),
      "sizeY", String.valueOf(sizeY));
    assertTool("--ngff-version", getNGFFVersion(),
      "--tile-width", "512",
      "--tile-height", "512",
      "--shard-width", String.valueOf(sizeX),
      "--shard-height", String.valueOf(sizeY));

    long[] arrayShape = new long[] {1, 1, 1, sizeY, sizeX};
    Array array = Array.open(store.resolve("0", "0"));
    assertArrayEquals(arrayShape, array.metadata().shape);
    Optional<Codec> shardingCodec =
      ArrayMetadata.getShardingIndexedCodec(array.metadata().codecs);
    assertFalse(shardingCodec.isPresent());
  }

  /**
   * @return compression settings
   */
  static Stream<Arguments> getCompressionSettings() {
    return Stream.of(
      Arguments.of((Object) new String[] {"-c", "gzip"}),
      Arguments.of((Object) new String[] {"-c", "gzip",
        "--compression-properties", "level=1"}),
      Arguments.of((Object) new String[] {"-c", "blosc",
        "--compression-properties", "cname=zlib",
        "--compression-properties", "blocksize=8"}),
      Arguments.of((Object) new String[] {"-c", "blosc",
        "--compression-properties", "clevel=1",
        "--compression-properties", "shuffle=noshuffle"}),
      Arguments.of((Object) new String[] {"-c", "zstd",
        "--compression-properties", "level=9",
        "--compression-properties", "checksum=false"})
    );
  }

  /**
   * @return compression settings expected to throw an exception
   */
  static Stream<Arguments> getBadCompressionSettings() {
    return Stream.of(
      Arguments.of("zlib")
    );
  }

  /**
   * Test different compression options.
   *
   * @param options compression type and properties passed directly to converter
   */
  @ParameterizedTest
  @MethodSource("getCompressionSettings")
  public void testCompressionOptions(String[] options) throws Exception {
    input = fake();
    String[] allOptions = new String[options.length + 2];
    allOptions[0] = "--ngff-version";
    allOptions[1] = getNGFFVersion();
    System.arraycopy(options, 0, allOptions, 2, options.length);
    assertTool(allOptions);

    Array array = Array.open(store.resolve("0", "0"));
    assertEquals(array.metadata().codecs.length, 1);
    assertTrue(array.metadata().codecs[0] instanceof ShardingIndexedCodec);
    ShardingIndexedCodec shards =
      (ShardingIndexedCodec) array.metadata().codecs[0];
    assertEquals(shards.configuration.codecs.length, 2);
    Codec codec = shards.configuration.codecs[1];
    if (options[1].equals("gzip")) {
      assertEquals(codec.getClass(), GzipCodec.class);
      if (options.length == 4) {
        GzipCodec c = (GzipCodec) codec;
        String[] option = options[3].split("=");
        assertEquals(option[0], "level");
        assertEquals(Integer.parseInt(option[1]), c.configuration.level);
      }
    }
    else if (options[1].equals("zstd")) {
      assertEquals(codec.getClass(), ZstdCodec.class);
      if (options.length > 3) {
        ZstdCodec c = (ZstdCodec) codec;
        for (int i=2; i<options.length; i+=2) {
          assertEquals(options[i], "--compression-properties");
          String[] option = options[i + 1].split("=");
          if (option[0].equals("level")) {
            assertEquals(Integer.parseInt(option[1]), c.configuration.level);
          }
          else if (option[0].equals("checksum")) {
            assertEquals(
              Boolean.parseBoolean(option[1]), c.configuration.checksum);
          }
        }
      }
    }
    else if (options[1].equals("blosc")) {
      assertEquals(codec.getClass(), BloscCodec.class);
      if (options.length > 3) {
        BloscCodec c = (BloscCodec) codec;
        for (int i=2; i<options.length; i+=2) {
          assertEquals(options[i], "--compression-properties");
          String[] option = options[i + 1].split("=");
          if (option[0].equals("clevel")) {
            assertEquals(Integer.parseInt(option[1]), c.configuration.clevel);
          }
          else if (option[0].equals("cname")) {
            assertEquals(option[1], c.configuration.cname.getValue());
          }
          else if (option[0].equals("blocksize")) {
            assertEquals(
              Integer.parseInt(option[1]), c.configuration.blocksize);
          }
          else if (option[0].equals("shuffle")) {
            assertEquals(
              Blosc.Shuffle.fromString(option[1]), c.configuration.shuffle);
          }
        }
      }
    }
    else {
      fail("Unexpected compression type " + options[1]);
    }
  }

  /**
   * Test different invalid compression options.
   *
   * @param codec codec name
   */
  @ParameterizedTest
  @MethodSource("getBadCompressionSettings")
  public void testBadCompressionOptions(String codec) throws Exception {
    input = fake();
    assertThrows(ExecutionException.class, () -> {
      assertTool("--ngff-version", getNGFFVersion(), "-c", codec);
    });
  }

}
