/**
 * Copyright (c) 2020 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.bioformats2raw.test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import com.glencoesoftware.bioformats2raw.Converter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import io.tiledb.java.api.Array;
import io.tiledb.java.api.ArraySchema;
import io.tiledb.java.api.Attribute;
import io.tiledb.java.api.Context;
import io.tiledb.java.api.Datatype;
import io.tiledb.java.api.Domain;
import io.tiledb.java.api.Layout;
import io.tiledb.java.api.NativeArray;
import io.tiledb.java.api.Query;
import io.tiledb.java.api.QueryType;
import io.tiledb.java.api.TileDBError;
import loci.common.services.ServiceFactory;
import loci.formats.in.FakeReader;
import loci.formats.ome.OMEXMLMetadata;
import loci.formats.services.OMEXMLService;

import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.CompressionAdapter;
import org.janelia.saalfeldlab.n5.DataType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TileDBTest extends ZarrTest {

  String fileType = "tiledb";

  Context ctx;

  Gson gson;

  /**
   * Setup test case.
   */
  @Before
  public void setup() throws TileDBError {
    ctx = new Context();
    gson = new GsonBuilder()
        .registerTypeAdapter(DataType.class, new DataType.JsonAdapter())
        .registerTypeHierarchyAdapter(
            Compression.class, CompressionAdapter.getJsonAdapter())
        .disableHtmlEscaping()
        .create();
  }

  /**
   * Tear down test case.
   */
  @After
  public void tearDown() {
    ctx.close();
  }

  @Override
  String getFileType() {
    return "tiledb";
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
    Assert.assertTrue(Array.exists(
        ctx, output.resolve("data.tiledb/abc/888/0/0").toString()));
    Assert.assertTrue(Array.exists(
        ctx, output.resolve("data.tiledb/ghi/999/1/0").toString()));
  }

  /**
   * Test a fake file conversion and ensure the layout is set.
   */
  @Test
  public void testDefaultLayoutIsSet() throws Exception {
    input = fake();
    assertTool();
    String uri = output.resolve("data.tiledb").toString();
    try (Context ctx = new Context();
         Array array = new Array(ctx, uri, QueryType.TILEDB_READ);
         NativeArray metadata = array.getMetadata(
             "bioformats2raw.layout", Datatype.TILEDB_INT8)
        )
    {
      Integer layout = gson.fromJson(
          new String((byte[]) metadata.toJavaArray(), StandardCharsets.UTF_8),
          Integer.class);
      Assert.assertEquals(Converter.LAYOUT, layout);
    }
  }

  /**
   * Test that multiscales metadata is present.
   */
  @Test
  public void testMultiscalesMetadata() throws Exception {
    input = fake();
    assertTool();
    String uri = output.resolve("data.tiledb").resolve("0").toString();
    try (Context ctx = new Context();
         Array array = new Array(ctx, uri, QueryType.TILEDB_READ);
         NativeArray metadata = array.getMetadata(
             "multiscales", Datatype.TILEDB_INT8)
        )
    {
      List<Map<String, Object>> multiscales = gson.fromJson(
          new String((byte[]) metadata.toJavaArray(), StandardCharsets.UTF_8),
          List.class);
      Assert.assertEquals(1, multiscales.size());
      Map<String, Object> multiscale = multiscales.get(0);
      Assert.assertEquals("0.1", multiscale.get("version"));
      List<Map<String, Object>> datasets =
              (List<Map<String, Object>>) multiscale.get("datasets");
      Assert.assertTrue(datasets.size() > 0);
      Assert.assertEquals("0", datasets.get(0).get("path"));
    }
  }

  private void assertDimensions(String uri, Long[] expected)
      throws TileDBError
  {
    try (Context ctx = new Context();
         Array array = new Array(ctx, uri, QueryType.TILEDB_READ);
         ArraySchema arraySchema = array.getSchema();
         Domain domain = arraySchema.getDomain()
        )
    {
      Long[] dimensions = domain.getDimensions()
          .stream()
          .map(v -> {
            try {
              return (long) v.getDomain().getSecond() + 1;
            }
            catch (TileDBError e) {
              throw new RuntimeException(e);
            }
          })
          .toArray(Long[]::new);
      Assert.assertArrayEquals(expected, dimensions);
    }
  }

  private void assertBlockSizes(String uri, Long[] expected)
      throws TileDBError
  {
    try (Context ctx = new Context();
         Array array = new Array(ctx, uri, QueryType.TILEDB_READ);
         ArraySchema arraySchema = array.getSchema();
         Domain domain = arraySchema.getDomain()
        )
    {
      Long[] dimensions = domain.getDimensions()
          .stream()
          .map(v -> {
            try {
              return (long) v.getTileExtent();
            }
            catch (TileDBError e) {
              throw new RuntimeException(e);
            }
          })
          .toArray(Long[]::new);
      Assert.assertArrayEquals(expected, dimensions);
    }
  }

  /**
   * Test alternative dimension order.
   */
  @Test
  public void testXYCZTDimensionOrder() throws Exception {
    input = fake("sizeC", "2", "dimOrder", "XYCZT");
    assertTool();
    String uri = output.resolve("data.tiledb").resolve("0/0").toString();
    assertDimensions(uri, new Long[] {1L, 1L, 2L, 512L, 512L});
  }

  /**
   * Test using a forced dimension order.
   */
  @Test
  public void testSetXYCZTDimensionOrder() throws Exception {
    input = fake("sizeC", "2");
    assertTool("--dimension-order", "XYCZT");
    String uri = output.resolve("data.tiledb").resolve("0/0").toString();
    assertDimensions(uri, new Long[] {1L, 1L, 2L, 512L, 512L});
  }

  /**
   * Test using a different tile size from the default (1024).
   */
  @Test
  public void testSetSmallerDefault() throws Exception {
    input = fake();
    assertTool("-h", "128", "-w", "128");
    String uri = output.resolve("data.tiledb").resolve("0/0").toString();
    assertDimensions(uri, new Long[] {1L, 1L, 1L, 512L, 512L});
    assertBlockSizes(uri, new Long[] {1L, 1L, 1L, 128L, 128L});
  }

  /**
   * Test using a different tile size from the default (1024) the does not
   * divide evenly.
   */
  @Test
  public void testSetSmallerDefaultWithRemainder() throws Exception {
    input = fake();
    assertTool("-h", "384", "-w", "384");
    String uri = output.resolve("data.tiledb").resolve("0/0").toString();
    assertDimensions(uri, new Long[] {1L, 1L, 1L, 512L, 512L});
    assertBlockSizes(uri, new Long[] {1L, 1L, 1L, 384L, 384L});
  }

  private void assertSeriesPlaneNumberZCT(
      String uri, long[] offsets, int[] expected)
          throws TileDBError
  {
    try (Context ctx = new Context();
         Array array = new Array(ctx, uri, QueryType.TILEDB_READ);
         Query query = new Query(array, QueryType.TILEDB_READ);
         NativeArray subarray = new NativeArray(ctx, offsets, Long.class)
        )
    {
      ByteBuffer tileBuffer = ByteBuffer.allocateDirect(512 * 512)
          .order(ByteOrder.nativeOrder());
      query.setLayout(Layout.TILEDB_ROW_MAJOR);
      query.setSubarray(subarray);
      query.setBuffer("a1", tileBuffer);
      query.submit();

      byte[] tile = new byte[tileBuffer.limit()];
      tileBuffer.get(tile);
      int[] seriesPlaneNumberZCT = FakeReader.readSpecialPixels(tile);
      Assert.assertArrayEquals(expected, seriesPlaneNumberZCT);
    }
  }

  /**
   * Test more than one series.
   */
  @Test
  public void testMultiSeries() throws Exception {
    input = fake("series", "2");
    assertTool();
    long[] offsets = new long[] {
      0, 0,   // T
      0, 0,   // C
      0, 0,   // Z
      0, 511, // Y
      0, 511  // X
    };

    // Check series 0 dimensions and special pixels
    String uri = output.resolve("data.tiledb").resolve("0/0").toString();
    assertDimensions(uri, new Long[] {1L, 1L, 1L, 512L, 512L});
    assertBlockSizes(uri, new Long[] {1L, 1L, 1L, 512L, 512L});
    assertSeriesPlaneNumberZCT(uri, offsets, new int[] {0, 0, 0, 0, 0});

    // Check series 1 dimensions and special pixels
    uri = output.resolve("data.tiledb").resolve("1/0").toString();
    assertDimensions(uri, new Long[] {1L, 1L, 1L, 512L, 512L});
    assertBlockSizes(uri, new Long[] {1L, 1L, 1L, 512L, 512L});
    assertSeriesPlaneNumberZCT(uri, offsets, new int[] {1, 0, 0, 0, 0});
  }

  /**
   * Test more than one Z-section.
   */
  @Test
  public void testMultiZ() throws Exception {
    input = fake("sizeZ", "2");
    assertTool();
    String uri = output.resolve("data.tiledb").resolve("0/0").toString();

    // Check dimensions and block size
    assertDimensions(uri, new Long[] {1L, 1L, 2L, 512L, 512L});
    assertBlockSizes(uri, new Long[] {1L, 1L, 1L, 512L, 512L});

    // Check Z 0 special pixels
    long[] offsets = new long[] {
      0, 0,   // T
      0, 0,   // C
      0, 0,   // Z
      0, 511, // Y
      0, 511  // X
    };
    assertSeriesPlaneNumberZCT(uri, offsets, new int[] {0, 0, 0, 0, 0});
    // Check Z 1 special pixels
    offsets = new long[] {
      0, 0,   // T
      0, 0,   // C
      1, 1,   // Z
      0, 511, // Y
      0, 511  // X
    };
    assertSeriesPlaneNumberZCT(uri, offsets, new int[] {0, 1, 1, 0, 0});
  }

  /**
   * Test more than one channel.
   */
  @Test
  public void testMultiC() throws Exception {
    input = fake("sizeC", "2");
    assertTool();
    String uri = output.resolve("data.tiledb").resolve("0/0").toString();

    // Check dimensions and block size
    assertDimensions(uri, new Long[] {1L, 2L, 1L, 512L, 512L});
    assertBlockSizes(uri, new Long[] {1L, 1L, 1L, 512L, 512L});

    // Check C 0 special pixels
    long[] offsets = new long[] {
      0, 0,   // T
      0, 0,   // C
      0, 0,   // Z
      0, 511, // Y
      0, 511  // X
    };
    assertSeriesPlaneNumberZCT(uri, offsets, new int[] {0, 0, 0, 0, 0});
    // Check C 1 special pixels
    offsets = new long[] {
      0, 0,   // T
      1, 1,   // C
      0, 0,   // Z
      0, 511, // Y
      0, 511  // X
    };
    assertSeriesPlaneNumberZCT(uri, offsets, new int[] {0, 1, 0, 1, 0});
  }

  /**
   * Test more than one timepoint.
   */
  @Test
  public void testMultiT() throws Exception {
    input = fake("sizeT", "2");
    assertTool();
    String uri = output.resolve("data.tiledb").resolve("0/0").toString();

    // Check dimensions and block size
    assertDimensions(uri, new Long[] {2L, 1L, 1L, 512L, 512L});
    assertBlockSizes(uri, new Long[] {1L, 1L, 1L, 512L, 512L});

    // Check T 0 special pixels
    long[] offsets = new long[] {
      0, 0,   // T
      0, 0,   // C
      0, 0,   // Z
      0, 511, // Y
      0, 511  // X
    };
    assertSeriesPlaneNumberZCT(uri, offsets, new int[] {0, 0, 0, 0, 0});
    // Check T 1 special pixels
    offsets = new long[] {
      1, 1,   // T
      0, 0,   // C
      0, 0,   // Z
      0, 511, // Y
      0, 511  // X
    };
    assertSeriesPlaneNumberZCT(uri, offsets, new int[] {0, 1, 0, 0, 1});
  }

  private void assertPixelType(String uri, Datatype expected)
      throws TileDBError
  {
    try (Context ctx = new Context();
         Array array = new Array(ctx, uri, QueryType.TILEDB_READ);
         ArraySchema arraySchema = array.getSchema();
         Attribute attribute = arraySchema.getAttribute("a1")
        )
    {
      Assert.assertEquals(expected, attribute.getType());
    }
  }

  /**
   * Test float pixel type.
   */
  @Test
  public void testFloatPixelType() throws Exception {
    input = fake("pixelType", "float");
    assertTool();
    String uri = output.resolve("data.tiledb").resolve("0/0").toString();

    // Check series dimensions and special pixels
    assertPixelType(uri, Datatype.TILEDB_FLOAT32);
    assertDimensions(uri, new Long[] {1L, 1L, 1L, 512L, 512L});
    assertBlockSizes(uri, new Long[] {1L, 1L, 1L, 512L, 512L});
    long[] offsets = new long[] {
      0, 0,   // T
      0, 0,   // C
      0, 0,   // Z
      0, 511, // Y
      0, 511  // X
    };
    assertSeriesPlaneNumberZCT(uri, offsets, new int[] {0, 0, 0, 0, 0});
  }

  /**
   * Test double pixel type.
   */
  @Test
  public void testDoublePixelType() throws Exception {
    input = fake("pixelType", "double");
    assertTool();
    String uri = output.resolve("data.tiledb").resolve("0/0").toString();

    // Check series dimensions and special pixels
    assertPixelType(uri, Datatype.TILEDB_FLOAT64);
    assertDimensions(uri, new Long[] {1L, 1L, 1L, 512L, 512L});
    assertBlockSizes(uri, new Long[] {1L, 1L, 1L, 512L, 512L});
    long[] offsets = new long[] {
      0, 0,   // T
      0, 0,   // C
      0, 0,   // Z
      0, 511, // Y
      0, 511  // X
    };
    assertSeriesPlaneNumberZCT(uri, offsets, new int[] {0, 0, 0, 0, 0});
  }

  /**
   * Test that there are no edge effects when tiles do not divide evenly
   * and downsampling.
   */
  @Test
  public void testDownsampleEdgeEffectsUInt8() throws Exception {
    input = fake("sizeX", "60", "sizeY", "300");
    assertTool("-w", "25", "-h", "75");
    String uri = output.resolve("data.tiledb").resolve("0/1").toString();

    // Check series dimensions
    assertDimensions(uri, new Long[] {1L, 1L, 1L, 150L, 30L});
    assertBlockSizes(uri, new Long[] {1L, 1L, 1L, 75L, 25L});
    long[] offsets = new long[] {
      0, 0,    // T
      0, 0,    // C
      0, 0,    // Z
      75, 149, // Y
      25, 29   // X
    };
    try (Context ctx = new Context();
         Array array = new Array(ctx, uri, QueryType.TILEDB_READ);
         Query query = new Query(array, QueryType.TILEDB_READ);
         NativeArray subarray = new NativeArray(ctx, offsets, Long.class)
        )
    {
      ByteBuffer tile = ByteBuffer.allocateDirect(75 * 5)
          .order(ByteOrder.nativeOrder());
      query.setLayout(Layout.TILEDB_ROW_MAJOR);
      query.setSubarray(subarray);
      query.setBuffer("a1", tile);
      query.submit();

      // Last row first pixel should be the 2x2 downsampled value;
      // test will break if the downsampling algorithm changes
      Assert.assertEquals(50, tile.get(74 * 5));
    }
  }

  /**
   * Test that there are no edge effects when tiles do not divide evenly
   * and downsampling.
   */
  @Test
  public void testDownsampleEdgeEffectsUInt16() throws Exception {
    input = fake("sizeX", "60", "sizeY", "300", "pixelType", "uint16");
    assertTool("-w", "25", "-h", "75");
    String uri = output.resolve("data.tiledb").resolve("0/1").toString();

    // Check series dimensions
    assertPixelType(uri, Datatype.TILEDB_UINT16);
    assertDimensions(uri, new Long[] {1L, 1L, 1L, 150L, 30L});
    assertBlockSizes(uri, new Long[] {1L, 1L, 1L, 75L, 25L});
    long[] offsets = new long[] {
      0, 0,    // T
      0, 0,    // C
      0, 0,    // Z
      75, 149, // Y
      25, 29   // X
    };
    try (Context ctx = new Context();
         Array array = new Array(ctx, uri, QueryType.TILEDB_READ);
         Query query = new Query(array, QueryType.TILEDB_READ);
         NativeArray subarray = new NativeArray(ctx, offsets, Long.class)
        )
    {
      ByteBuffer tile = ByteBuffer.allocateDirect(75 * 5 * 2)
          .order(ByteOrder.nativeOrder());
      query.setLayout(Layout.TILEDB_ROW_MAJOR);
      query.setSubarray(subarray);
      query.setBuffer("a1", tile);
      query.submit();

      // Last row first pixel should be the 2x2 downsampled value;
      // test will break if the downsampling algorithm changes
      Assert.assertEquals(50, tile.asShortBuffer().get(74 * 5));
    }
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
    Assert.assertEquals(originalMetadata.size(), convertedMetadata.size());
    for (String key : originalMetadata.keySet()) {
      Assert.assertEquals(
        originalMetadata.get(key), convertedMetadata.get(key));
    }
  }

}
