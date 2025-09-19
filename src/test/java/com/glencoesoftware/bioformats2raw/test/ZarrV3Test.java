/**
 * Copyright (c) 2025 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.bioformats2raw.test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.Group;

import loci.formats.in.FakeReader;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ZarrV3Test extends AbstractZarrTest {

  @Override
  String getNGFFVersion() {
    return "0.5";
  }

  /**
   * Test basic v3 conversion.
   */
  @Test
  public void testDefault() throws Exception {
    input = fake();
    assertTool("--v3");

    FilesystemStore store = new FilesystemStore(output);

    Array array = Array.open(store.resolve("0", "0"));
    assertArrayEquals(new long[] {1, 1, 1, 512, 512}, array.metadata.shape);

    Group rootGroup = Group.open(store.resolve("0"));
    Map<String, Object> attrs = rootGroup.metadata.attributes;
    List<Map<String, Object>> multiscales =
      (List<Map<String, Object>>) attrs.get("multiscales");
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
    assertTool("--v3");

    FilesystemStore store = new FilesystemStore(output);

    int[] shape = new int[] {1, 1, 1, 512, 512};
    long[] arrayShape = new long[] {sizeT, sizeC, sizeZ, 512, 512};
    for (int s=0; s<seriesCount; s++) {
      Array array = Array.open(store.resolve(String.valueOf(s), "0"));
      assertArrayEquals(arrayShape, array.metadata.shape);

      int plane = 0;
      for (int t=0; t<sizeT; t++) {
        for (int c=0; c<sizeC; c++) {
          for (int z=0; z<sizeZ; z++, plane++) {
            long[] offset = new long[] {t, c, z, 0, 0};
            ucar.ma2.Array tile = array.read(offset, shape);
            ByteBuffer buf = tile.getDataAsByteBuffer(ByteOrder.BIG_ENDIAN);
            byte[] pixels = new byte[buf.remaining()];
            buf.get(pixels);
            assertEquals(pixels.length, 512 * 512);

            int[] specialPixels = FakeReader.readSpecialPixels(pixels);
            assertArrayEquals(new int[] {s, plane, z, c, t}, specialPixels);
          }
        }
      }
    }
  }


}
