/**
 * Copyright (c) 2025 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.bioformats2raw.test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import dev.zarr.zarrjava.utils.Utils;
import dev.zarr.zarrjava.v3.Array;

import loci.common.Constants;
import loci.common.services.ServiceFactory;
import loci.formats.FormatTools;
import loci.formats.Modulo;
import loci.formats.in.OMETiffReader;
import loci.formats.ome.OMEXMLMetadata;
import loci.formats.services.OMEXMLService;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class OMEZarr1Test extends ZarrV3Test {

  @Override
  String getNGFFVersion() {
    return "0.9.dev1";
  }

  /**
   * Test modulo dimension handling.
   * Since this is OME-Zarr 0.9.dev1 which includes RFC-3, modulo dimensions
   * should be reported as their own axis.
   *
   * @param moduloFile OME-TIFF file with modulo dimension(s)
   * @param compact true if compact dimensions should be written
   */
  @ParameterizedTest
  @MethodSource("getModuloFiles")
  public void testModulo(String moduloFile, boolean compact) throws Exception {
    input = getTestFile(moduloFile);
    if (compact) {
      assertTool("--ngff-version", getNGFFVersion(), "--compact");
    }
    else {
      assertTool("--ngff-version", getNGFFVersion());
    }

    OMEXMLMetadata meta = getOMEMetadataStore();
    OMEXMLService service =
          new ServiceFactory().getInstance(OMEXMLService.class);

    int x = meta.getPixelsSizeX(0).getValue().intValue();
    int y = meta.getPixelsSizeY(0).getValue().intValue();
    int z = meta.getPixelsSizeZ(0).getValue().intValue();
    int c = meta.getPixelsSizeC(0).getValue().intValue();
    int t = meta.getPixelsSizeT(0).getValue().intValue();
    Modulo mz = service.getModuloAlongZ(meta, 0);
    Modulo mc = service.getModuloAlongC(meta, 0);
    Modulo mt = service.getModuloAlongT(meta, 0);

    int[] zAxes = getAxes(z, mz, compact);
    int[] cAxes = getAxes(c, mc, compact);
    int[] tAxes = getAxes(t, mt, compact);
    int dims = zAxes.length + cAxes.length + tAxes.length + 2;
    int[] chunkShape = new int[dims];
    Arrays.fill(chunkShape, 1);
    chunkShape[dims - 2] = y;
    chunkShape[dims - 1] = x;

    long[] shape = new long[dims];
    System.arraycopy(Utils.toLongArray(tAxes), 0, shape, 0, tAxes.length);
    System.arraycopy(Utils.toLongArray(cAxes), 0,
      shape, tAxes.length, cAxes.length);
    System.arraycopy(Utils.toLongArray(zAxes), 0,
      shape, tAxes.length + cAxes.length, zAxes.length);
    shape[dims - 2] = y;
    shape[dims - 1] = x;

    Array series0 = Array.open(store.resolve("0", "0"));
    assertArrayEquals(shape, series0.metadata().shape);
    assertArrayEquals(chunkShape, series0.metadata().chunkShape());

    try (OMETiffReader r = new OMETiffReader()) {
      r.setId(input.toString());
      for (int p=0; p<r.getImageCount(); p++) {
        int[] zct = r.getZCTCoords(p);
        int[] zz = reverse(
          FormatTools.rasterToPosition(reverse(zAxes), zct[0]));
        int[] cc = reverse(
          FormatTools.rasterToPosition(reverse(cAxes), zct[1]));
        int[] tt = reverse(
          FormatTools.rasterToPosition(reverse(tAxes), zct[2]));
        int[] offset = new int[dims];
        System.arraycopy(tt, 0, offset, 0, tt.length);
        System.arraycopy(cc, 0, offset, tt.length, cc.length);
        System.arraycopy(zz, 0, offset, tt.length + cc.length, zz.length);
        offset[dims - 2] = 0;
        offset[dims - 1] = 0;
        byte[] src = r.openBytes(p);
        ucar.ma2.Array dest = series0.read(
          Utils.toLongArray(offset),
          Utils.toLongArray(series0.metadata().chunkShape()));
        ByteBuffer buf = dest.getDataAsByteBuffer();
        byte[] destBytes = new byte[buf.remaining()];
        buf.get(destBytes);

        assertArrayEquals(src, destBytes,
          "plane #" + p + ", offset = " + Arrays.toString(offset));
      }
    }
  }

  private int[] getAxes(int size, Modulo m, boolean compact) {
    if (compact && size == 1) {
      return new int[0];
    }
    if (m != null) {
      int mLength = m.length();
      int newSize = size / m.length();
      if (compact && newSize == 1) {
        return new int[] {mLength};
      }
      if (compact && mLength == 1) {
        return new int[] {newSize};
      }
      if (Math.abs(m.step - 1) > Constants.EPSILON) {
        return new int[] {mLength, newSize};
      }
      else {
        return new int[] {newSize, mLength};
      }
    }
    return new int[] {size};
  }

  private int[] reverse(int[] pos) {
    if (pos.length == 1) {
      return pos;
    }
    int[] reversed = new int[pos.length];
    for (int i=0; i<pos.length/2; i++) {
      reversed[i] = pos[pos.length - i - 1];
      reversed[reversed.length - i - 1] = pos[i];
    }
    return reversed;
  }

  @Override
  public List<Map<String, Object>> getAxes(Map<String, Object> multiscale) {
    List<Map<String, Object>> coordinateSystems =
      (List<Map<String, Object>>) multiscale.get("coordinateSystems");
    assertEquals(coordinateSystems.size(), 1);
    assertEquals(coordinateSystems.get(0).get("name"), "default");
    return (List<Map<String, Object>>) coordinateSystems.get(0).get("axes");
  }

}
