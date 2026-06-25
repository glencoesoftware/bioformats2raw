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

public class OMEZarr1Test extends ZarrV3Test {

  @Override
  String getNGFFVersion() {
    return "1.0-DEV";
  }

  /**
   * Test modulo dimension handling.
   * Since this is OME-Zarr 1.0-DEV which includes RFC-3, modulo dimensions
   * should be reported as their own axis.
   *
   * @param moduloFile OME-TIFF file with modulo dimension(s)
   */
  @ParameterizedTest
  @MethodSource("getModuloFiles")
  public void testModulo(String moduloFile) throws Exception {
    input = getTestFile(moduloFile);
    assertTool("--ngff-version", getNGFFVersion());

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

    int[] zAxes = new int[] {z};
    int[] cAxes = new int[] {c};
    int[] tAxes = new int[] {t};
    if (mz != null && mz.length() > 1) {
      if (Math.abs(mz.step - 1) < Constants.EPSILON) {
        zAxes = new int[] {mz.length(), z / mz.length()};
      }
      else {
        zAxes = new int[] {z / mz.length(), mz.length()};
      }
    }
    if (mc != null && mc.length() > 1) {
      if (Math.abs(mc.step - 1) < Constants.EPSILON) {
        cAxes = new int[] {mc.length(), c / mc.length()};
      }
      else {
        cAxes = new int[] {c / mc.length(), mc.length()};
      }
    }
    if (mt != null && mt.length() > 1) {
      if (Math.abs(mt.step - 1) < Constants.EPSILON) {
        tAxes = new int[] {mt.length(), t / mt.length()};
      }
      else {
        tAxes = new int[] {t / mt.length(), mt.length()};
      }
    }
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
        int[] zz = FormatTools.rasterToPosition(zAxes, zct[0]);
        int[] cc = FormatTools.rasterToPosition(cAxes, zct[1]);
        int[] tt = FormatTools.rasterToPosition(tAxes, zct[2]);
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

}
