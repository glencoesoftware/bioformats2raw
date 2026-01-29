/**
 * Copyright (c) 2025 Glencoe Software, Inc. All rights reserved.
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import loci.common.LogbackTools;
import loci.common.services.ServiceFactory;
import loci.formats.FormatTools;
import loci.formats.in.FakeReader;
import loci.formats.services.OMEXMLService;
import ome.xml.model.OME;

import com.glencoesoftware.bioformats2raw.Converter;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Array;
import dev.zarr.zarrjava.store.FilesystemStore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import picocli.CommandLine;

public abstract class AbstractZarrTest {
  Path input;
  Path output;
  Converter converter;
  FilesystemStore store;

  /**
   * Set logging to warn before all methods.
   *
   * @param tmp temporary directory for output file
   */
  @BeforeEach
  public void setup(@TempDir Path tmp) throws Exception {
    output = tmp.resolve("test");
    LogbackTools.setRootLevel("warn");
    store = null;
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
    store = new FilesystemStore(output);
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

  void checkAxes(List<Map<String, Object>> axes, String order,
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

  Path getTestFile(String resourceName) throws IOException {
    try {
      return Paths.get(this.getClass().getResource(resourceName).toURI());
    }
    catch (Exception e) {
      throw new IOException(e);
    }
  }

  OME getOMEMetadata() throws Exception {
    Path xml = output.resolve("OME").resolve("METADATA.ome.xml");
    ServiceFactory sf = new ServiceFactory();
    OMEXMLService xmlService = sf.getInstance(OMEXMLService.class);
    String omexml = new String(Files.readAllBytes(xml), StandardCharsets.UTF_8);
    assertTrue(xmlService.validateOMEXML(omexml));
    return (OME) xmlService.createOMEXMLRoot(omexml);
  }

  void checkPlateSeriesMetadata(List<String> groupMap,
    int rowCount, int colCount, int fieldCount)
  {
    assertEquals(groupMap.size(), rowCount * colCount * fieldCount);
    int index = 0;
    for (int r=0; r<rowCount; r++) {
      for (int c=0; c<colCount; c++) {
        for (int f=0; f<fieldCount; f++) {
          String groupPath = (char) (r + 'A') + "/" + (c + 1) + "/" + f;
          assertEquals(groupMap.get(index++), groupPath);
        }
      }
    }
  }

  abstract void checkPlateDimensions(Map<String, Object> plate,
    int rowCount, int colCount, int fieldCount);

  void checkPlateDimensions(Map<String, Object> plate,
    int rowCount, int colCount, int fieldCount, boolean checkVersion)
  {
    assertEquals(fieldCount, ((Number) plate.get("field_count")).intValue());
    if (checkVersion) {
      assertEquals(getNGFFVersion(), plate.get("version"));
    }

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
  }

  /**
   * Check that FakeReader-defined special pixels are as expected
   * in the given array. Assumes uint8 pixel data.
   *
   * @param s series index
   * @param sizeZ total Z size, or 0 if the Z axis is omitted
   * @param sizeC total C size, or 0 if the C axis is omitted
   * @param sizeT total T size, or 0 if the T axis is omitted
   * @param shape shape of tile to read
   * @param array array to check
   * @throws ZarrException
   */
  public void checkSpecialPixels(int s, int sizeZ, int sizeC, int sizeT,
    int[] shape, Array array)
    throws ZarrException
  {
    checkSpecialPixels(s, sizeZ, sizeC, sizeT, shape, array, FormatTools.UINT8);
  }

  /**
   * Check that FakeReader-defined special pixels are as expected
   * in the given array.
   *
   * @param s series index
   * @param sizeZ total Z size, or 0 if the Z axis is omitted
   * @param sizeC total C size, or 0 if the C axis is omitted
   * @param sizeT total T size, or 0 if the T axis is omitted
   * @param shape shape of tile to read
   * @param array array to check
   * @param pixelType pixel type as defined in FormatTools
   * @throws ZarrException
   */
  public void checkSpecialPixels(int s, int sizeZ, int sizeC, int sizeT,
    int[] shape, Array array, int pixelType)
    throws ZarrException
  {
    boolean noZ = sizeZ == 0;
    boolean noC = sizeC == 0;
    boolean noT = sizeT == 0;

    int plane = 0;
    for (int t=0; t<(int) Math.max(1, sizeT); t++) {
      for (int c=0; c<(int) Math.max(1, sizeC); c++) {
        for (int z=0; z<(int) Math.max(1, sizeZ); z++, plane++) {
          long[] offset = new long[shape.length];
          int nextPointer = 0;
          if (!noT) {
            offset[nextPointer++] = t;
          }
          if (!noC) {
            offset[nextPointer++] = c;
          }
          if (!noZ) {
            offset[nextPointer++] = z;
          }
          ucar.ma2.Array tile = array.read(offset, shape);
          ByteBuffer buf = tile.getDataAsByteBuffer();
          byte[] pixels = new byte[buf.remaining()];
          buf.get(pixels);
          int bpp = FormatTools.getBytesPerPixel(pixelType);
          assertEquals(pixels.length,
            shape[shape.length - 2] * shape[shape.length - 1] * bpp);

          int[] specialPixels =
            FakeReader.readSpecialPixels(pixels, pixelType, false);
          int[] expected = new int[] {s, plane, z, c, t};
          if (pixelType == FormatTools.UINT8) {
            for (int p=0; p<expected.length; p++) {
              expected[p] %= 256;
            }
          }
          assertArrayEquals(expected, specialPixels);
        }
      }
    }
  }

  abstract void checkMultiscale(Map<String, Object> multiscale, String name);

  abstract String getNGFFVersion();
}
