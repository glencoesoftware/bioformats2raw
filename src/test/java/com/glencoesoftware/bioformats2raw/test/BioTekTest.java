/**
 * Copyright (c) 2019 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.bioformats2raw.test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import com.glencoesoftware.bioformats2raw.BioTekReader;
import loci.common.LogbackTools;
import loci.common.services.ServiceFactory;
import loci.formats.services.OMEXMLService;
import loci.formats.ome.OMEXMLMetadata;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests specific to the BioTek reader included with bioformats2raw.
 */
public class BioTekTest {

  /** Temporary directory containing an artificial BioTek plate. */
  Path input;

  /**
   * Set logging to warn before all methods and create a new temporary
   * directory to copy input data.
   * This gets called once per test, before the test itself is run.
   *
   * @param tmp temporary directory for input data
   */
  @BeforeEach
  public void setup(@TempDir Path tmp) throws Exception {
    input = tmp.resolve("test-input");
    input.toFile().mkdirs();
    LogbackTools.setRootLevel("warn");
  }

  /**
   * Given a list of file paths that make up a plate, copy the minimal test TIFF
   * in bioformats2raw/src/test/resources/... to each path. This creates an
   * artificial BioTek plate that can be initialized by the reader.
   *
   * @param filenames relative paths to be created
   * @throws IOException if file copying fails
   */
  void createPlate(String[] filenames) throws IOException {
    // test file in repository is not real data, but the result of:
    // $ bfconvert "test&sizeX=2&sizeY=2.fake" test.tiff
    // $ tiffset -u 270 test.tiff
    //
    // the "tiffset" command removes the TIFF comment, because
    // bfconvert stores ImageJ-style dimension information there,
    // but BioTekReader expects either no comment or BioTek-specific XML
    Path testTiff = getTestFile("test.tiff");

    for (String f : filenames) {
      Path copyLocation = input.resolve(f);
      if (!input.equals(copyLocation.getParent())) {
        // supplied file path includes a subdirectory
        copyLocation.getParent().toFile().mkdirs();
      }
      Files.copy(testTiff, copyLocation);
    }

    // reset the input path to the first file in the list
    // the reader cannot initialize from a directory
    input = input.resolve(filenames[0]);
  }

  /**
   * Create an artificial BioTek plate with the given list of file names.
   * Checks that the correct well row/column and ZCT sizes are detected.
   *
   * This test will be run once for each Arguments object
   * returned by getTestCases below.
   *
   * @param paths path to each file in the plate
   * @param wellRow row index for each well (from 0)
   * @param wellColumn column index for each well (from 0)
   * @param fields number of fields in the well
   * @param sizeZ number of Z sections
   * @param sizeC number of channels
   * @param sizeT number of timepoints
   */
  @ParameterizedTest
  @MethodSource("getTestCases")
  public void testBioTek(String[] paths, int[] wellRow, int[] wellColumn,
    int fields, int sizeZ, int sizeC, int sizeT) throws Exception
  {
    // set up the artificial plate
    createPlate(paths);

    try (BioTekReader reader = new BioTekReader()) {
      // initialize the reader, making sure that OME-XML metadata is populated
      // so that we can check plate/well metadata
      OMEXMLMetadata metadata = getOMEMetadata();
      reader.setMetadataStore(metadata);
      // if file name detection were to break, setId is likely to throw
      // an exception which would fail the test
      reader.setId(input.toString());

      // the number of OME Images should match the expected well * field count
      // this should be the same as the series count
      assertEquals(metadata.getImageCount(), fields * wellRow.length);
      assertEquals(metadata.getImageCount(), reader.getSeriesCount());
      // there should be exactly one plate
      assertEquals(metadata.getPlateCount(), 1);
      assertEquals(metadata.getWellCount(0), wellRow.length);
      // the well's row and column indexes should match expectations
      // this is especially important for "sparse" plates where the first
      // row and/or column in the plate are missing
      for (int w=0; w<wellRow.length; w++) {
        assertEquals(metadata.getWellRow(0, w).getValue(), wellRow[w]);
        assertEquals(metadata.getWellColumn(0, w).getValue(), wellColumn[w]);
        // all of the fields should be linked to the well
        assertEquals(metadata.getWellSampleCount(0, w), fields);
        for (int f=0; f<fields; f++) {
          // sanity check that the Images are linked to the
          // well in the correct order
          int imageIndex = (fields * w) + f;
          assertEquals(metadata.getWellSampleImageRef(0, w, f),
            "Image:" + imageIndex);
          // check that the number of Z sections, channels, and timepoints
          // all match expectations
          assertEquals(metadata.getPixelsSizeZ(imageIndex).getValue(), sizeZ);
          assertEquals(metadata.getPixelsSizeC(imageIndex).getValue(), sizeC);
          assertEquals(metadata.getPixelsSizeT(imageIndex).getValue(), sizeT);
        }
      }
    }
  }

  /**
   * Make sure that a TIFF file that doesn't follow the expected BioTek naming
   * conventions is not read by the BioTek reader.
   */
  @Test
  public void testInvalidPath() throws IOException {
    // this is the same test file that we copy to make artifical plates
    // just without a name that matches what we expect for BioTek data
    Path testTiff = getTestFile("test.tiff");

    try (BioTekReader reader = new BioTekReader()) {
      // make sure the reader recognizes that this is not a BioTek file
      assertFalse(reader.isThisType(testTiff.toString()));

      // make sure that an exception is thrown if we try to force
      // the reader to read it anyway
      assertThrows(IllegalArgumentException.class, () -> {
        reader.setId(testTiff.toString());
      });
    }
  }

  /**
   * Generate test cases that are given to testBioTek(...) above.
   * A list of file names, the well row and column index, number of fields,
   * and ZCT dimensions are all specified here.
   *
   * These are all names taken from real-world data, with minor sanitization
   * of some of the channel names and wavelengths.
   *
   * @return BioTek plate test cases
   */
  static Stream<Arguments> getTestCases() {
    return Stream.of(
      Arguments.of(new String[] {
        "A1_-1_1_1_Tsf[Phase Contrast]_001.tif"
      }, new int[] {0}, new int[] {0}, 1, 1, 1, 1),
      Arguments.of(new String[] {
        "A1_01_1_1_Phase Contrast_001.tif",
        "A1_01_1_2_Phase Contrast_001.tif",
        "A1_01_1_3_Phase Contrast_001.tif",
        "A1_01_1_4_Phase Contrast_001.tif",
        "A1_01_1_5_Phase Contrast_001.tif",
        "A1_01_1_6_Phase Contrast_001.tif",
        "A1_01_1_7_Phase Contrast_001.tif",
        "A1_01_1_8_Phase Contrast_001.tif",
        "A1_01_1_9_Phase Contrast_001.tif",
      }, new int[] {0}, new int[] {0}, 9, 1, 1, 1),
      Arguments.of(new String[] {
        "P24_1_Bright Field_1_001_02.tif"
      }, new int[] {15}, new int[] {23}, 1, 1, 1, 1),
      Arguments.of(new String[] {
        "B2_1_Bright Field_1_001_02.tif"
      }, new int[] {1}, new int[] {1}, 1, 1, 1, 1),
      Arguments.of(new String[] {
        "A1_1_Stitched[AandB_Phase Contrast]_1_001_-1.tif"
      }, new int[] {0}, new int[] {0}, 1, 1, 1, 1),
      Arguments.of(new String[] {
        "A1_1Z0_DAPI_1_001_.tif",
        "A1_1Z1_DAPI_1_001_.tif",
        "A1_1Z2_DAPI_1_001_.tif",
        "A1_1Z3_DAPI_1_001_.tif",
        "A1_1Z4_DAPI_1_001_.tif"
      }, new int[] {0}, new int[] {0}, 1, 5, 1, 1),
      Arguments.of(new String[] {
        "A1_-1_1_1_Stitched[Channel1 300,400]_001.tif",
        "A1_-1_2_1_Stitched[Channel2 500,600]_001.tif",
        "A1_-1_3_1_Stitched[Channel 3 600,650]_001.tif"
      }, new int[] {0}, new int[] {0}, 1, 1, 3, 1),
      Arguments.of(new String[] {
        "A1_-2_1_1_Tsf[Stitched[Channel1 300,400]]_001.tif",
        "A1_-2_2_1_Tsf[Stitched[Channel2 500,600]]_001.tif",
        "A1_-2_3_1_Tsf[Stitched[Channel 3 600,650]]_001.tif"
      }, new int[] {0}, new int[] {0}, 1, 1, 3, 1),
      Arguments.of(new String[] {
        "B2/B2_1_Bright Field_1_001_02.tif",
        "D3/D3_1_Bright Field_1_001_02.tif",
      }, new int[] {1, 3}, new int[] {1, 2}, 1, 1, 1, 1),
      Arguments.of(new String[] {
        "ID_A9 info/A9_-1_1_1_Phase Contrast_001.tif",
        "ID_A9 info/A9_-1_2_1_GFP_001.tif",
        "ID_A9 info/A9_-1_3_1_DAPI_001.tif",
        "ID_A10 info/A10_-1_1_1_Phase Contrast_001.tif",
        "ID_A10 info/A10_-1_2_1_GFP_001.tif",
        "ID_A10 info/A10_-1_3_1_DAPI_001.tif",
        "ID_E9 info/E9_-1_1_1_Phase Contrast_001.tif",
        "ID_E9 info/E9_-1_2_1_GFP_001.tif",
        "ID_E9 info/E9_-1_3_1_DAPI_001.tif",
        "ID_E10 info/E10_-1_1_1_Phase Contrast_001.tif",
        "ID_E10 info/E10_-1_2_1_GFP_001.tif",
        "ID_E10 info/E10_-1_3_1_DAPI_001.tif"
      }, new int[] {0, 0, 4, 4}, new int[] {8, 9, 8, 9}, 1, 1, 3, 1),
      Arguments.of(new String[] {
        "A1ROI1_01_1_1Z0_Confocal DAPI_001.tif",
        "A1ROI1_01_1_1Z1_Confocal DAPI_001.tif",
        "A1ROI1_01_1_1Z2_Confocal DAPI_001.tif",
        "A1ROI1_01_1_1Z0_Confocal RFP_001.tif",
        "A1ROI1_01_1_1Z1_Confocal RFP_001.tif",
        "A1ROI1_01_1_1Z2_Confocal RFP_001.tif",
        "A1ROI1_01_1_2Z0_Confocal DAPI_001.tif",
        "A1ROI1_01_1_2Z1_Confocal DAPI_001.tif",
        "A1ROI1_01_1_2Z2_Confocal DAPI_001.tif",
        "A1ROI1_01_1_2Z0_Confocal RFP_001.tif",
        "A1ROI1_01_1_2Z1_Confocal RFP_001.tif",
        "A1ROI1_01_1_2Z2_Confocal RFP_001.tif",
        "A1ROI2_01_1_1Z0_Confocal DAPI_001.tif",
        "A1ROI2_01_1_1Z1_Confocal DAPI_001.tif",
        "A1ROI2_01_1_1Z2_Confocal DAPI_001.tif",
        "A1ROI2_01_1_1Z0_Confocal RFP_001.tif",
        "A1ROI2_01_1_1Z1_Confocal RFP_001.tif",
        "A1ROI2_01_1_1Z2_Confocal RFP_001.tif",
        "A1ROI2_01_1_2Z0_Confocal DAPI_001.tif",
        "A1ROI2_01_1_2Z1_Confocal DAPI_001.tif",
        "A1ROI2_01_1_2Z2_Confocal DAPI_001.tif",
        "A1ROI2_01_1_2Z0_Confocal RFP_001.tif",
        "A1ROI2_01_1_2Z1_Confocal RFP_001.tif",
        "A1ROI2_01_1_2Z2_Confocal RFP_001.tif"
      }, new int[] {0}, new int[] {0}, 4, 3, 2, 1),
      Arguments.of(new String[] {
        "A1ROI1_01_1_1Z0_Confocal DAPI_001.tif",
        "A1ROI1_01_1_1Z1_Confocal DAPI_001.tif",
        "A1ROI1_01_1_1Z2_Confocal DAPI_001.tif",
        "A1ROI1_01_1_1Z0_Confocal RFP_001.tif",
        "A1ROI1_01_1_1Z1_Confocal RFP_001.tif",
        "A1ROI1_01_1_1Z2_Confocal RFP_001.tif",
        "A1ROI2_01_1_1Z0_Confocal DAPI_001.tif",
        "A1ROI2_01_1_1Z1_Confocal DAPI_001.tif",
        "A1ROI2_01_1_1Z2_Confocal DAPI_001.tif",
        "A1ROI2_01_1_1Z0_Confocal RFP_001.tif",
        "A1ROI2_01_1_1Z1_Confocal RFP_001.tif",
        "A1ROI2_01_1_1Z2_Confocal RFP_001.tif",
        "A1ROI3_01_1_1Z0_Confocal DAPI_001.tif",
        "A1ROI3_01_1_1Z1_Confocal DAPI_001.tif",
        "A1ROI3_01_1_1Z2_Confocal DAPI_001.tif",
        "A1ROI3_01_1_1Z0_Confocal RFP_001.tif",
        "A1ROI3_01_1_1Z1_Confocal RFP_001.tif",
        "A1ROI3_01_1_1Z2_Confocal RFP_001.tif",
      }, new int[] {0}, new int[] {0}, 3, 3, 2, 1),
      Arguments.of(new String[] {
        "A1_01_3_4_Blue_001.tif",
        "A1_01_3_3_Blue_001.tif",
        "A1_01_3_2_Blue_001.tif",
        "A1_01_3_1_Blue_001.tif",
        "A1_01_2_4_Green_001.tif",
        "A1_01_2_3_Green_001.tif",
        "A1_01_2_2_Green_001.tif",
        "A1_01_2_1_Green_001.tif",
        "A1_01_1_4_Red_001.tif",
        "A1_01_1_3_Red_001.tif",
        "A1_01_1_2_Red_001.tif",
        "A1_01_1_1_Red_001.tif"
      }, new int[] {0}, new int[] {0}, 4, 1, 3, 1)
    );
  }

  /**
   * Get the absolute path to a resource for this class.
   *
   * @param resourceName a relative name for something in
   *                     bioformats2raw/src/test/resources/...
   * @return Path object corresponding to the named resource
   * @throws IOException if the resource could not be located
   */
  private Path getTestFile(String resourceName) throws IOException {
    try {
      return Paths.get(this.getClass().getResource(resourceName).toURI());
    }
    catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * @return an empty OMEXMLMetadata object that the BioTek reader can fill up
   */
  private OMEXMLMetadata getOMEMetadata() throws Exception {
    ServiceFactory sf = new ServiceFactory();
    OMEXMLService xmlService = sf.getInstance(OMEXMLService.class);
    return xmlService.createOMEXMLMetadata();
  }
}
