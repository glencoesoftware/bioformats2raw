/**
 * Copyright (c) 2019 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.bioformats2raw;

import java.io.IOException;

import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.zarr.N5ZarrReader;
import org.janelia.saalfeldlab.n5.zarr.N5ZarrWriter;

/**
 * Enumeration that backs the --file_type flag. Instances can be used
 * as a factory method to create {@link N5Reader} and {@link N5Writer}
 * instances.
 */
public enum FileType {
  n5 {
    N5Reader reader(String path) throws IOException {
      return new N5FSReader(path);
    }
    N5Writer writer(String path) throws IOException {
      return new N5FSWriter(path);
    }
  },
  zarr {
    N5Reader reader(String path) throws IOException {
      return new N5ZarrReader(path);
    }
    N5Writer writer(String path) throws IOException {
      return new N5ZarrWriter(path);
    }
  };
  abstract N5Reader reader(String path) throws IOException;
  abstract N5Writer writer(String path) throws IOException;
}
