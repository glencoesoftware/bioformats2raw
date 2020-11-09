/**
 * Copyright (c) 2019 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.bioformats2raw;


import org.janelia.saalfeldlab.n5.Bzip2Compression;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.Lz4Compression;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.XzCompression;
import org.janelia.saalfeldlab.n5.blosc.BloscCompression;

public class N5Compression {
  enum CompressionTypes { blosc, bzip2, gzip, lz4, raw, xz };

  /**
   * Get an N5 compressor for the given compression type.
   *
   * @param type compression type
   * @param compressionParameter type-specific parameter (may be null)
   * @return Compression object that can compress data, or null if
             the compression type is not recognized
   */
  public static Compression getCompressor(
          CompressionTypes type,
          Integer compressionParameter)
  {
    switch (type) {
      case blosc:
        return new BloscCompression(
                "lz4",
                5, // clevel
                BloscCompression.SHUFFLE,  // shuffle
                0, // blocksize (0 = auto)
                1  // nthreads
        );
      case gzip:
        if (compressionParameter == null) {
          return new GzipCompression();
        }
        else {
          return new GzipCompression(compressionParameter.intValue());
        }
      case bzip2:
        if (compressionParameter == null) {
          return new Bzip2Compression();
        }
        else {
          return new Bzip2Compression(compressionParameter.intValue());
        }
      case xz:
        if (compressionParameter == null) {
          return new XzCompression();
        }
        else {
          return new XzCompression(compressionParameter.intValue());
        }
      case lz4:
        if (compressionParameter == null) {
          return new Lz4Compression();
        }
        else {
          return new Lz4Compression(compressionParameter.intValue());
        }
      case raw:
        return new RawCompression();
      default:
        return null;
    }
  }
}
