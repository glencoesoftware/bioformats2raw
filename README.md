bioformats2raw converter
========================

Java application to convert image file formats, including .mrxs,
to an intermediate N5/Zarr structure.
The [raw2ometiff](https://github.com/glencoesoftware/raw2ometiff)
application can then be used to produce a
Bio-Formats 5.9.x ("Faas") or Bio-Formats 6.x (true OME-TIFF) pyramid.

Requirements
============

libblosc (https://github.com/Blosc/c-blosc) version 1.9.0 or later must be installed separately.
The native libraries are not packaged with any relevant jars.  See also note in n5-zarr readme (https://github.com/saalfeldlab/n5-zarr/blob/0.0.2-beta/README.md)

 * Mac OSX: `brew install c-blosc`
 * Ubuntu 18.04+: `apt-get install libblosc1`

Installation
============

1. Download and unpack a release artifact:

    https://github.com/glencoesoftware/bioformats2raw/releases

Development Installation
========================

1. Clone the repository:

    git clone git@github.com:glencoesoftware/bioformats2raw.git

2. Run the Gradle build as required, a list of available tasks can be found by running:

    ./gradlew tasks

Eclipse Configuration
=====================

1. Run the Gradle Eclipse task:

    ./gradlew eclipse

Usage
=====

Run the conversion:

    bioformats2raw /path/to/file.mrxs /path/to/n5-pyramid --resolutions 6
    bioformats2raw /path/to/file.svs /path/to/n5-pyramid --resolutions 6

Maximum tile dimensions are can be configured with the `--tile_width` and `--tile_height` options.  Defaults can be viewed with
`bioformats2raw --help`.  `--resolutions` is optional; if omitted, the number of resolutions is set so that the smallest
resolution is no greater than 256x256.

By default, two additional readers (MiraxReader and PyramidTiffReader) are added to the beginning of Bio-Formats' list of reader classes.
Either or both of these readers can be excluded with the `--extra-readers` option:

    # only include the reader for .mrxs, exclude the reader for Faas pyramids
    bioformats2raw /path/to/file.tiff /path/to/n5-pyramid --extra-readers com.glencoesoftware.bioformats2raw.MiraxReader
    # don't add any additional readers, just use the ones provided by Bio-Formats
    bioformats2raw /path/to/file.mrxs /path/to/n5-pyramid --extra-readers

Reader-specific options can be specified using `--options`:

    bioformats2raw /path/to/file.mrxs /path/to/n5-pyramid --options mirax.use_metadata_dimensions=false

Be aware when experimenting with different values for `--options` that the corresponding memo (cache) file may need to be
removed in order for new options to take effect.  This file will be e.g. `/path/to/.file.mrxs.bfmemo`.

The output in `/path/to/n5-pyramid` can be passed to `raw2ometiff` to produce
an OME-TIFF that can be opened in ImageJ, imported into OMERO, etc. See
https://github.com/glencoesoftware/raw2ometiff for more information.

Performance
===========

This package is __highly__ sensitive to underlying hardware as well as
the following configuration options:

 * `--max_workers`
 * `--tile_width`
 * `--tile_height`

On systems with significant I/O bandwidth, particularly SATA or
NVMe based storage, you may find sharply diminishing returns with high
worker counts.  There are significant performance gains to be had utilizing
larger tile sizes but be mindful of the consequences on the downstream
workflow.

In general, expect to need to tune the above settings and measure
relative performance.

License
=======

The converter is distributed under the terms of the GPL license.
Please see `LICENSE.txt` for further details.
