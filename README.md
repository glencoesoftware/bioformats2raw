[![Actions Status](https://github.com/glencoesoftware/bioformats2raw/workflows/Gradle/badge.svg)](https://github.com/glencoesoftware/bioformats2raw/actions)


bioformats2raw converter
========================

Java application to convert image file formats, including .mrxs,
to an intermediate Zarr structure.
The [raw2ometiff](https://github.com/glencoesoftware/raw2ometiff)
application can then be used to produce a
Bio-Formats 5.9.x ("Faas") or Bio-Formats 6.x (true OME-TIFF) pyramid.

Requirements
============

Java 8 or later is required.

libblosc (https://github.com/Blosc/c-blosc) version 1.9.0 or later must be installed separately.
The native libraries are not packaged with any relevant jars.  See also note in jzarr readme (https://github.com/bcdev/jzarr/blob/master/README.md)

 * macOS: `brew install c-blosc` then set `JAVA_OPTS='-Djna.library.path=/usr/local/Cellar/c-blosc/*/lib'`
 * Windows: Pre-built blosc DLLs are available from the [Fiji project](https://sites.imagej.net/N5/lib/win64/).  Rename the downloaded DLL to `blosc.dll` and place in a fixed location then set `JAVA_OPTS="-Djna.library.path=C:\path\to\blosc\folder"`.
 * Ubuntu 18.04+: `apt-get install libblosc1`

__NOTE:__ If you are setting `jna.library.path` via the `JAVA_OPTS` environment variable, make sure the path is to the folder __containing__ the library not path to the library itself.

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

    bioformats2raw /path/to/file.mrxs /path/to/zarr-pyramid --resolutions 6
    bioformats2raw /path/to/file.svs /path/to/zarr-pyramid --resolutions 6

Maximum tile dimensions are can be configured with the `--tile_width` and `--tile_height` options.  Defaults can be viewed with
`bioformats2raw --help`.  `--resolutions` is optional; if omitted, the number of resolutions is set so that the smallest
resolution is no greater than 256x256.

If the input file has multiple series, a subset of the series can be converted by specifying a comma-separated list of indexes:

    bioformats2raw /path/to/file.scn /path/to/zarr-pyramid --series 0,2,3,4

By default, three additional readers (MiraxReader, PyramidTiffReader, and BioTekReader) are added to the beginning of Bio-Formats' list of reader classes.
These readers are considered to be experimental and as a result only a limited range of input data is supported.

Any of these readers can be excluded with the `--extra-readers` option:

    # only include the reader for .mrxs
    bioformats2raw /path/to/file.tiff /path/to/zarr-pyramid --extra-readers com.glencoesoftware.bioformats2raw.MiraxReader
    # don't add any additional readers, just use the ones provided by Bio-Formats
    bioformats2raw /path/to/file.mrxs /path/to/zarr-pyramid --extra-readers

Reader-specific options can be specified using `--options`:

    bioformats2raw /path/to/file.mrxs /path/to/zarr-pyramid --options mirax.use_metadata_dimensions=false

Be aware when experimenting with different values for `--options` that the corresponding memo (cache) file may need to be
removed in order for new options to take effect.  This file will be e.g. `/path/to/.file.mrxs.bfmemo`.

The output in `/path/to/zarr-pyramid` can be passed to `raw2ometiff` to produce
an OME-TIFF that can be opened in ImageJ, imported into OMERO, etc. See
https://github.com/glencoesoftware/raw2ometiff for more information.

Output Formatting Options
=========================

Using any combination of these options will result in a Zarr dataset that is not compatible with raw2ometiff,
but may be suitable for other applications.

#### --pyramid-name

Specifies a subdirectory of the output directory where Zarr data should be written.
Using this option will insert another level into the output hierarchy, for example:

    $ bin/bioformats2raw --pyramid-name pyramid-test "test&sizeX=4096&sizeY&=4096&sizeZ=3.fake" example1
    $ tree example1
    example1/
    └── pyramid-test
        ├── 0
        │   ├── 0
               ...
        │   ├── 1
               ...
        │   ├── 2
               ...
        │   ├── 3
               ...
        │   └── 4
               ...
        └── OME
            └── METADATA.ome.xml

#### --scale-format-string

A [Java format string](https://docs.oracle.com/javase/8/docs/api/java/util/Formatter.html#syntax) that defines
how series and resolutions should be described in the output directory hierarchy.
The default value is `%d/%d`; the first argument supplied to the format string is the series index (from 0)
and the second argument is the resolution index (also from 0).

An example of removing the series index from the hierarchy altogether:

    $ bin/bioformats2raw --scale-format-string '%2$d/' "test&sizeX=4096&sizeY&=4096&sizeZ=3.fake" example2
    $ tree example2
    example2/
    ├── 0
       ...
    ├── 1
       ...
    ├── 2
       ...
    ├── 3
       ...
    ├── 4
    └── OME
        └── METADATA.ome.xml

Note the trailing `/` and quotes around the `--scale-format-string` argument.
Omitting the trailing `/` may cause an error, and single quotes are often necessary to prevent shell expansion.

#### --additional-scale-format-string-args

Specify a .csv file that contains additional information which can be used by `--scale-format-string`.
The .csv must contain one row per series, and must not contain a header row.
Each column will be passed as an additional argument when formatting the `--scale-format-string` argument.

An example .csv that defines a unique ID, acquisition date, and username for a series:

    $ cat example3.csv
    12345,2021-07-21,user_xyz

which can then be used to create a hierarchy by date, username, and ID:

    $ bin/bioformats2raw --additional-scale-format-string-args example3.csv --scale-format-string '%4$s/%5$s/%3$s/%1$d/%2$d' "test&sizeX=4096&size&=4096&sizeZ=3.fake" example3
    $ tree example3
    example3/
    ├── 2021-07-21
    │   └── user_xyz
    │       └── 12345
    │           └── 0
    │               ├── 0
                       ...
    │               ├── 1
                       ...
    │               ├── 2
                       ...
    │               ├── 3
                       ...
    │               └── 4
                       ...
    └── OME
        └── METADATA.ome.xml

#### --no-ome-meta-export

Prevents the input file's OME-XML metadata from being saved.  There will no longer be an `OME` directory
under the top-level output directory.

#### --no-root-group

By default, a Zarr group (`.zgroup` file) is written in the top-level output directory.
Adding the `--no-root-group` option prevents this group from being written.

Usage Changes
=============

Versions 0.2.6 and prior supported both N5 and Zarr output using the `--file_type` option.
This option is not present in 0.3.0 and later, as only Zarr output is supported.

Versions 0.2.6 and prior used the input file's dimension order to determine the output
dimension order, unless `--dimension-order` was specified.
Version 0.3.0 uses the `TCZYX` order by default, for compatibility with https://ngff.openmicroscopy.org/0.2/#image-layout.
The `--dimension-order` option can still be used to set a specific output dimension order, e.g.:

    bioformats2raw /path/to/file.mrxs /path/to/zarr-pyramid --dimension-order XYCZT

or can be set to use the input file's ordering, preserving the behavior of 0.2.6:

    bioformats2raw /path/to/file.mrxs /path/to/zarr-pyramid --dimension-order original

If a specific dimension order is passed to `--dimension-order`, it must be a valid dimension order as defined in
the [OME 2016-06 schema](https://www.openmicroscopy.org/Schemas/Documentation/Generated/OME-2016-06/ome_xsd.html#Pixels_DimensionOrder).
The specified dimension order is then reversed when creating Zarr arrays, e.g. `XYCZT` would become `TZCYX` in Zarr.

Prior to version 0.3.0, N5/Zarr output was placed in a subdirectory (`data.[n5|zarr]`) with a `METADATA.ome.xml` file
at the same level.  As of 0.3.0 the desired output directory is now a Zarr group and the `METADATA.ome.xml` file is
placed in a `OME` directory within.  These changes reflect layout version 3.

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

The worker count defaults to the number of detected CPUs.  This may or may not be appropriate for the chosen input data.
If reading a single tile from the input data requires a lot of memory, decreasing the worker count will be necessary
to prevent memory exhaustion.  JPEG, PNG, and certain TIFFs are especially susceptible to this problem.

The worker count should be set to 1 if the input data requires a Bio-Formats reader that is not thread-safe.
This is not a common case, but is a known issue with Imaris HDF data in particular.

In general, expect to need to tune the above settings and measure
relative performance.

License
=======

The converter is distributed under the terms of the GPL license.
Please see `LICENSE.txt` for further details.
