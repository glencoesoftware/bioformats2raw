[![Actions Status](https://github.com/glencoesoftware/bioformats2raw/workflows/Gradle/badge.svg)](https://github.com/glencoesoftware/bioformats2raw/actions)


bioformats2raw converter
========================

Java application to convert image file formats, including .mrxs,
to an intermediate Zarr structure compatible with the OME-NGFF
specification.
The [raw2ometiff](https://github.com/glencoesoftware/raw2ometiff)
application can then be used to produce a
Bio-Formats 5.9.x ("Faas") or Bio-Formats 6.x (true OME-TIFF) pyramid.

Requirements
============

Java 8 or later is required.

libblosc (https://github.com/Blosc/c-blosc) version 1.9.0 or later must be installed separately.
The native libraries are not packaged with any relevant jars.  See also note in jzarr readme (https://github.com/bcdev/jzarr/blob/master/README.md)

 * macOS: `brew install c-blosc` then set `JAVA_OPTS=-Djna.library.path=$(echo $(brew --cellar c-blosc)/*/lib/)`
 * Windows: Pre-built blosc DLLs are available from the [Fiji project](https://sites.imagej.net/N5/lib/win64/).  Rename the downloaded DLL to `blosc.dll` and place in a fixed location then set `JAVA_OPTS="-Djna.library.path=C:\path\to\blosc\folder"`.
 * Ubuntu 18.04+: `apt-get install libblosc1`
 * conda: Installing `bioformats2raw` via conda (see below) will include `blosc` as a dependency.

If using features that rely on OpenCV (see the [Downsampling type](#downsampling-type) section below), minimum supported versions are:

 * Ubuntu 18.04
 * RHEL 8
 * Windows 10
   - expect to see warnings as described in https://github.com/opencv/opencv/issues/20113; these can be ignored

__NOTE:__ If you are setting `jna.library.path` via the `JAVA_OPTS` environment variable, make sure the path is to the folder __containing__ the library not path to the library itself.

Installation
============

1. Download and unpack a release artifact:

    https://github.com/glencoesoftware/bioformats2raw/releases

2. OR, install via `conda` as described at [conda-bioformats2raw](https://github.com/ome/conda-bioformats2raw).

Development Installation
========================

1. Clone the repository:

    git clone https://github.com/glencoesoftware/bioformats2raw.git

2. Run the Gradle build as required, a list of available tasks can be found by running:

    ./gradlew tasks

Configuring Logging
===================

Logging is provided using the logback library. The `logback.xml` file in `src/dist/lib/config/` provides a default configuration for the command line tool.
In release and snapshot artifacts, `logback.xml` is in `lib/config/`.
You can configure logging by editing the provided `logback.xml` or by specifying the path to a different file:

    JAVA_OPTS="-Dlogback.configurationFile=/path/to/external/logback.xml" \
    bioformats2raw ...

Alternatively you can use the `--debug` flag, optionally writing the stdout to a file:

    bioformats2raw /path/to/file.mrxs /path/to/zarr-pyramid --debug > bf2raw.log

The `--log-level` option takes an [slf4j logging level](https://www.slf4j.org/faq.html#fatal) for additional simple logging configuration.
`--log-level DEBUG` is equivalent to `--debug`. For even more verbose logging:

    bioformats2raw /path/to/file.mrxs /path/to/zarr-pyramid --log-level TRACE

Eclipse Configuration
=====================

1. Run the Gradle Eclipse task:

    ./gradlew eclipse

2. Add the logback configuration in `src/dist/lib/config/` to your CLASSPATH.

Usage
=====

Run the conversion:

    bioformats2raw /path/to/file.mrxs /path/to/zarr-pyramid
    bioformats2raw /path/to/file.svs /path/to/zarr-pyramid

By default, the resolutions will be set so that the smallest resolution is no greater than 256x256.
The target of the smallest resolution can be configured with `--target-min-size` e.g. to ensure
that the smallest resolution is no greater than 128x128

    bioformats2raw /path/to/file.mrxs /path/to/zarr-pyramid --target-min-size 128
    bioformats2raw /path/to/file.svs /path/to/zarr-pyramid --target-min-size 128


Alternatively, the `--resolutions` options can be passed to specify the exact number of resolution levels:

    bioformats2raw /path/to/file.mrxs /path/to/zarr-pyramid --resolutions 6
    bioformats2raw /path/to/file.svs /path/to/zarr-pyramid --resolutions 6


Maximum tile dimensions can be configured with the `--tile-width` and `--tile-height` options.  Defaults can be viewed with
`bioformats2raw --help`. Be mindful of the downstream workflow when selecting a tile size other than the default.
A smaller than default tile size is rarely recommended.

If the input file has multiple series, a subset of the series can be converted by specifying a comma-separated list of indexes:

    bioformats2raw /path/to/file.scn /path/to/zarr-pyramid --series 0,2,3,4

By default, several additional readers are added to the beginning of Bio-Formats' list of reader classes.
These readers are considered to be experimental and as a result only a limited range of input data is supported.
See the [Additional readers](#additional-readers) section below for more information.

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

By default, the output of `bioformats2raw` will be a
[Zarr dataset](https://zarr.readthedocs.io/en/stable/spec/v2.html) which follows the
metadata conventions defined by the
[OME-NGFF 0.4 specification](https://ngff.openmicroscopy.org/0.4/) including the
[bioformats2raw.layout specification](https://ngff.openmicroscopy.org/0.4/#bf2raw).

Several formatting options can be passed to the converter and will result in a Zarr dataset
that is not compatible with raw2ometiff and does not strictly follow the OME-NGFF
specification but may be suitable for other applications.

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

#### --no-original-metadata

Prevents `OriginalMetadata` annotations from being written to `OME/METADATA.ome.xml`.
This can reduce the size of the OME-XML, and as discussed in [#250](https://github.com/glencoesoftware/bioformats2raw/issues/250),
is one way to guard against unwanted experimental metadata being included in the conversion.

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

Prior to version 0.5.0, the plate and series Zarr groups followed the metadata defined in
the [0.2 version of the OME-NGFF specification](https://ngff.openmicroscopy.org/0.2). As of
0.5.0, these groups now follow the metadata conventions defined in the
[0.4 version of the OME-NGFF specification](https://ngff.openmicroscopy.org/0.4). Additionally,
the layout of the top-level Zarr group is now part of the upstream specification - see
https://ngff.openmicroscopy.org/0.4/#bf2raw and the `OME` directory containing the
`METADATA.ome.xml` file is now a Zarr group.

Versions 0.5.0 and later write [OMERO rendering metadata](https://ngff.openmicroscopy.org/0.4/#omero-md)
by default. This includes calculating the minimum and maximum pixel values for the entire image.
We recommend keeping this metadata for maximum compatibility with downstream applications, but it can
be omitted by using the `--no-minmax` option.

Performance
===========

This package is __highly__ sensitive to underlying hardware as well as
the following configuration options:

 * `--max-workers`
 * `--tile-width`
 * `--tile-height`

On systems with significant I/O bandwidth, particularly SATA or
NVMe based storage, you may find sharply diminishing returns with high
worker counts.  There are significant performance gains to be had utilizing
larger tile sizes but be mindful of the consequences on the downstream
workflow. Smaller tile sizes than the default are rarely recommended.

The worker count defaults to the number of detected CPUs.  This may or may not be appropriate for the chosen input data.
If reading a single tile from the input data requires a lot of memory, decreasing the worker count will be necessary
to prevent memory exhaustion.  JPEG, PNG, and certain TIFFs are especially susceptible to this problem.

The worker count should be set to 1 if the input data requires a Bio-Formats reader that is not thread-safe.
This is not a common case, but is a known issue with Imaris HDF data in particular.

In general, expect to need to tune the above settings and measure
relative performance.

Metadata caching
================

During conversion, a temporary `.*.bfmemo` file may be created. By default, this file is in the same directory as the input data
and will be removed after the conversion finishes. The location of the `.*.bfmemo` file can be configured using the `--memo-directory` option:

    bioformats2raw /path/to/file.mrxs /path/to/zarr-pyramid --memo-directory /tmp/

This is particularly helpful if you do not have write permissions in the input data directory.

As of version 0.5.0, `.*.bfmemo` files are deleted at the end of conversion by default. We do not recommend keeping these files for normal
conversions, but if they are needed for troubleshooting then the `--keep-memo-files` option can be used. Note that if a memo file did not
need to be created, `--keep-memo-files` will still result in no `.*.bfmemo` files at the end of conversion. This is particularly common
for small datasets that can be read very quickly.

Downsampling type
=================

By default, pyramid resolutions are generated using a [very simple downsampling algorithm](https://github.com/ome/ome-common-java/blob/master/src/main/java/loci/common/image/SimpleImageScaler.java).
For some input data types, this may not be ideal. The `--downsample-type` option can be used to specify an alternative algorithm.
Supported values are `SIMPLE` (default), `GAUSSIAN`, `AREA`, `LINEAR`, `CUBIC`, and `LANCZOS`, as declared in the [Downsampling enum](https://github.com/glencoesoftware/bioformats2raw/blob/master/src/main/java/com/glencoesoftware/bioformats2raw/Downsampling.java).
No additional downsampling algorithms are directly implemented in bioformats2raw; OpenCV is used to for any value of `--downsample-type` other than the default.

If the minimum system requirements (see above) are not met, or the input data type is int8 or int32 (see https://github.com/glencoesoftware/bioformats2raw/pull/199),
then any value of `--downsample-type` other than the default is expected to throw an exception.

Additional readers
==================

Readers are listed here in the order in which they appear on the reader list.

PyramidTiffReader
-----------------

Supports TIFF files that contain a pyramid, with one pyramid resolution per IFD. While this is different from standard pyramid OME-TIFF files,
any OME-XML stored in the first IFD's `ImageDescription` tag will be used to set the number of channels, timepoints, and Z sections.
If no OME-XML is present, each IFD is assumed to represent one channel at a particular resolution; multiple IFDs at the same resolution
therefore indicates multiple channels.

MiraxReader
-----------

Supports 3D HISTECH .mrxs data. Only the full-resolution image is read; bioformats2raw will generate a pyramid from
the full-resolution image, but will not read the original pyramid for this format. Datasets in this format include
a .mrxs file (which is a JPEG thumbnail), along with a similarly-named directory containing a `Slidedat.ini`, `Index.dat`,
and many `Data*.dat` files. The .mrxs file alone does not contain anything apart from the thumbnail;
it is very important to include the entire corresponding directory when transferring these datasets.

The `mirax.use_metadata_dimensions` reader option can be used change how XY dimensions are calculated.
By default, this option is `true`, but setting it to `false` may be helpful if the image size appears incorrect.

BioTekReader
------------

Supports BioTek Cytation 5 plates. Plates in this format consist of .tif files that follow a specific naming scheme;
unlike most other plate formats, there are no metadata files that describe the whole plate. All files for a plate must be in
the same folder as the selected bioformats2raw input file. File names must match one of a limited set of regular expressions:

* `([A-Z]{1,2})(\\d{1,2})_(-?\\d+)_(\\d+)_(\\d+)_([A-Za-z0-9 ,\\[\\]]+)_(\\d+).tif[f]?`
  - This corresponds to: `<well row letter><well column index>_<ignored index>_<ignored index>_<field index>_<channel name>_<ignored index>`
  - Examples:
    - `A1_01_1_1_Phase Contrast_001.tif` (well A1, field 1, `Phase Contrast` channel)
    - `P24_01_1_9_DAPI_002.tif` (well P24, field 9, `DAPI` channel)
    - `A1_-2_1_1_Tsf[Stitched[Channel1 300,400]]_001.tif` (well A1, field 1, `Tsf[Stitched[Channel1 300,400]]` channel)
* `([A-Z]{1,2})(\\d{1,2})_(-?\\d+)(Z(\\d+))?_([A-Za-z0-9 ,\\[\\]]+)_(\\d+)_(\\d+)_(\\d+)?.tif[f]?`
  - This corresponds to: `<well row letter><well column index>_<field index><optional 'Z' and index>_<channel name>_<ignored index>_<t index>_<optional ignored index>`
  - Examples:
    - `A1_1Z0_DAPI_1_001_.tif` (well A1, field 1, Z slice 0, `DAPI` channel, timepoint 1)
    - `A1_1Z4_DAPI_1_003_.tif` (well A1, field 1, Z slice 4, `DAPI` channel, timepoint 3)
    - `B2_1_Bright Field_1_001_02.tif` (well B2, field 1, `Bright Field` channel, timepoint 1)
* `([A-Z]{1,2})(\\d{1,2})_(-?\\d+)_.+\\[(.+)_([A-Za-z0-9 ,\\[\\]]+)\\]_(\\d+)_(\\d+)_([0-9-]+)?.tif[f]?`
  - This corresponds to: `<well row letter><well column index>_<field index>_<optional ignored data>[<ignored data>_<channel name>]_<ignored index>_<t index>_<optional ignored index>`
  - Example:
    - `H10_1_Stitched[AandB_Phase Contrast]_1_001_-1.tif` (well H10, field 1, `Phase Contrast` channel, timepoint 1)

If the input file does not match the given regular expression, then the basic TIFF reader will be used to convert the
single input file without looking for other .tif files. It is especially important to check the conversion output when
working with BioTek plates for the first time or after any acquisition system updates, as there will not be an error in
the logs if the file name does not match any of the above regular expressions.

ND2PlateReader
--------------

Supports grouping multiple .nd2 files into a single HCS plate. To our knowledge, .nd2 files contain no HCS metadata or
awareness that multiple files are part of the same acquisition. This reader relies entirely upon the file name structure
to group .nd2 files in the same directory into a plate. Each file is assumed to represent one well, which may contain
multiple fields. All files for a plate must be in the same folder as the selected bioformats2raw input file.

File names must match the regular expression `(.*_?)Well([A-Z])(\\d{2})_Channel(.*)_Seq(\\d{4}).nd2`, e.g.
`Plate000_WellB02_ChannelDAPI,CY5,CY3_Seq0000.nd2`. In this case, `Plate000` is the plate name; only files with the
same plate name will be grouped together.

If the input file does not match the given regular expression, then the base `ND2Reader` will be used to convert the
single input file without looking for other .nd2 files. It is especially important to check the conversion output when
working with ND2 plates for the first time or after any acquisition system updates, as there will not be an error in
the logs if the file name does not match `ND2PlateReader`'s expectations.

Object Storage
==============

Support for object storage is handled through Java NIO2.
Currently, only the output can be on cloud storage, and `--overwrite` is not supported.

AWS S3
------

S3 support is provided by [s3fs](https://github.com/lasersonlab/Amazon-S3-FileSystem-NIO2/).
Various parameters can be specified through `--output-options`.

Google Cloud Storage (GCS)
--------------------------

Credentials are handled through [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials).
The credentials used require read/write access to the bucket. (Minimally, this can be `Storage Object Creator`,
`Storage Object Viewer` and `Storage Object Delete`).

`--output-options` are *not* currently supported with GCS.
=======
MCDReader
---------

Supports Fluidigm Hyperion .mcd data. Both raw data and panorama images are read. The raw data typically has a small XY size,
but most panorama images are large enough that a pyramid will be generated.

Note that the `X`, `Y`, and `Z` channels in the raw data are calibration images. While these 3 channels appear first
in the .mcd file, `MCDReader` moves them to the end of the channel list for a better viewing experience in OMERO.

License
=======

The converter is distributed under the terms of the GPL license.
Please see `LICENSE.txt` for further details.
