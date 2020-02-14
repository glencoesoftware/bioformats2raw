bioformats2raw converter
========================

Java application to convert whole slide imaging file formats, including .mrxs,
to an intermediate N5 structure.
The raw2ometiff application can then be used to produce a
Bio-Formats 5.9.x ("Faas") or Bio-Formats 6.x (true OME-TIFF) pyramid.

Requirements
============

libblosc (https://github.com/Blosc/c-blosc) version 1.9.0 or later must be installed separately.
The native libraries are not packaged with any relevant jars.  See also note in n5-zarr readme (https://github.com/saalfeldlab/n5-zarr/blob/0.0.2-beta/README.md)

Usage
=====

Build with Gradle:

    gradle clean build

Unpack the distribution:

    cd build/distributions
    unzip bioformats2raw-$VERSION.zip
    cd bioformats2raw-$VERSION

Run the conversion:

    bin/bioformats2raw /path/to/file.mrxs /path/to/n5-pyramid --resolutions 6
    bin/bioformats2raw /path/to/file.svs /path/to/n5-pyramid --resolutions 6

Maximum tile dimensions are can be configured with the `--tile_width` and `--tile_height` options.  Defaults can be viewed with
`bin/bioformats2raw --help`.  `--resolutions` is optional; if omitted, the number of resolutions is set so that the smallest
resolution is no greater than 256x256.

By default, two additional readers (MiraxReader and PyramidTiffReader) are added to the beginning of Bio-Formats' list of reader classes.
These and any other readers can be removed from the list with the `--exclude_readers` option:

    # exclude the reader for Faas pyramids
    bin/bioformats2raw /path/to/file.tiff /path/to/n5-pyramid --exclude_readers PyramidTiffReader
    # exclude MiraxReader and JPEGReader, so .mrxs throws UnknownFormatException
    bin/bioformats2raw /path/to/file.mrxs /path/to/n5-pyramid --exclude_readers MiraxReader,JPEGReader
