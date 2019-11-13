mrxs2raw converter
==================

Java application to convert an .mrxs dataset to an intermediate N5 structure.
The raw2ometiff application can then be used to produce a
Bio-Formats 5.9.x ("Faas") or Bio-Formats 6.x (true OME-TIFF) pyramid.


Usage
=====

Build with Gradle:

    gradle clean build

Unpack the distribution:

    cd build/distributions
    unzip mrxs2raw-$VERSION.zip
    cd mrxs2raw-$VERSION

Run the conversion:

    bin/mrxs2raw /path/to/file.mrxs /path/to/n5-pyramid --resolutions 6

Maximum tile dimensions are 2048x2048, and can be configured with the `--tile_width` and `--tile_height` options.
`--resolutions` is optional; if omitted, the number of resolutions is set so that the smallest resolution is no greater than 256x256.
