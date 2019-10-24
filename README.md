mrxs2ometiff converter
======================

Java application to convert an .mrxs dataset to a TIFF/OME-TIFF pyramid.
Both Bio-Formats 5.9.x ("Faas") and Bio-Formats 6.x (true OME-TIFF) pyramids are supported.


Usage
=====

Build with Gradle:

    gradle clean build

Unpack the distribution:

    cd build/distributions
    unzip mrxs2ometiff-$VERSION.zip
    cd mrxs2ometiff-$VERSION

Run the conversion (for a 5.9.x pyramid):

    bin/mrxs2ometiff /path/to/file.mrxs --output pyramid.tiff --resolutions 6 --legacy

Run the conversion (for a 6.x pyramid):

    bin/mrxs2ometiff /path/to/file.mrxs --output pyramid.ome.tiff --resolutions 6

The compression defaults to JPEG-2000, but can be configured with the `--compression` option.
Maximum tile dimensions are 2048x2048, and can be configured with the `--tile-width` and `--tile-height` options.
