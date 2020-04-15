# Development Dockerfile for bioformats2raw
# -----------------------------------------

# To install the built distribution into other runtimes
# pass a build argument, e.g.:
#
#   docker build --build-arg IMAGE=openjdk:9 ...
#

# Similarly, the BUILD_IMAGE argument can be overwritten
# but this is generally not needed.
ARG BUILD_IMAGE=gradle:5.2.1-jdk8

#
# Build phase: Use the gradle image for building.
#
FROM ${BUILD_IMAGE} as build
USER root
RUN apt-get update -qq && apt-get install -y -qq zeroc-ice-all-dev libblosc-dev
RUN echo Temporary && apt-get install -y -qq git maven
RUN mkdir /bioformats2raw && chown 1000:1000 /bioformats2raw

# Build all
USER 1000

COPY --chown=1000:1000 . /bioformats2raw
WORKDIR /bioformats2raw
RUN echo Temporary \
 && git clone -b 0.0.2-beta git://github.com/saalfeldlab/n5-zarr \
 && cd n5-zarr \
 && mvn install -DskipTests
RUN gradle build
RUN cd build/distributions && rm bioformats2raw*tar && unzip bioformats2raw*zip && rm -rf bioformats2raw*zip
USER root
RUN mv /bioformats2raw/build/distributions/bioformats2raw* /opt/bioformats2raw
USER 1000
WORKDIR /opt/bioformats2raw
ENTRYPOINT ["/opt/bioformats2raw/bin/bioformats2raw"]
