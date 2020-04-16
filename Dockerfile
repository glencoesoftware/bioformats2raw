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
RUN apt-get update -qq && apt-get install -y -qq zeroc-ice-all-runtime libblosc1
RUN mkdir /bioformats2raw && chown 1000:1000 /bioformats2raw

# Build all
USER 1000

COPY --chown=1000:1000 . /bioformats2raw
WORKDIR /bioformats2raw
RUN gradle build
RUN cd build/distributions && rm bioformats2raw*tar && unzip bioformats2raw*zip && rm -rf bioformats2raw*zip
USER root
RUN mv /bioformats2raw/build/distributions/bioformats2raw* /opt/bioformats2raw
USER 1000
WORKDIR /opt/bioformats2raw
ENTRYPOINT ["/opt/bioformats2raw/bin/bioformats2raw"]
