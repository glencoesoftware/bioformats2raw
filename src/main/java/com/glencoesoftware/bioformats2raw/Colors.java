/**
 * Copyright (c) 2022 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.bioformats2raw;

import java.util.List;

import loci.formats.ome.OMEXMLMetadata;

import ome.units.UNITS;
import ome.units.quantity.Length;
import ome.units.unit.Unit;
import ome.xml.meta.OMEXMLMetadataRoot;
import ome.xml.model.Channel;
import ome.xml.model.Filter;
import ome.xml.model.FilterSet;
import ome.xml.model.Image;
import ome.xml.model.LightPath;
import ome.xml.model.TransmittanceRange;
import ome.xml.model.primitives.Color;

/**
 * Helper methods for choosing channel colors.
 */
public class Colors {

  private static final double BLUE_TO_GREEN = 500.0;
  private static final double GREEN_TO_RED = 560.0;
  private static final double RANGE = 15.0;

  /**
   * Determine which color to use for the given channel.
   * If a channel color is already set, that will be returned.
   * If there is only one channel, it will be set to grey.
   * If a channel emission wavelength is set, that will be used
   * to determine the color; red, green, or blue based on the
   * wavelength in nm.
   * If filter metadata is populated, the first filter with
   * CutIn and/or CutOut wavelengths will be used to determine
   * the color.
   * Otherwise, the channel index is used to pick red, green, or blue.
   *
   * @param meta OME-XML object with acquisition metadata
   * @param series Bio-Formats series/OME-XML Image index
   * @param c channel index
   * @return channel color
   */
  public static Color getColor(OMEXMLMetadata meta, int series, int c) {
    Color color = meta.getChannelColor(series, c);
    if (color != null) {
      return color;
    }
    // default to grey for single channel data
    if (meta.getChannelCount(series) == 1) {
      return new Color(128, 128, 128, 255);
    }

    Length emWave = meta.getChannelEmissionWavelength(series, c);
    if (emWave != null) {
      return colorFromWavelength(emWave);
    }

    OMEXMLMetadataRoot root = (OMEXMLMetadataRoot) meta.getRoot();
    Image img = root.getImage(series);
    Channel channel = img.getPixels().getChannel(c);
    LightPath path = channel.getLightPath();
    FilterSet filterSet = channel.getLinkedFilterSet();

    Length valueFilter = null;

    // look at LightPath.EmissionFilterRef

    if (path != null) {
      List<Filter> emFilters = path.copyLinkedEmissionFilterList();
      valueFilter = handleFilters(emFilters, true);
    }

    // look at FilterSet.EmissionFilterRef

    if (valueFilter == null && filterSet != null) {
      List<Filter> emFilters = filterSet.copyLinkedEmissionFilterList();
      valueFilter = handleFilters(emFilters, true);
    }

    // look at LightSourceSettings.Laser.Wavelength and return if present
    // but EmissionFilterRef not present

    Length exWave = meta.getChannelExcitationWavelength(series, c);
    if (exWave != null) {
      return colorFromWavelength(exWave);
    }

    // look at LightPath.ExcitationFilterRef

    if (valueFilter == null && path != null) {
      List<Filter> exFilters = path.copyLinkedExcitationFilterList();
      valueFilter = handleFilters(exFilters, false);
    }

    // look at FilterSet.ExcitationFilterRef

    if (valueFilter == null && filterSet != null) {
      List<Filter> exFilters = filterSet.copyLinkedExcitationFilterList();
      valueFilter = handleFilters(exFilters, false);
    }

    // if any filter ref has a valid value, return the first one found

    if (valueFilter != null) {
      return colorFromWavelength(valueFilter);
    }

    // return red, green, or blue depending on the channel index
    int baseColor = 0xff;
    int shift = (3 - (c % 3)) * 8;
    baseColor = baseColor << shift;
    return new Color(baseColor + 0xff);
  }

  private static Length handleFilters(List<Filter> filters, boolean emission) {
    for (Filter f : filters) {
      Length v = getValueFromFilter(f, emission);
      if (v != null) {
        return v;
      }
    }
    return null;
  }

  private static Length getValueFromFilter(Filter filter, boolean emission) {
    if (filter == null) {
      return null;
    }
    TransmittanceRange transmittance = filter.getTransmittanceRange();
    if (transmittance == null) {
      return null;
    }
    Length cutIn = transmittance.getCutIn();

    if (emission) {
      if (cutIn == null) {
        return null;
      }
      return new Length(cutIn.value().doubleValue() + RANGE, cutIn.unit());
    }
    Length cutOut = transmittance.getCutOut();
    if (cutOut == null) {
      return null;
    }
    double cutOutValue = cutOut.value().doubleValue();
    if (cutIn == null || cutIn.value().doubleValue() == 0) {
      cutIn = new Length(cutOutValue - 2 * RANGE, cutOut.unit());
    }
    Unit<Length> unit = cutOut.unit();
    Length v =
      new Length(cutIn.value(unit).doubleValue() + cutOutValue / 2, unit);
    if (v.value().doubleValue() < 0) {
      return new Length(0, unit);
    }
    return v;
  }

  private static Color colorFromWavelength(Length wave) {
    if (wave == null) {
      return null;
    }

    double nm = wave.value(UNITS.NM).doubleValue();

    if (nm < BLUE_TO_GREEN) {
      return new Color(0, 0, 255, 255);
    }
    else if (nm < GREEN_TO_RED) {
      return new Color(0, 255, 0, 255);
    }
    return new Color(255, 0, 0, 255);
  }



}
