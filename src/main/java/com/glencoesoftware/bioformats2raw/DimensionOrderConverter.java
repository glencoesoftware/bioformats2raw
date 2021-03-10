/**
 * Copyright (c) 2021 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */

package com.glencoesoftware.bioformats2raw;

import ome.xml.model.enums.DimensionOrder;
import picocli.CommandLine.ITypeConverter;

/**
 * Convert a string to a DimensionOrder.
 */
public class DimensionOrderConverter implements ITypeConverter<DimensionOrder> {
  @Override
  public DimensionOrder convert(String value) throws Exception {
    if (value == null || value.equalsIgnoreCase("original")) {
      return null;
    }
    return DimensionOrder.fromString(value);
  }
}
