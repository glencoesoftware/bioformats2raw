/**
 * Copyright (c) 2023 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.bioformats2raw;

import java.util.ArrayList;

import loci.common.DataTools;
import loci.common.IniTable;

public class MetaxpressSite {
  public String id;
  public int x;
  public int y;
  public ArrayList<String> files = new ArrayList<String>();
  public ArrayList<String> channelNames = new ArrayList<String>();
  public double xpos;
  public double ypos;
  public int z = 1;
  public int c = 1;
  public int t = 1;

  public int minZ = Integer.MAX_VALUE;
  public int maxZ = 0;
  public int minT = Integer.MAX_VALUE;
  public int maxT = 0;

  /**
   * Create empty site.
   */
  public MetaxpressSite() {
  }

  /**
   * Populate a site from INI data.
   *
   * @param table INI table representing this site
   */
  public MetaxpressSite(IniTable table) {
    id = table.get("ID");
    x = Integer.parseInt(table.get("X"));
    y = Integer.parseInt(table.get("Y"));
    z = Integer.parseInt(table.get("Z"));
    c = Integer.parseInt(table.get("C"));
    t = Integer.parseInt(table.get("T"));
    xpos = DataTools.parseDouble(table.get("XPosition"));
    ypos = DataTools.parseDouble(table.get("YPosition"));

    // min/max Z and T values do not need to be stored
    // since they are only used for calculating z and t

    for (String key : table.keySet()) {
      boolean file = key.startsWith("File_");
      boolean channel = key.startsWith("ChannelName_");
      if (file || channel) {
        int index = Integer.parseInt(key.substring(key.indexOf("_") + 1));
        if (file) {
          while (index >= files.size()) {
            files.add(null);
          }
          files.set(index, table.get(key));
        }
        else if (channel) {
          while (index >= channelNames.size()) {
            channelNames.add(null);
          }
          channelNames.set(index, table.get(key));
        }
      }
    }
  }

  @Override
  public String toString() {
    return "id=" + id + ", x=" + x + ", y=" + y +
      ", z=" + z + ", c=" + c + ", t=" + t +
      ", files.size=" + files.size();
  }

  /**
   * @return an INI table representing this site
   */
  public IniTable getIniTable() {
    IniTable table = new IniTable();
    table.put(IniTable.HEADER_KEY, "Site " + id);
    table.put("ID", id);
    table.put("X", String.valueOf(x));
    table.put("Y", String.valueOf(y));
    table.put("Z", String.valueOf(z));
    table.put("C", String.valueOf(c));
    table.put("T", String.valueOf(t));
    table.put("XPosition", String.valueOf(xpos));
    table.put("YPosition", String.valueOf(ypos));
    for (int i=0; i<channelNames.size(); i++) {
      table.put("ChannelName_" + i, channelNames.get(i));
    }
    for (int i=0; i<files.size(); i++) {
      table.put("File_" + i, files.get(i));
    }
    return table;
  }

}
