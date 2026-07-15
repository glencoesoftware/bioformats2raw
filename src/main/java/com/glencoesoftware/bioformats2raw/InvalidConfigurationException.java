/**
 * Copyright (c) 2026 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.bioformats2raw;

/**
 * Indicates that a command line or programmatic converter setting is invalid.
 */
class InvalidConfigurationException extends IllegalArgumentException {

  InvalidConfigurationException(String message) {
    super(message);
  }

  InvalidConfigurationException(String message, Throwable cause) {
    super(message, cause);
  }
}
