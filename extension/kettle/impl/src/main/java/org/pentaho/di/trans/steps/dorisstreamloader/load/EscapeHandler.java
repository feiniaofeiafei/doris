// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.pentaho.di.trans.steps.dorisstreamloader.load;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.pentaho.di.trans.steps.dorisstreamloader.load.LoadConstants.FIELD_DELIMITER_DEFAULT;
import static org.pentaho.di.trans.steps.dorisstreamloader.load.LoadConstants.FIELD_DELIMITER_KEY;
import static org.pentaho.di.trans.steps.dorisstreamloader.load.LoadConstants.LINE_DELIMITER_DEFAULT;
import static org.pentaho.di.trans.steps.dorisstreamloader.load.LoadConstants.LINE_DELIMITER_KEY;

/** Handler for escape in properties. */
public class EscapeHandler {
    public static final String ESCAPE_DELIMITERS_FLAGS = "\\x";
    public static final Pattern ESCAPE_PATTERN = Pattern.compile("\\\\x([0-9|a-f|A-F]{2})");

    public static String escapeString(String source) {
        if (source.contains(ESCAPE_DELIMITERS_FLAGS)) {
            Matcher m = ESCAPE_PATTERN.matcher(source);
            StringBuffer buf = new StringBuffer();
            while (m.find()) {
                m.appendReplacement(
                        buf, String.format("%s", (char) Integer.parseInt(m.group(1), 16)));
            }
            m.appendTail(buf);
            return buf.toString();
        }
        return source;
    }

    public static void handle(Properties properties) {
        String fieldDelimiter =
                properties.getProperty(FIELD_DELIMITER_KEY, FIELD_DELIMITER_DEFAULT);
        if (fieldDelimiter.contains(ESCAPE_DELIMITERS_FLAGS)) {
            properties.setProperty(FIELD_DELIMITER_KEY, escapeString(fieldDelimiter));
        }
        String lineDelimiter = properties.getProperty(LINE_DELIMITER_KEY, LINE_DELIMITER_DEFAULT);
        if (lineDelimiter.contains(ESCAPE_DELIMITERS_FLAGS)) {
            properties.setProperty(LINE_DELIMITER_KEY, escapeString(lineDelimiter));
        }
    }

    public static void handleEscape(Properties properties) {
        handle(properties);
    }
}
