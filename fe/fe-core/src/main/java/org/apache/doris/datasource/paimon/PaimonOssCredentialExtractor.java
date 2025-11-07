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

package org.apache.doris.datasource.paimon;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.rest.RESTToken;
import org.apache.paimon.rest.RESTTokenFileIO;
import org.apache.paimon.table.Table;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Default credential extractor for S3 credentials.
 */
public class PaimonOssCredentialExtractor {
    private static final Logger LOG = LogManager.getLogger(PaimonOssCredentialExtractor.class);
    // AWS credential property keys for backend
    private static final String BACKEND_AWS_ACCESS_KEY = "AWS_ACCESS_KEY";
    private static final String BACKEND_AWS_SECRET_KEY = "AWS_SECRET_KEY";
    private static final String BACKEND_AWS_TOKEN = "AWS_TOKEN";
    private static final String BACKEND_AWS_ENDPOINT = "AWS_ENDPOINT";
    private static final String BACKEND_AWS_REGION = "AWS_REGION";
    // oss credential property keys from DLF Paimon FileIO
    private static final String OSS_ACCESS_KEY = "fs.oss.accessKeyId";
    private static final String OSS_SECRET_KEY = "fs.oss.accessKeySecret";
    private static final String OSS_SECURITY_TOKEN = "fs.oss.securityToken";
    private static final String OSS_ENDPOINT = "fs.oss.endpoint";

    private static final Pattern STANDARD_ENDPOINT_PATTERN = Pattern
            .compile("^(?:https?://)?(?:s3\\.)?oss-([a-z0-9-]+?)(?:-internal)?\\.aliyuncs\\.com$");
    public static final Set<Pattern> ENDPOINT_PATTERN = ImmutableSet.of(STANDARD_ENDPOINT_PATTERN,
            Pattern.compile("(?:https?://)?([a-z]{2}-[a-z0-9-]+)\\.oss-dls\\.aliyuncs\\.com"),
            Pattern.compile("^(?:https?://)?dlf(?:-vpc)?\\.([a-z0-9-]+)\\.aliyuncs\\.com(?:/.*)?$"),
            Pattern.compile("^(?:https?://)?datalake(?:-vpc)?\\.([a-z0-9-]+)\\.aliyuncs\\.com(?:/.*)?$"));

    public static Map<String, String> extractVendedCredentialsFromTable(Table table) {
        if (table == null || table.fileIO() == null) {
            return Maps.newHashMap();
        }

        if (!(table.fileIO() instanceof RESTTokenFileIO)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("File IO of table {} is not RESTTokenFileIO, cannot extract vended credentials: {}",
                        table.name(), table.fileIO().getClass().getName());
            }
            return Maps.newHashMap();
        }

        RESTTokenFileIO restTokenFileIO = (RESTTokenFileIO) table.fileIO();
        RESTToken restToken = restTokenFileIO.validToken();
        Map<String, String> tokens = restToken.token();
        return extractCredentials(tokens);
    }

    private static Map<String, String> extractCredentials(Map<String, String> properties) {
        Map<String, String> credentials = Maps.newHashMap();

        if (properties == null || properties.isEmpty()) {
            return credentials;
        }

        // Extract AWS credentials from Iceberg S3 FileIO format
        if (properties.containsKey(OSS_ACCESS_KEY)) {
            credentials.put(BACKEND_AWS_ACCESS_KEY, properties.get(OSS_ACCESS_KEY));
        }
        if (properties.containsKey(OSS_SECRET_KEY)) {
            credentials.put(BACKEND_AWS_SECRET_KEY, properties.get(OSS_SECRET_KEY));
        }
        if (properties.containsKey(OSS_SECURITY_TOKEN)) {
            credentials.put(BACKEND_AWS_TOKEN, properties.get(OSS_SECURITY_TOKEN));
        }
        if (properties.containsKey(OSS_ENDPOINT)) {
            credentials.put(BACKEND_AWS_ENDPOINT, properties.get(OSS_ENDPOINT));
            Optional<String> regionOpt = extractRegion(properties.get(OSS_ENDPOINT));
            if (regionOpt.isPresent()) {
                credentials.put(BACKEND_AWS_REGION, regionOpt.get());
            }
        }

        return credentials;
    }

    public static Optional<String> extractRegion(String endpoint) {
        for (Pattern pattern : ENDPOINT_PATTERN) {
            Matcher matcher = pattern.matcher(endpoint.toLowerCase());
            if (matcher.matches()) {
                // Check all possible groups for region (group 1, 2, or 3)
                for (int i = 1; i <= matcher.groupCount(); i++) {
                    String group = matcher.group(i);
                    if (StringUtils.isNotBlank(group)) {
                        return Optional.of(group);
                    }
                }
            }
        }
        return Optional.empty();
    }
}
