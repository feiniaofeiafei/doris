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

import org.apache.doris.datasource.property.constants.PaimonProperties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class PaimonRestExternalCatalog extends PaimonExternalCatalog {
    private static final Logger LOG = LogManager.getLogger(PaimonRestExternalCatalog.class);

    public PaimonRestExternalCatalog(long catalogId, String name, String resource,
                                    Map<String, String> props, String comment) {
        super(catalogId, name, resource, props, comment);
    }

    @Override
    protected void initLocalObjectsImpl() {
        super.initLocalObjectsImpl();
        catalogType = PAIMON_REST;
        catalog = createCatalog();
    }

    /**
     *         catalogOptions.set("metastore", "rest");
     *         catalogOptions.set("warehouse", "new_dfl_paimon_catalog");
     *         catalogOptions.set("uri", "http://cn-beijing-vpc.dlf.aliyuncs.com");
     *         catalogOptions.set("token.provider", "dlf");
     *         catalogOptions.set("dlf.access-key-id", aliyunAk);
     *         catalogOptions.set("dlf.access-key-secret", aliyunSk);
     *
     * CREATE CATALOG paimon_dlf_test PROPERTIES (
     *     'type' = 'paimon',
     *     'paimon.catalog.type' = 'rest',
     *     'uri' = 'http://cn-beijing-vpc.dlf.aliyuncs.com',
     *     'warehouse' = 'new_dfl_paimon_catalog',
     *     'paimon.rest.token.provider' = 'dlf',
     *     'paimon.rest.dlf.access-key-id' = '<ak>',
     *     'paimon.rest.dlf.access-key-secret' = '<sk>'
     * );
     * @param properties
     * @param options
     */
    @Override
    protected void setPaimonCatalogOptions(Map<String, String> properties, Map<String, String> options) {
        options.put(PaimonProperties.PAIMON_CATALOG_TYPE, "rest");
        options.put("token.provider", "dlf");
        options.put("uri", properties.get(PaimonProperties.HIVE_METASTORE_URIS));
        for (Map.Entry<String, String> kv : properties.entrySet()) {
            if (kv.getKey().startsWith(PaimonProperties.PAIMON_REST_PREFIX)) {
                options.put(kv.getKey().substring(PaimonProperties.PAIMON_REST_PREFIX
                        .length()), kv.getValue());
            }
        }
    }
}
