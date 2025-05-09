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

package org.apache.doris.qe;

import org.apache.doris.common.Config;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.plugin.AuditPlugin;
import org.apache.doris.plugin.Plugin;
import org.apache.doris.plugin.PluginInfo.PluginType;
import org.apache.doris.plugin.PluginMgr;

import com.google.common.base.Strings;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Class for processing all audit events.
 * It will receive audit events and handle them to all AUDIT type plugins.
 */
public class AuditEventProcessor {
    private static final Logger LOG = LogManager.getLogger(AuditEventProcessor.class);
    private static final long UPDATE_PLUGIN_INTERVAL_MS = 60 * 1000; // 1min

    private PluginMgr pluginMgr;

    private List<Plugin> auditPlugins;
    private long lastUpdateTime = 0;

    private BlockingQueue<AuditEvent> eventQueue = Queues.newLinkedBlockingDeque();
    private Thread workerThread;

    private volatile boolean isStopped = false;

    private Set<String> skipAuditUsers = Sets.newHashSet();

    public AuditEventProcessor(PluginMgr pluginMgr) {
        this.pluginMgr = pluginMgr;
    }

    public void start() {
        initSkipAuditUsers();
        workerThread = new Thread(new Worker(), "AuditEventProcessor");
        workerThread.setDaemon(true);
        workerThread.start();
    }

    private void initSkipAuditUsers() {
        if (Strings.isNullOrEmpty(Config.skip_audit_user_list)) {
            return;
        }
        String[] users = Config.skip_audit_user_list.replaceAll(" ", "").split(",");
        for (String user : users) {
            skipAuditUsers.add(user);
        }
        LOG.info("skip audit users: {}", skipAuditUsers);
    }

    public void stop() {
        isStopped = true;
        if (workerThread != null) {
            try {
                workerThread.join();
            } catch (InterruptedException e) {
                LOG.warn("join worker join failed.", e);
            }
        }
    }

    public boolean handleAuditEvent(AuditEvent auditEvent) {
        if (skipAuditUsers.contains(auditEvent.user)) {
            // return true to ignore this event
            return true;
        }
        boolean isAddSucc = true;
        try {
            if (eventQueue.size() >= Config.audit_event_log_queue_size) {
                isAddSucc = false;
                LOG.warn("the audit event queue is full with size {}, discard the audit event: {}",
                        eventQueue.size(), auditEvent.queryId);
            } else {
                eventQueue.add(auditEvent);
            }
        } catch (Exception e) {
            isAddSucc = false;
            LOG.warn("encounter exception when handle audit event {}, discard the event",
                    auditEvent.queryId, e);
        }
        return isAddSucc;
    }

    public class Worker implements Runnable {
        @Override
        public void run() {
            AuditEvent auditEvent;
            while (!isStopped) {
                // update audit plugin list every UPDATE_PLUGIN_INTERVAL_MS.
                // because some plugins may be installed or uninstalled at runtime.
                if (auditPlugins == null || System.currentTimeMillis() - lastUpdateTime > UPDATE_PLUGIN_INTERVAL_MS) {
                    auditPlugins = pluginMgr.getActivePluginList(PluginType.AUDIT);
                    lastUpdateTime = System.currentTimeMillis();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("update audit plugins. num: {}", auditPlugins.size());
                    }
                }

                try {
                    auditEvent = eventQueue.poll(5, TimeUnit.SECONDS);
                    if (auditEvent == null) {
                        continue;
                    }
                } catch (InterruptedException e) {
                    LOG.warn("encounter exception when getting audit event from queue, ignore", e);
                    continue;
                }

                try {
                    for (Plugin plugin : auditPlugins) {
                        if (((AuditPlugin) plugin).eventFilter(auditEvent.type)) {
                            ((AuditPlugin) plugin).exec(auditEvent);
                        }
                    }
                } catch (Exception e) {
                    LOG.warn("encounter exception when processing audit events. ignore", e);
                }
            }
        }

    }
}
