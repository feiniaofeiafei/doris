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

package org.apache.doris.metric;

import org.apache.doris.catalog.CloudTabletStatMgr;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.monitor.jvm.JvmStats;
import org.apache.doris.monitor.jvm.JvmStats.GarbageCollector;
import org.apache.doris.monitor.jvm.JvmStats.MemoryPool;
import org.apache.doris.monitor.jvm.JvmStats.Threads;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.google.common.base.Joiner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * Like this:
 * # HELP doris_fe_job_load_broker_cost_ms doris_fe_job_load_broker_cost_ms
 * # TYPE doris_fe_job_load_broker_cost_ms gauge
 * doris_fe_job{job="load", type="mini", state="pending"} 0
 */
public class PrometheusMetricVisitor extends MetricVisitor {

    private static final Logger logger = LogManager.getLogger(PrometheusMetricVisitor.class);
    // jvm
    private static final String JVM_HEAP_SIZE_BYTES = "jvm_heap_size_bytes";
    private static final String JVM_NON_HEAP_SIZE_BYTES = "jvm_non_heap_size_bytes";
    private static final String JVM_YOUNG_SIZE_BYTES = "jvm_young_size_bytes";
    private static final String JVM_OLD_SIZE_BYTES = "jvm_old_size_bytes";
    private static final String JVM_THREAD = "jvm_thread";

    private static final String JVM_GC = "jvm_gc";

    private static final String HELP = "# HELP ";
    private static final String TYPE = "# TYPE ";

    private Set<String> metricNames = new HashSet();

    public PrometheusMetricVisitor() {
        super();
    }

    @Override
    public void visitJvm(JvmStats jvmStats) {
        // heap
        sb.append(Joiner.on(" ").join(HELP, JVM_HEAP_SIZE_BYTES, "jvm heap stat\n"));
        sb.append(Joiner.on(" ").join(TYPE, JVM_HEAP_SIZE_BYTES, "gauge\n"));
        sb.append(JVM_HEAP_SIZE_BYTES).append("{type=\"max\"} ").append(jvmStats.getMem().getHeapMax().getBytes())
                .append("\n");
        sb.append(JVM_HEAP_SIZE_BYTES).append("{type=\"committed\"} ")
                .append(jvmStats.getMem().getHeapCommitted().getBytes()).append("\n");
        sb.append(JVM_HEAP_SIZE_BYTES).append("{type=\"used\"} ").append(jvmStats.getMem().getHeapUsed().getBytes())
                .append("\n");
        // non heap
        sb.append(Joiner.on(" ").join(HELP, JVM_NON_HEAP_SIZE_BYTES, "jvm non heap stat\n"));
        sb.append(Joiner.on(" ").join(TYPE, JVM_NON_HEAP_SIZE_BYTES, "gauge\n"));
        sb.append(JVM_NON_HEAP_SIZE_BYTES).append("{type=\"committed\"} ")
                .append(jvmStats.getMem().getNonHeapCommitted().getBytes()).append("\n");
        sb.append(JVM_NON_HEAP_SIZE_BYTES).append("{type=\"used\"} ")
                .append(jvmStats.getMem().getNonHeapUsed().getBytes()).append("\n");

        // mem pool
        Iterator<MemoryPool> memIter = jvmStats.getMem().iterator();
        while (memIter.hasNext()) {
            MemoryPool memPool = memIter.next();
            if (memPool.getName().equalsIgnoreCase("young")) {
                sb.append(Joiner.on(" ").join(HELP, JVM_YOUNG_SIZE_BYTES, "jvm young mem pool stat\n"));
                sb.append(Joiner.on(" ").join(TYPE, JVM_YOUNG_SIZE_BYTES, "gauge\n"));
                sb.append(JVM_YOUNG_SIZE_BYTES).append("{type=\"used\"} ")
                        .append(memPool.getUsed().getBytes()).append("\n");
                sb.append(JVM_YOUNG_SIZE_BYTES).append("{type=\"peak_used\"} ")
                        .append(memPool.getPeakUsed().getBytes()).append("\n");
                sb.append(JVM_YOUNG_SIZE_BYTES).append("{type=\"max\"} ")
                        .append(memPool.getMax().getBytes()).append("\n");
            } else if (memPool.getName().equalsIgnoreCase("old")) {
                sb.append(Joiner.on(" ").join(HELP, JVM_OLD_SIZE_BYTES, "jvm old mem pool stat\n"));
                sb.append(Joiner.on(" ").join(TYPE, JVM_OLD_SIZE_BYTES, "gauge\n"));
                sb.append(JVM_OLD_SIZE_BYTES).append("{type=\"used\"} ")
                        .append(memPool.getUsed().getBytes()).append("\n");
                sb.append(JVM_OLD_SIZE_BYTES).append("{type=\"peak_used\"} ")
                        .append(memPool.getPeakUsed().getBytes()).append("\n");
                sb.append(JVM_OLD_SIZE_BYTES).append("{type=\"max\"} "
                ).append(memPool.getMax().getBytes()).append("\n");
            }
        }

        // gc
        sb.append(Joiner.on(" ").join(HELP, JVM_GC, "jvm gc stat\n"));
        sb.append(Joiner.on(" ").join(TYPE, JVM_GC, "gauge\n"));
        for (GarbageCollector gc : jvmStats.getGc()) {
            sb.append(JVM_GC).append("{");
            sb.append("name=\"").append(gc.getName()).append(" Count").append("\", ").append("type=\"count\"} ")
                    .append(gc.getCollectionCount()).append("\n");
            sb.append(JVM_GC).append("{");
            sb.append("name=\"").append(gc.getName()).append(" Time").append("\", ").append("type=\"time\"} ")
                    .append(gc.getCollectionTime().getMillis()).append("\n");
        }

        // threads
        Threads threads = jvmStats.getThreads();
        sb.append(Joiner.on(" ").join(HELP, JVM_THREAD, "jvm thread stat\n"));
        sb.append(Joiner.on(" ").join(TYPE, JVM_THREAD, "gauge\n"));
        sb.append(JVM_THREAD).append("{type=\"count\"} ")
                .append(threads.getCount()).append("\n");
        sb.append(JVM_THREAD).append("{type=\"peak_count\"} ")
                .append(threads.getPeakCount()).append("\n");
        sb.append(JVM_THREAD).append("{type=\"new_count\"} ")
                .append(threads.getThreadsNewCount()).append("\n");
        sb.append(JVM_THREAD).append("{type=\"runnable_count\"} ")
                .append(threads.getThreadsRunnableCount()).append("\n");
        sb.append(JVM_THREAD).append("{type=\"blocked_count\"} ")
                .append(threads.getThreadsBlockedCount()).append("\n");
        sb.append(JVM_THREAD).append("{type=\"waiting_count\"} ")
                .append(threads.getThreadsWaitingCount()).append("\n");
        sb.append(JVM_THREAD).append("{type=\"timed_waiting_count\"} ")
                .append(threads.getThreadsTimedWaitingCount()).append("\n");
        sb.append(JVM_THREAD).append("{type=\"terminated_count\"} ")
                .append(threads.getThreadsTerminatedCount()).append("\n");
        return;
    }

    @Override
    public void visit(String prefix, @SuppressWarnings("rawtypes") Metric metric) {
        // title
        final String fullName = prefix + metric.getName();
        if (!metricNames.contains(fullName)) {
            sb.append(HELP).append(fullName).append(" ").append(metric.getDescription()).append("\n");
            sb.append(TYPE).append(fullName).append(" ").append(metric.getType().name().toLowerCase()).append("\n");
            metricNames.add(fullName);
        }
        sb.append(fullName);

        // name
        @SuppressWarnings("unchecked")
        List<MetricLabel> labels = metric.getLabels();
        if (!labels.isEmpty()) {
            sb.append("{");
            List<String> labelStrs = labels.stream().map(l -> l.getKey() + "=\"" + l.getValue()
                    + "\"").collect(Collectors.toList());
            sb.append(Joiner.on(", ").join(labelStrs));
            sb.append("}");
        }

        // value
        sb.append(" ").append(metric.getValue().toString()).append("\n");
    }

    @Override
    public void visitHistogram(String prefix, String name, Histogram histogram) {
        // part.part.part.k1=v1.k2=v2
        List<String> names = new ArrayList<>();
        List<String> tags = new ArrayList<>();
        for (String part : name.split("\\.")) {
            String[] kv = part.split("=");
            if (kv.length == 1) {
                names.add(kv[0]);
            } else if (kv.length == 2) {
                tags.add(String.format("%s=\"%s\"", kv[0], kv[1]));
            }
        }
        final String fullName = prefix + String.join("_", names);
        final String fullTag = String.join(",", tags);
        // we should define metric name only once
        if (!metricNames.contains(fullName)) {
            sb.append(HELP).append(fullName).append(" ").append("\n");
            sb.append(TYPE).append(fullName).append(" ").append("summary\n");
            metricNames.add(fullName);
        }
        String delimiter = tags.isEmpty() ? "" : ",";
        Snapshot snapshot = histogram.getSnapshot();
        sb.append(fullName).append("{quantile=\"0.75\"").append(delimiter).append(fullTag).append("} ")
            .append(snapshot.get75thPercentile()).append("\n");
        sb.append(fullName).append("{quantile=\"0.95\"").append(delimiter).append(fullTag).append("} ")
            .append(snapshot.get95thPercentile()).append("\n");
        sb.append(fullName).append("{quantile=\"0.98\"").append(delimiter).append(fullTag).append("} ")
            .append(snapshot.get98thPercentile()).append("\n");
        sb.append(fullName).append("{quantile=\"0.99\"").append(delimiter).append(fullTag).append("} ")
            .append(snapshot.get99thPercentile()).append("\n");
        sb.append(fullName).append("{quantile=\"0.999\"").append(delimiter).append(fullTag).append("} ")
            .append(snapshot.get999thPercentile()).append("\n");
        sb.append(fullName).append("_sum {").append(fullTag).append("} ")
                .append(histogram.getCount() * snapshot.getMean()).append("\n");
        sb.append(fullName).append("_count {").append(fullTag).append("} ")
                .append(histogram.getCount()).append("\n");
    }

    @Override
    public void visitNodeInfo() {
        final String NODE_INFO = "node_info";
        sb.append(Joiner.on(" ").join(TYPE, NODE_INFO, "gauge\n"));
        sb.append(NODE_INFO).append("{type=\"fe_node_num\", state=\"total\"} ")
                .append(Env.getCurrentEnv().getFrontends(null).size()).append("\n");
        sb.append(NODE_INFO).append("{type=\"be_node_num\", state=\"total\"} ")
                .append(Env.getCurrentSystemInfo().getAllBackendIds(false).size()).append("\n");
        sb.append(NODE_INFO).append("{type=\"be_node_num\", state=\"alive\"} ")
                .append(Env.getCurrentSystemInfo().getAllBackendIds(true).size()).append("\n");
        sb.append(NODE_INFO).append("{type=\"be_node_num\", state=\"decommissioned\"} ")
                .append(Env.getCurrentSystemInfo().getDecommissionedBackendIds().size()).append("\n");
        sb.append(NODE_INFO).append("{type=\"broker_node_num\", state=\"dead\"} ").append(
                Env.getCurrentEnv().getBrokerMgr().getAllBrokers()
                        .stream().filter(b -> !b.isAlive).count()).append("\n");

        // only master FE has this metrics, to help the Grafana knows who is the master
        if (Env.getCurrentEnv().isMaster()) {
            sb.append(NODE_INFO).append("{type=\"is_master\"} ").append(1).append("\n");
        }
        return;
    }

    @Override
    public void visitCloudTableStats() {
        if (Config.isNotCloudMode() || Env.getCurrentEnv().getTabletStatMgr() == null) {
            return;
        }

        CloudTabletStatMgr tabletStatMgr = (CloudTabletStatMgr) Env.getCurrentEnv().getTabletStatMgr();

        StringBuilder dataSizeBuilder = new StringBuilder();
        StringBuilder rowsetCountBuilder = new StringBuilder();
        StringBuilder segmentCountBuilder = new StringBuilder();
        StringBuilder tableRowCountBuilder = new StringBuilder();

        Collection<OlapTable.Statistics> values = tabletStatMgr.getCloudTableStatsMap().values();
        // calc totalTableSize
        long totalTableSize = 0;
        for (OlapTable.Statistics stats : values) {
            totalTableSize += stats.getDataSize();
        }
        // output top N metrics
        if (values.size() > Config.prom_output_table_metrics_limit) {
            // only copy elements if number of tables > prom_output_table_metrics_limit
            PriorityQueue<OlapTable.Statistics> topStats = new PriorityQueue<>(
                    Config.prom_output_table_metrics_limit,
                    Comparator.comparingLong(OlapTable.Statistics::getDataSize));
            for (OlapTable.Statistics stats : values) {
                if (topStats.size() < Config.prom_output_table_metrics_limit) {
                    topStats.offer(stats);
                } else if (!topStats.isEmpty()
                        && stats.getDataSize() > topStats.peek().getDataSize()) {
                    topStats.poll();
                    topStats.offer(stats);
                }
            }
            values = topStats;
        }
        for (OlapTable.Statistics stats : values) {

            dataSizeBuilder.append("doris_fe_table_data_size{db_name=\"");
            dataSizeBuilder.append(stats.getDbName());
            dataSizeBuilder.append("\", table_name=\"");
            dataSizeBuilder.append(stats.getTableName());
            dataSizeBuilder.append("\"} ");
            dataSizeBuilder.append(stats.getDataSize());
            dataSizeBuilder.append("\n");

            rowsetCountBuilder.append("doris_fe_table_rowset_count{db_name=\"");
            rowsetCountBuilder.append(stats.getDbName());
            rowsetCountBuilder.append("\", table_name=\"");
            rowsetCountBuilder.append(stats.getTableName());
            rowsetCountBuilder.append("\"} ");
            rowsetCountBuilder.append(stats.getRowsetCount());
            rowsetCountBuilder.append("\n");

            segmentCountBuilder.append("doris_fe_table_segment_count{db_name=\"");
            segmentCountBuilder.append(stats.getDbName());
            segmentCountBuilder.append("\", table_name=\"");
            segmentCountBuilder.append(stats.getTableName());
            segmentCountBuilder.append("\"} ");
            segmentCountBuilder.append(stats.getSegmentCount());
            segmentCountBuilder.append("\n");

            tableRowCountBuilder.append("doris_fe_table_row_count{db_name=\"");
            tableRowCountBuilder.append(stats.getDbName());
            tableRowCountBuilder.append("\", table_name=\"");
            tableRowCountBuilder.append(stats.getTableName());
            tableRowCountBuilder.append("\"} ");
            tableRowCountBuilder.append(stats.getRowCount());
            tableRowCountBuilder.append("\n");
        }

        if (dataSizeBuilder.length() > 0) {
            sb.append(Joiner.on(" ").join(HELP, "doris_fe_table_data_size", "table data size\n"));
            sb.append(Joiner.on(" ").join(TYPE, "doris_fe_table_data_size", "gauge\n"));
            sb.append(dataSizeBuilder.toString());
        }

        if (segmentCountBuilder.length() > 0) {
            sb.append(Joiner.on(" ").join(HELP, "doris_fe_table_rowset_count", "table rowset count\n"));
            sb.append(Joiner.on(" ").join(TYPE, "doris_fe_table_rowset_count", "gauge\n"));
            sb.append(rowsetCountBuilder.toString());
        }

        if (segmentCountBuilder.length() > 0) {
            sb.append(Joiner.on(" ").join(HELP, "doris_fe_table_segment_count", "table segment count\n"));
            sb.append(Joiner.on(" ").join(TYPE, "doris_fe_table_segment_count", "gauge\n"));
            sb.append(segmentCountBuilder.toString());
        }

        if (tableRowCountBuilder.length() > 0) {
            sb.append(Joiner.on(" ").join(HELP, "doris_fe_table_row_count", "table row count\n"));
            sb.append(Joiner.on(" ").join(TYPE, "doris_fe_table_row_count", "gauge\n"));
            sb.append(tableRowCountBuilder.toString());
        }

        // total table size
        sb.append(Joiner.on(" ").join(HELP, "doris_fe_table_data_size_total", "total table data size\n"));
        sb.append(Joiner.on(" ").join(TYPE, "doris_fe_table_data_size_total", "gauge\n"));
        sb.append("doris_fe_table_data_size_total ");
        sb.append(totalTableSize);
        sb.append("\n");

        // total recycle bin size
        long totalRecycleSize = 0;
        for (Map.Entry<Long, Pair<Long, Long>> entry : Env.getCurrentRecycleBin().getDbToRecycleSize().entrySet()) {
            totalRecycleSize += entry.getValue().first;
        }
        sb.append(Joiner.on(" ").join(HELP, "doris_fe_recycle_data_size_total", "total recycle bin data size\n"));
        sb.append(Joiner.on(" ").join(TYPE, "doris_fe_recycle_data_size_total", "gauge\n"));
        sb.append("doris_fe_recycle_data_size_total ");
        sb.append(totalRecycleSize);
        sb.append("\n");
        return;
    }

    @Override
    public void visitWorkloadGroup() {
        StringBuilder tmpSb = new StringBuilder();
        try {
            String counterTitle = "doris_workload_group_query_detail";
            tmpSb.append("# HELP " + counterTitle + "\n");
            tmpSb.append("# TYPE " + counterTitle + " counter\n");
            Map<String, List<String>> workloadGroupMap = Env.getCurrentEnv().getWorkloadGroupMgr()
                    .getWorkloadGroupQueryDetail();
            for (Map.Entry<String, List<String>> entry : workloadGroupMap.entrySet()) {
                String name = entry.getKey();
                List<String> valList = entry.getValue();
                tmpSb.append(String.format("%s{name=\"%s\", type=\"%s\"} %s\n", counterTitle, name, "running_query_num",
                        valList.get(0)));
                tmpSb.append(String.format("%s{name=\"%s\", type=\"%s\"} %s\n", counterTitle, name, "waiting_query_num",
                        valList.get(1)));
            }
            sb.append(tmpSb);
        } catch (Exception e) {
            logger.warn("error happends when get workload group query detail ", e);
        }
    }
}
