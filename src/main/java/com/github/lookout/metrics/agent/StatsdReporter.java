/**
 * Copyright (C) 2012-2013 Sean Laurent
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
/**
 Edited by Jon Haddad at SHIFT to work with
 */

package com.github.lookout.metrics.agent;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.github.lookout.metrics.agent.generators.CassandraJMXGenerator;
import com.github.lookout.metrics.agent.generators.MetricGenerator;
import com.timgroup.statsd.StatsDClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class StatsdReporter extends ScheduledReporter {

    private static final Logger LOG = LoggerFactory.getLogger(StatsdReporter.class);

    private final StatsDClient statsd;
    private final MetricRegistry metricRegistry;

    private boolean reportedStartup = false;
    private final HostPortInterval hostPortInterval;

    private final Set<MetricGenerator> generators = new HashSet<>();

    public StatsdReporter(MetricRegistry metricRegistry, final HostPortInterval hostPortInterval, final StatsDClient statsd) {
        super(metricRegistry, "statsd", MetricFilter.ALL, TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
        this.metricRegistry = metricRegistry;
        this.hostPortInterval = hostPortInterval;
        this.statsd = statsd;

        // All are gauges so no handling for timers and stuff
        // all of these are Longs
        this.metricRegistry.registerAll(new GarbageCollectorMetricSet());
        // all of these are Longs, unless they are RatioGauges, then they are doubles
        this.metricRegistry.registerAll(new MemoryUsageGaugeSet());

        // This really should be done with an injection framework, but that's too heavy for this
        generators.add(new CassandraJMXGenerator());
        generators.add(new CassandraExposedJMX());
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
        if (!reportedStartup || LOG.isDebugEnabled()) {
            LOG.info("Statsd reporting to {}", hostPortInterval);
            reportedStartup = true;
        }
        for (MetricGenerator generator : generators) {
            try {
                generator.generate(statsd);
            } catch (RuntimeException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Error writing to statsd", e);
                } else {
                    LOG.warn("Error writing to statsd: {}", e.getMessage());
                }
            }
        }

        // send all metrics to statsd
        for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
            Gauge value = entry.getValue();
            if (value instanceof RatioGauge) {
                statsd.gauge(entry.getKey(), (Double) value.getValue());
            }
            else {
                statsd.gauge(entry.getKey(), (Long) value.getValue());
            }
        }
    }
}
