package com.github.lookout.metrics.agent;

import com.timgroup.statsd.StatsDClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.util.Set;

public class CassandraExposedJMX implements com.github.lookout.metrics.agent.generators.MetricGenerator {
	private static final Logger LOG = LoggerFactory.getLogger(CassandraExposedJMX.class);
	private final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

	@Override
	public void generate(StatsDClient statsDClient) {
		// https://www.datadoghq.com/blog/how-to-collect-cassandra-metrics/
		final Set<ObjectInstance> mBeans;
		try {
			mBeans = mBeanServer.queryMBeans(new ObjectName("org.apache.cassandra.metrics:type=ColumnFamily,keyspace=*,scope=*,name=*"), null);
			for (ObjectInstance mBean : mBeans) {
				ObjectName objectName = mBean.getObjectName();
				String keyspace = objectName.getKeyProperty("keyspace");
				String table = objectName.getKeyProperty("scope");
				String name = objectName.getKeyProperty("name");

				String metricName = keyspace + "." + table + "." + name;
				try {
					String className = mBean.getClassName();
					if (className.contains("JmxGauge")) {
						Object value = mBeanServer.getAttribute(objectName, "Value");
						gaugeAndLog(statsDClient, metricName, value);
					}
					else if (className.contains("JmxCounter")) {
						Object count = mBeanServer.getAttribute(objectName, "Count");
						countAndLog(statsDClient, metricName, count);
					}
					else if (className.contains("JmxTimer")) {
						Object count = mBeanServer.getAttribute(objectName, "Count");
						Object mean = mBeanServer.getAttribute(objectName, "Mean");
						timeAndLog(statsDClient, metricName, count, mean);
					}
					else if (className.contains("JmxHistogram")) {
						// Skip histograms
					}
					else {
						// LOG.warn("unknown class {}", className);
					}
				}
				catch (MBeanException | AttributeNotFoundException | ReflectionException | InstanceNotFoundException e) {
					e.printStackTrace();
				}
			}
		}
		catch (MalformedObjectNameException e) {
		}

	}

	private void gaugeAndLog(StatsDClient statsDClient, String gaugeName, Object value) {
		if (value instanceof Double) {
			statsDClient.gauge(gaugeName, (Double) value);
		}
		else if (value instanceof Long) {
			statsDClient.gauge(gaugeName, (Long) value);
		}
		else if (value instanceof Integer) {
			statsDClient.gauge(gaugeName, (Integer) value);
		}
		else if (value instanceof long[]) {
			// ignore, some kind of histogram
		}
		else {
			// LOG.warn("unknown type for {}", gaugeName);
		}
		// LOG.debug("Reporting {} as {}", gaugeName, value);
	}

	private void countAndLog(StatsDClient statsDClient, String gaugeName, Object count) {
		statsDClient.count(gaugeName, (Long) count);
		// LOG.debug("Reporting {} as {}", gaugeName, count);
	}

	private void timeAndLog(StatsDClient statsDClient, String gaugeName, Object count, Object mean) {
		long longCount = (long) count;
		double doubleMean = (double) mean;
		long time = (long) (doubleMean * longCount);
		statsDClient.recordExecutionTime(gaugeName, time, longCount);
		// LOG.debug("Reporting {}: mean {} count {}", gaugeName, mean, count);
	}
}
