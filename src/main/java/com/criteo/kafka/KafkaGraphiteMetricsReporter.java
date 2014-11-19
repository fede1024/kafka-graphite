package com.criteo.kafka;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.yammer.metrics.Metrics;
import kafka.metrics.KafkaMetricsConfig;
import kafka.metrics.KafkaMetricsReporter;
import kafka.utils.VerifiableProperties;

import kafka.metrics.KafkaMetricsConfig

import com.codahale.metrics.MetricFilter;

import org.apache.log4j.*;

import javax.net.SocketFactory;

import com.yammer.metrics.scala.

public class KafkaGraphiteMetricsReporter implements KafkaMetricsReporter,
	KafkaGraphiteMetricsReporterMBean {

	static Logger LOG = Logger.getLogger(KafkaGraphiteMetricsReporter.class);
	static String GRAPHITE_DEFAULT_HOST = "localhost";
	static int GRAPHITE_DEFAULT_PORT = 2003;
	static String GRAPHITE_DEFAULT_PREFIX = "kafka";

	String graphiteHost = GRAPHITE_DEFAULT_HOST;
	int graphitePort = GRAPHITE_DEFAULT_PORT;
	String graphiteGroupPrefix = GRAPHITE_DEFAULT_PREFIX;

	GraphiteReporter reporter = null;
    MetricFilter predicate = MetricFilter.ALL;

	boolean initialized = false;
	boolean running = false;

	@Override
	public String getMBeanName() {
		return "kafka:type=com.criteo.kafka.KafkaGraphiteMetricsReporter";
	}

	@Override
	public synchronized void startReporter(long pollingPeriodSecs) {
		if (initialized && !running) {
			reporter.start(pollingPeriodSecs, TimeUnit.SECONDS);
			running = true;
			LOG.info(String.format("Started Kafka Graphite metrics reporter with polling period %d seconds", pollingPeriodSecs));
		}
	}

	@Override
	public synchronized void stopReporter() {
		if (initialized && running) {
			reporter.shutdown();
			running = false;
			LOG.info("Stopped Kafka Graphite metrics reporter");
			reporter = createReporter(graphiteHost, graphitePort, graphiteGroupPrefix, predicate);
		}
	}

	@Override
	public synchronized void init(VerifiableProperties props) {
		if (!initialized) {
			KafkaMetricsConfig metricsConfig = new KafkaMetricsConfig(props);
            graphiteHost = props.getString("kafka.graphite.metrics.host", GRAPHITE_DEFAULT_HOST);
            graphitePort = props.getInt("kafka.graphite.metrics.port", GRAPHITE_DEFAULT_PORT);
            graphiteGroupPrefix = props.getString("kafka.graphite.metrics.group", GRAPHITE_DEFAULT_PREFIX);
            String regex = props.getString("kafka.graphite.metrics.filter.regex", null);
			String logDir = props.getString("kafka.graphite.metrics.logFile", null);

			if (logDir != null) {
				setLogFile(logDir);
			}

            LOG.debug("Initialize GraphiteReporter ["+graphiteHost+","+graphitePort+","+graphiteGroupPrefix+"]");

            if (regex != null)
            	predicate = new MetricFilter() {
					@Override
					public boolean matches(String name, Metric metric) {
						return false;
					}
				};
            else
            	predicate = MetricFilter.ALL;

			reporter = createReporter(graphiteHost, graphitePort, graphiteGroupPrefix, predicate);

            if (props.getBoolean("kafka.graphite.metrics.reporter.enabled", false)) {
            	initialized = true;
            	startReporter(metricsConfig.pollingIntervalSecs());
                LOG.info("GraphiteReporter started.");
            }
        }
	}

	private GraphiteReporter createReporter(String graphiteHost, int graphitePort, String graphiteGroupPrefix,
											MetricFilter predicate){
		GraphiteReporter reporter = null;
		MetricRegistry registry = new MetricRegistryAdapter(Metrics.defaultRegistry());
		GraphiteReporter.Builder builder = GraphiteReporter.forRegistry(Metrics.defaultRegistry());

		try {
//			reporter = new GraphiteReporter(
//					Metrics.defaultRegistry(),
//					graphiteGroupPrefix,
//					predicate,
//					new GraphiteReporter.DefaultSocketProvider(graphiteHost, graphitePort),
//					Clock.defaultClock());

			reporter = builder.build(new Graphite(new InetSocketAddress("localhost", 2003), SocketFactory.getDefault()));
			LOG.info(String.format("Create new graphite reporter to %s:%d", graphiteHost, graphitePort));
		} catch (Exception e) {
			LOG.error("Unable to initialize GraphiteReporter", e);
		}

		return reporter;
	}

	private void setLogFile(String logDir){
		RollingFileAppender fa = new RollingFileAppender();
		fa.setName("GraphiteReporter");
		fa.setFile(logDir);
		fa.setLayout(new PatternLayout("[%d] %-4r [%t] %-5p %c %x - %m%n"));
		fa.setThreshold(Level.INFO);
		fa.setMaxFileSize("256MB");
		fa.setMaxBackupIndex(2);
		fa.setAppend(true);
		fa.activateOptions();

		LogManager.getLogger("com.criteo").removeAllAppenders();
		LogManager.getLogger("com.criteo").addAppender(fa);
	}
}
