package com.criteo.kafka;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.*;
import com.yammer.metrics.reporting.AbstractReporter;
import com.yammer.metrics.reporting.SocketProvider;
import com.yammer.metrics.stats.Snapshot;
import com.yammer.metrics.core.MetricPredicate;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.Thread.State;
import java.net.Socket;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * A simple reporter which sends out application metrics to a <a href="http://graphite.wikidot.com/faq">Graphite</a>
 * server periodically.
 */
public class GraphiteReporter extends AbstractReporter implements MetricProcessor<Long>, Runnable {
    private static final Logger LOG = Logger.getLogger(GraphiteReporter.class);
    protected final String prefix;
    protected final MetricPredicate predicate;
    protected final Locale locale = Locale.US;
    protected final Clock clock;
    protected final SocketProvider socketProvider;
    protected final VirtualMachineMetrics vm;
    protected Writer writer;
    protected Socket socket = null;
    public boolean printVMMetrics = true;
    private final ScheduledExecutorService executor;

    /**
     * Enables the graphite reporter to send data for the default metrics registry to graphite
     * server with the specified period.
     *
     * @param period the period between successive outputs
     * @param unit   the time unit of {@code period}
     * @param host   the host name of graphite server (carbon-cache agent)
     * @param port   the port number on which the graphite server is listening
     */
    public static void enable(long period, TimeUnit unit, String host, int port) {
        enable(Metrics.defaultRegistry(), period, unit, host, port);
    }

    /**
     * Enables the graphite reporter to send data for the given metrics registry to graphite server
     * with the specified period.
     *
     * @param metricsRegistry the metrics registry
     * @param period          the period between successive outputs
     * @param unit            the time unit of {@code period}
     * @param host            the host name of graphite server (carbon-cache agent)
     * @param port            the port number on which the graphite server is listening
     */
    public static void enable(MetricsRegistry metricsRegistry, long period, TimeUnit unit, String host, int port) {
        enable(metricsRegistry, period, unit, host, port, null);
    }

    /**
     * Enables the graphite reporter to send data to graphite server with the specified period.
     *
     * @param period the period between successive outputs
     * @param unit   the time unit of {@code period}
     * @param host   the host name of graphite server (carbon-cache agent)
     * @param port   the port number on which the graphite server is listening
     * @param prefix the string which is prepended to all metric names
     */
    public static void enable(long period, TimeUnit unit, String host, int port, String prefix) {
        enable(Metrics.defaultRegistry(), period, unit, host, port, prefix);
    }

    /**
     * Enables the graphite reporter to send data to graphite server with the specified period.
     *
     * @param metricsRegistry the metrics registry
     * @param period          the period between successive outputs
     * @param unit            the time unit of {@code period}
     * @param host            the host name of graphite server (carbon-cache agent)
     * @param port            the port number on which the graphite server is listening
     * @param prefix          the string which is prepended to all metric names
     */
    public static void enable(MetricsRegistry metricsRegistry, long period, TimeUnit unit, String host, int port, String prefix) {
        enable(metricsRegistry, period, unit, host, port, prefix, MetricPredicate.ALL);
    }

    /**
     * Enables the graphite reporter to send data to graphite server with the specified period.
     *
     * @param metricsRegistry the metrics registry
     * @param period          the period between successive outputs
     * @param unit            the time unit of {@code period}
     * @param host            the host name of graphite server (carbon-cache agent)
     * @param port            the port number on which the graphite server is listening
     * @param prefix          the string which is prepended to all metric names
     * @param predicate       filters metrics to be reported
     */
    public static void enable(MetricsRegistry metricsRegistry, long period, TimeUnit unit, String host, int port, String prefix, MetricPredicate predicate) {
        try {
            final com.yammer.metrics.reporting.GraphiteReporter reporter = new com.yammer.metrics.reporting.GraphiteReporter(metricsRegistry,
                    prefix,
                    predicate,
                    new DefaultSocketProvider(host,
                            port),
                    Clock.defaultClock());
            reporter.start(period, unit);
        } catch (Exception e) {
            LOG.error("Error creating/starting Graphite reporter:", e);
        }
    }

    /**
     * Creates a new {@link com.yammer.metrics.reporting.GraphiteReporter}.
     *
     * @param host   is graphite server
     * @param port   is port on which graphite server is running
     * @param prefix is prepended to all names reported to graphite
     * @throws IOException if there is an error connecting to the Graphite server
     */
    public GraphiteReporter(String host, int port, String prefix) throws IOException {
        this(Metrics.defaultRegistry(), host, port, prefix);
    }

    /**
     * Creates a new {@link com.yammer.metrics.reporting.GraphiteReporter}.
     *
     * @param metricsRegistry the metrics registry
     * @param host            is graphite server
     * @param port            is port on which graphite server is running
     * @param prefix          is prepended to all names reported to graphite
     * @throws IOException if there is an error connecting to the Graphite server
     */
    public GraphiteReporter(MetricsRegistry metricsRegistry, String host, int port, String prefix) throws IOException {
        this(metricsRegistry,
                prefix,
                MetricPredicate.ALL,
                new DefaultSocketProvider(host, port),
                Clock.defaultClock());
    }

    /**
     * Creates a new {@link com.yammer.metrics.reporting.GraphiteReporter}.
     *
     * @param metricsRegistry the metrics registry
     * @param prefix          is prepended to all names reported to graphite
     * @param predicate       filters metrics to be reported
     * @param socketProvider  a {@link SocketProvider} instance
     * @param clock           a {@link Clock} instance
     * @throws IOException if there is an error connecting to the Graphite server
     */
    public GraphiteReporter(MetricsRegistry metricsRegistry, String prefix, MetricPredicate predicate, SocketProvider socketProvider, Clock clock) throws IOException {
        this(metricsRegistry, prefix, predicate, socketProvider, clock,
                VirtualMachineMetrics.getInstance());
    }

    /**
     * Creates a new {@link com.yammer.metrics.reporting.GraphiteReporter}.
     *
     * @param metricsRegistry the metrics registry
     * @param prefix          is prepended to all names reported to graphite
     * @param predicate       filters metrics to be reported
     * @param socketProvider  a {@link SocketProvider} instance
     * @param clock           a {@link Clock} instance
     * @param vm              a {@link VirtualMachineMetrics} instance
     * @throws IOException if there is an error connecting to the Graphite server
     */
    public GraphiteReporter(MetricsRegistry metricsRegistry, String prefix, MetricPredicate predicate, SocketProvider socketProvider, Clock clock, VirtualMachineMetrics vm) throws IOException {
        this(metricsRegistry, prefix, predicate, socketProvider, clock, vm, "graphite-reporter");
    }

    /**
     * Creates a new {@link com.yammer.metrics.reporting.GraphiteReporter}.
     *
     * @param metricsRegistry the metrics registry
     * @param prefix          is prepended to all names reported to graphite
     * @param predicate       filters metrics to be reported
     * @param socketProvider  a {@link SocketProvider} instance
     * @param clock           a {@link Clock} instance
     * @param vm              a {@link VirtualMachineMetrics} instance
     * @throws IOException if there is an error connecting to the Graphite server
     */
    public GraphiteReporter(MetricsRegistry metricsRegistry, String prefix, MetricPredicate predicate, SocketProvider socketProvider, Clock clock, VirtualMachineMetrics vm, String name) throws IOException {
        super(metricsRegistry);
        this.socketProvider = socketProvider;
        this.vm = vm;
        this.executor = metricsRegistry.newScheduledThreadPool(1, "graphite-reporter");

        this.clock = clock;

        if (prefix != null) {
            // Pre-append the "." so that we don't need to make anything conditional later.
            this.prefix = prefix + ".";
        } else {
            this.prefix = "";
        }
        this.predicate = predicate;
    }

    public void start(long period, TimeUnit unit) {
        this.executor.scheduleAtFixedRate(this, period, period, unit);
    }

    public void shutdown() {
        executor.shutdown();
    }

    @Override
    public void run() {
        int counter = 0;

        try {
            if (!isConnected(socket)) {
                closeConnection(socket);
                socket = this.socketProvider.get();
            }
            writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));

            final long epoch = clock.time() / 1000;
            if (this.printVMMetrics) {
                printVmMetrics(epoch);
            }
            LOG.info("Sending metrics. Timestamp: " + epoch);
            counter = printRegularMetrics(epoch);
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Error writing to Graphite", e);
            } else {
                LOG.error("Error writing to Graphite: ", e);
            }
        } finally {
            if (writer != null) {
                try {
                    writer.flush();
                } catch (IOException e1) {
                    LOG.error("Error while flushing writer:", e1);
                }
            }
            LOG.info(Integer.toString(counter) + " metric groups sent.");
        }
    }

    protected boolean isConnected(Socket socket) {
        if (socket == null) {
            return false;
        }

        // Sends a newline to test if the connection is open
        try {
            socket.getOutputStream().write("\n".getBytes());
        } catch (IOException e) {
            return false;
        }

        return true;
    }

    protected void closeConnection(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
                LOG.error("Error while closing socket:", e);
            }
        }
    }

    protected int printRegularMetrics(final Long epoch) {
        int metricCounter = 0;
        int failCounter = 0;

        for (Entry<String,SortedMap<MetricName,Metric>> entry : getMetricsRegistry().groupedMetrics(
                predicate).entrySet()) {
            for (Entry<MetricName, Metric> subEntry : entry.getValue().entrySet()) {
                final Metric metric = subEntry.getValue();
                if (metric != null) {
                    try {
                        metric.processWith(this, subEntry.getKey(), epoch);
                        metricCounter++;
                    } catch (Exception ignored) {
                        failCounter++;
                        if (failCounter < 3) { // Don't flood the log with stack traces
                            LOG.error("Error printing regular metrics:", ignored);
                        }
                    }
                }
                else {
                    LOG.info("Null metric: " + subEntry.getKey());
                }
            }
        }

        if (failCounter != 0) {
            LOG.error("Total failed metrics: " + failCounter);
        }

        return metricCounter;
    }

    protected void sendInt(long timestamp, String name, String valueName, long value) throws IOException {
        sendToGraphite(timestamp, name, valueName + " " + String.format(locale, "%d", value));
    }

    protected void sendFloat(long timestamp, String name, String valueName, double value) throws IOException {
        sendToGraphite(timestamp, name, valueName + " " + String.format(locale, "%2.2f", value));
    }

    protected void sendObjToGraphite(long timestamp, String name, String valueName, Object value) throws IOException {
        sendToGraphite(timestamp, name, valueName + " " + String.format(locale, "%s", value));
    }

    protected void sendToGraphite(long timestamp, String name, String value) throws IOException {
        if (!prefix.isEmpty()) {
            writer.write(prefix);
        }
        writer.write(sanitizeString(name));
        writer.write('.');
        writer.write(value);
        writer.write(' ');
        writer.write(Long.toString(timestamp));
        writer.write('\n');
        writer.flush();
    }

    protected String sanitizeName(MetricName name) {
        final StringBuilder sb = new StringBuilder()
                .append(name.getGroup())
                .append('.')
                .append(name.getType())
                .append('.');
        if (name.hasScope()) {
            sb.append(name.getScope())
                    .append('.');
        }
        return sb.append(name.getName()).toString();
    }

    protected String sanitizeString(String s) {
        return s.replace(' ', '-');
    }

    @Override
    public void processGauge(MetricName name, Gauge<?> gauge, Long epoch) throws IOException {
        sendObjToGraphite(epoch, sanitizeName(name), "value", gauge.value());
    }

    @Override
    public void processCounter(MetricName name, Counter counter, Long epoch) throws IOException {
        sendInt(epoch, sanitizeName(name), "count", counter.count());
    }

    @Override
    public void processMeter(MetricName name, Metered meter, Long epoch) throws IOException {
        final String sanitizedName = sanitizeName(name);
        sendInt(epoch, sanitizedName, "count", meter.count());
        sendFloat(epoch, sanitizedName, "meanRate", meter.meanRate());
        sendFloat(epoch, sanitizedName, "1MinuteRate", meter.oneMinuteRate());
        sendFloat(epoch, sanitizedName, "5MinuteRate", meter.fiveMinuteRate());
        sendFloat(epoch, sanitizedName, "15MinuteRate", meter.fifteenMinuteRate());
    }

    @Override
    public void processHistogram(MetricName name, Histogram histogram, Long epoch) throws IOException {
        final String sanitizedName = sanitizeName(name);
        sendSummarizable(epoch, sanitizedName, histogram);
        sendSampling(epoch, sanitizedName, histogram);
    }

    @Override
    public void processTimer(MetricName name, Timer timer, Long epoch) throws IOException {
        processMeter(name, timer, epoch);
        final String sanitizedName = sanitizeName(name);
        sendSummarizable(epoch, sanitizedName, timer);
        sendSampling(epoch, sanitizedName, timer);
    }

    protected void sendSummarizable(long epoch, String sanitizedName, Summarizable metric) throws IOException {
        sendFloat(epoch, sanitizedName, "min", metric.min());
        sendFloat(epoch, sanitizedName, "max", metric.max());
        sendFloat(epoch, sanitizedName, "mean", metric.mean());
        sendFloat(epoch, sanitizedName, "stddev", metric.stdDev());
    }

    protected void sendSampling(long epoch, String sanitizedName, Sampling metric) throws IOException {
        final Snapshot snapshot = metric.getSnapshot();
        sendFloat(epoch, sanitizedName, "median", snapshot.getMedian());
        sendFloat(epoch, sanitizedName, "75percentile", snapshot.get75thPercentile());
        sendFloat(epoch, sanitizedName, "95percentile", snapshot.get95thPercentile());
        sendFloat(epoch, sanitizedName, "98percentile", snapshot.get98thPercentile());
        sendFloat(epoch, sanitizedName, "99percentile", snapshot.get99thPercentile());
        sendFloat(epoch, sanitizedName, "999percentile", snapshot.get999thPercentile());
    }

    protected void printVmMetrics(long epoch) throws IOException {
        sendFloat(epoch, "jvm.memory", "heap_usage", vm.heapUsage());
        sendFloat(epoch, "jvm.memory", "non_heap_usage", vm.nonHeapUsage());
        for (Entry<String, Double> pool : vm.memoryPoolUsage().entrySet()) {
            sendFloat(epoch, "jvm.memory.memory_pool_usages", sanitizeString(pool.getKey()), pool.getValue());
        }

        sendInt(epoch, "jvm", "daemon_thread_count", vm.daemonThreadCount());
        sendInt(epoch, "jvm", "thread_count", vm.threadCount());
        sendInt(epoch, "jvm", "uptime", vm.uptime());
        sendFloat(epoch, "jvm", "fd_usage", vm.fileDescriptorUsage());

        for (Entry<State, Double> entry : vm.threadStatePercentages().entrySet()) {
            sendFloat(epoch, "jvm.thread-states", entry.getKey().toString().toLowerCase(), entry.getValue());
        }

        for (Entry<String, VirtualMachineMetrics.GarbageCollectorStats> entry : vm.garbageCollectors().entrySet()) {
            final String name = "jvm.gc." + sanitizeString(entry.getKey());
            sendInt(epoch, name, "time", entry.getValue().getTime(TimeUnit.MILLISECONDS));
            sendInt(epoch, name, "runs", entry.getValue().getRuns());
        }
    }

    public static class DefaultSocketProvider implements SocketProvider {

        private final String host;
        private final int port;

        public DefaultSocketProvider(String host, int port) {
            this.host = host;
            this.port = port;

        }

        @Override
        public Socket get() throws Exception {
            LOG.info("Opening connection to: " + this.host + " " + this.port);
            Socket s = new Socket(this.host, this.port);

            s.setKeepAlive(true);

            return s;
        }

    }
}