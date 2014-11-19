package com.criteo.kafka;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.yammer.metrics.core.MetricsRegistry;

import java.util.SortedMap;

/**
 * Created by fgiraud on 11/19/14.
 */
public class MetricRegistryAdapter extends MetricRegistry {
    private MetricsRegistry registry = null;

    public MetricRegistryAdapter(MetricsRegistry metricsRegistry) {
        this.registry = metricsRegistry;
    }

    public SortedMap<String, Counter> getCounters(MetricFilter filter) {
        return registry.allMetrics() getMetrics(Counter.class, filter);
    }
}
