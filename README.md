Kafka Graphite Metrics Reporter
==============================

This is a simple reporter for kafka using the 
[GraphiteReporter](http://metrics.codahale.com/manual/graphite/). It works with 
kafka 0.8.1.1 version.

About this fork
---------------

This fork adds new features over the original graphite reporter available
[here](https://github.com/damienclaveau/kafka-graphite).

1. Correct timing: the previous version used the function `scheduleWithFixedDelay` to
schedule metric reporting, causing the time required to send metrics to be added
added to the normal wait interval, leading to missing data points in graphite.
In this version `scheduleAtFixedRate` is used instead.
2. Prefix: the prefix option allows to specify which metrics we want to export.
3. Logging: logging on file is supported to check if the reporter is operating correctly.

Install On Broker
-----------------

1. Build the `kafka-graphite-1.1.2.jar` jar using `mvn package`.
2. Add `kafka-graphite-1.1.2.jar` and `metrics-graphite-2.2.0.jar` to the `libs/`
   directory of your kafka broker installation or to the classpath
3. Configure the broker (see the configuration section below)
4. Restart the broker

Configuration
------------

Edit the `server.properties` file of your installation, activate the reporter by setting:

    kafka.metrics.reporters=com.criteo.kafka.KafkaGraphiteMetricsReporter[,kafka.metrics.KafkaCSVMetricsReporter[,....]]
    kafka.graphite.metrics.reporter.enabled=true

Here is a list of default properties used:

    kafka.graphite.metrics.host=localhost
    kafka.graphite.metrics.port=8649
    kafka.graphite.metrics.group=kafka
    # This can be use to filter some metrics from graphite
    # since kafka has quite a lot of metrics, it is useful
    # if you have many topics/partitions.
    # Only metrics matching with the pattern will be written to graphite.
    kafka.graphite.metrics.filter.regex="kafka.server":type="BrokerTopicMetrics",.*
    kafka.graphite.metrics.logFile=/var/log/kafka/graphite-reporter.log
