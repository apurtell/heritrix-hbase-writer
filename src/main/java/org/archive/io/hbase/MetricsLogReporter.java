package org.archive.io.hbase;

import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

public class MetricsLogReporter extends ScheduledReporter {

  public static Builder forRegistry(MetricRegistry registry) {
    return new Builder(registry);
  }

  public static class Builder {
    private final MetricRegistry registry;
    private Log logger;
    private TimeUnit rateUnit;
    private TimeUnit durationUnit;
    private MetricFilter filter;

    private Builder(MetricRegistry registry) {
      this.registry = registry;
      this.logger = LogFactory.getLog("metrics");
      this.rateUnit = TimeUnit.SECONDS;
      this.durationUnit = TimeUnit.MILLISECONDS;
      this.filter = MetricFilter.ALL;
    }

    public Builder outputTo(Log logger) {
      this.logger = logger;
      return this;
    }

    public Builder convertRatesTo(TimeUnit rateUnit) {
      this.rateUnit = rateUnit;
      return this;
    }

    public Builder convertDurationsTo(TimeUnit durationUnit) {
      this.durationUnit = durationUnit;
      return this;
    }

    public Builder filter(MetricFilter filter) {
      this.filter = filter;
      return this;
    }

    public MetricsLogReporter build() {
      return new MetricsLogReporter(registry, logger, rateUnit, durationUnit, filter);
    }
  }

  private final Log logger;

  private MetricsLogReporter(MetricRegistry registry, Log logger,
      TimeUnit rateUnit, TimeUnit durationUnit, MetricFilter filter) {
    super(registry, "log-reporter", filter, rateUnit, durationUnit);
    this.logger = logger;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void report(SortedMap<String, Gauge> gauges,
                     SortedMap<String, Counter> counters,
                     SortedMap<String, Histogram> histograms,
                     SortedMap<String, Meter> meters,
                     SortedMap<String, Timer> timers) {
    for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
      logGauge(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, Counter> entry : counters.entrySet()) {
      logCounter(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
      logHistogram(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, Meter> entry : meters.entrySet()) {
      logMeter(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, Timer> entry : timers.entrySet()) {
      logTimer(entry.getKey(), entry.getValue());
    }
  }

  private void logTimer(String name, Timer timer) {
    final Snapshot snapshot = timer.getSnapshot();
    logger.info(
      String.format(
        "type=TIMER, name=%s, count=%d, min=%2.2f, max=%2.2f, mean=%2.2f, stddev=%2.2f, median=%2.2f, " +
        "p75=%2.2f, p95=%2.2f, p98=%2.2f, p99=%2.2f, p999=%2.2f, mean_rate=%2.2f, m1=%2.2f, m5=%2.2f, " +
        "m15=%2.2f, rate_unit=%s, duration_unit=%s",
        name,
        timer.getCount(),
        convertDuration(snapshot.getMin()),
        convertDuration(snapshot.getMax()),
        convertDuration(snapshot.getMean()),
        convertDuration(snapshot.getStdDev()),
        convertDuration(snapshot.getMedian()),
        convertDuration(snapshot.get75thPercentile()),
        convertDuration(snapshot.get95thPercentile()),
        convertDuration(snapshot.get98thPercentile()),
        convertDuration(snapshot.get99thPercentile()),
        convertDuration(snapshot.get999thPercentile()),
        convertRate(timer.getMeanRate()),
        convertRate(timer.getOneMinuteRate()),
        convertRate(timer.getFiveMinuteRate()),
        convertRate(timer.getFifteenMinuteRate()),
        getRateUnit(),
        getDurationUnit()));
  }

  private void logMeter(String name, Meter meter) {
    logger.info(
      String.format(
        "type=METER, name=%s, count=%d, mean_rate=%2.2f, m1=%2.2f, m5=%2.2f, m15=%2.2f, rate_unit=%2.2f",
        name,
        meter.getCount(),
        convertRate(meter.getMeanRate()),
        convertRate(meter.getOneMinuteRate()),
        convertRate(meter.getFiveMinuteRate()),
        convertRate(meter.getFifteenMinuteRate()),
        getRateUnit()));
  }

  private void logHistogram(String name, Histogram histogram) {
    final Snapshot snapshot = histogram.getSnapshot();
    logger.info(
      String.format(
        "type=HISTOGRAM, name=%s, count=%d, min=%2.2f, max=%2.2f, mean=%2.2f, stddev=%2.2f, " +
        "median=%2.2f, p75=%2.2f, p95=%2.2f, p98=%2.2f, p99=%2.2f, p999=%2.2f",
        name,
        histogram.getCount(),
        snapshot.getMin(),
        snapshot.getMax(),
        snapshot.getMean(),
        snapshot.getStdDev(),
        snapshot.getMedian(),
        snapshot.get75thPercentile(),
        snapshot.get95thPercentile(),
        snapshot.get98thPercentile(),
        snapshot.get99thPercentile(),
        snapshot.get999thPercentile()));
  }

  private void logCounter(String name, Counter counter) {
    logger.info(String.format("type=COUNTER, name=%s, count=%d", name, counter.getCount()));
  }

  @SuppressWarnings("rawtypes")
  private void logGauge(String name, Gauge gauge) {
    logger.info(String.format("type=GAUGE, name=%s, value=%d", name, gauge.getValue()));
  }

  @Override
  protected String getRateUnit() {
    return "events/" + super.getRateUnit();
  }

}
