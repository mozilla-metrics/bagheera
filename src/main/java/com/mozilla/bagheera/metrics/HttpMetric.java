package com.mozilla.bagheera.metrics;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

public class HttpMetric {
    public final String id;
    private Counter totalReqsCount, failedReqsCount, successfulReqsCount;
    private Counter totalReqsSize, failedReqsSize, successfulReqsSize;

    HttpMetric(String id) {
        this.id = id;
        this.configureMetrics();
    }
    
    private void configureMetrics() {
        this.totalReqsCount = Metrics.newCounter(new MetricName(this.getClass(), this.id + ".total_reqs_count"));
        this.failedReqsCount = Metrics.newCounter(new MetricName(this.getClass(), this.id + ".failed_reqs_count"));
        this.successfulReqsCount = Metrics.newCounter(new MetricName(this.getClass(), this.id + ".successful_reqs_count"));
        this.totalReqsSize = Metrics.newCounter(new MetricName(this.getClass(), this.id + ".total_reqs_size"));
        this.failedReqsSize = Metrics.newCounter(new MetricName(this.getClass(), this.id + ".failed_reqs_size"));
        this.successfulReqsSize = Metrics.newCounter(new MetricName(this.getClass(), this.id + ".successful_reqs_size"));
    }

    public void updateFailed(Integer size) {
        this.failedReqsCount.inc();
        this.failedReqsSize.inc(size);
        this.totalReqsCount.inc();
        this.totalReqsSize.inc(size);

    }

    public void updateSuccessful(Integer size) {
        this.successfulReqsCount.inc();
        this.successfulReqsSize.inc(size);
        this.totalReqsCount.inc();
        this.totalReqsSize.inc(size);
    }
}
