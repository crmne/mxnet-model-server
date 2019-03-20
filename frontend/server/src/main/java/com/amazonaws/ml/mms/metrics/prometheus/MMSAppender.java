package com.amazonaws.ml.mms.metrics.prometheus;

import com.amazonaws.ml.mms.metrics.Dimension;
import com.amazonaws.ml.mms.metrics.Metric;
import com.amazonaws.ml.mms.util.ConfigManager;
import io.prometheus.client.Histogram;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

public class MMSAppender extends AppenderSkeleton {

    private static final String MODEL_NAME_STATSD_NAME;
    private static final String PREDICTION_TIME_STATSD_NAME;
    private static final Histogram PREDICTION_HISTOGRAM;

    static {
        MODEL_NAME_STATSD_NAME = "ModelName";
        PREDICTION_TIME_STATSD_NAME = "PredictionTime";
        PREDICTION_HISTOGRAM =
                Histogram.build()
                        .name("mms_prediction_seconds")
                        .help("Prediction time in seconds.")
                        .labelNames("model_name", "hostname")
                        .register();
    }

    /** Create a new instrumented appender using the default registry. */
    public MMSAppender() {}

    @Override
    public void activateOptions() {}

    private void appendModelMetrics(Metric metric) {
        if (metric.getMetricName().equals(PREDICTION_TIME_STATSD_NAME)) {
            double metricInSeconds;
            switch (metric.getUnit()) {
                case "Milliseconds":
                    double metricInMillis = Double.parseDouble(metric.getValue());
                    metricInSeconds = metricInMillis / 1000;
                    break;

                case "Seconds":
                    metricInSeconds = Double.parseDouble(metric.getValue());
                    break;

                default:
                    return;
            }

            String modelName = "MODEL_NAME_NOT_SPECIFIED";
            for (Dimension dimension : metric.getDimensions()) {
                if (dimension.getName().equals(MODEL_NAME_STATSD_NAME)) {
                    modelName = dimension.getValue();
                }
            }

            PREDICTION_HISTOGRAM.labels(modelName, metric.getHostName()).observe(metricInSeconds);
        }
    }

    @Override
    protected void append(LoggingEvent event) {
        Metric metric;
        switch (event.getLoggerName()) {
            case ConfigManager.MODEL_METRICS_LOGGER:
                metric = (Metric) event.getMessage();
                appendModelMetrics(metric);
                break;

            default:
                break;
        }
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public boolean requiresLayout() {
        return false;
    }
}
