//
// Created by ashione on 2019/4/1.
//

#ifndef STREAMING_PROTOTYPE_STREAMING_METRICS_H
#define STREAMING_PROTOTYPE_STREAMING_METRICS_H
#include <iostream>

class StreamingMetricsReporter {
 public :
  virtual void Report() = 0;
};

class DefaultMetricsReporter : public StreamingMetricsReporter {
 public :
  void Report() { std::cout << "Default Reporter" << std::endl;}
};

class MetricsReporterDecorator : public StreamingMetricsReporter {
 public :
  MetricsReporterDecorator(StreamingMetricsReporter *reporter) {
    reporter_ = reporter;
  }
  virtual void Report() {
    if (reporter_) {
      reporter_->Report();
    }
  }
 private:
  StreamingMetricsReporter *reporter_ = nullptr;
};

class KmonitorReporter : public MetricsReporterDecorator {
 public :
  KmonitorReporter(StreamingMetricsReporter *reporter) : MetricsReporterDecorator(reporter) {}
  void Report() {
    MetricsReporterDecorator::Report();
    std::cout << "Kmonitor Reporter" << std::endl;
  }
};

class PrometheusReporter: public MetricsReporterDecorator {
 public :
  PrometheusReporter(StreamingMetricsReporter *reporter) : MetricsReporterDecorator(reporter) {}
  void Report() {
    MetricsReporterDecorator::Report();
    std::cout << "Prometheus Reporter" << std::endl;
  }
};

#endif //STREAMING_PROTOTYPE_STREAMING_METRICS_H
