#ifndef RAY_STREAMING_STREAMING_METRICS_H
#define RAY_STREAMING_STREAMING_METRICS_H
#include <iostream>

namespace ray {
namespace streaming {

class StreamingMetricsReporter {
 public :
  virtual void Report() = 0;
};

class DefaultMetricsReporter : public StreamingMetricsReporter {
 public :
  void Report();
};

class MetricsReporterDecorator : public StreamingMetricsReporter {
 public :
  MetricsReporterDecorator(std::shared_ptr<StreamingMetricsReporter> reporter);
  virtual void Report();
 private:
  std::shared_ptr<StreamingMetricsReporter> reporter_;
};

class KmonitorReporter : public MetricsReporterDecorator {
 public :
  KmonitorReporter(std::shared_ptr<StreamingMetricsReporter> reporter);
  void Report();
};

class PrometheusReporter: public MetricsReporterDecorator {
 public :
  PrometheusReporter(std::shared_ptr<StreamingMetricsReporter> reporter);
  void Report();
};
}
}

#endif //RAY_STREAMING_STREAMING_METRICS_H
