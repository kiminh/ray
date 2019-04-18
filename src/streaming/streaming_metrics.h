#ifndef RAY_STREAMING_STREAMING_METRICS_H
#define RAY_STREAMING_STREAMING_METRICS_H
#include <memory>

namespace ray {
namespace streaming {

class StreamingMetricsReporter {
 public:
  virtual void Report() = 0;
  virtual ~StreamingMetricsReporter(){};
};

class DefaultMetricsReporter : public StreamingMetricsReporter {
 public:
  void Report();
  ~DefaultMetricsReporter();
};

class MetricsReporterDecorator : public StreamingMetricsReporter {
 public:
  MetricsReporterDecorator(std::shared_ptr<StreamingMetricsReporter> reporter);
  virtual void Report();
  virtual ~MetricsReporterDecorator(){};

 private:
  std::shared_ptr<StreamingMetricsReporter> reporter_;
};

class KmonitorReporter : public MetricsReporterDecorator {
 public:
  KmonitorReporter(std::shared_ptr<StreamingMetricsReporter> reporter);
  void Report();
  virtual ~KmonitorReporter();
};

class PrometheusReporter : public MetricsReporterDecorator {
 public:
  PrometheusReporter(std::shared_ptr<StreamingMetricsReporter> reporter);
  void Report();
  virtual ~PrometheusReporter();
};
}  // namespace streaming
}  // namespace ray

#endif  // RAY_STREAMING_STREAMING_METRICS_H
