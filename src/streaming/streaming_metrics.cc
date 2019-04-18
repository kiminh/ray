#include "ray/util/logging.h"

#include "streaming_metrics.h"
namespace ray {
namespace streaming {

void DefaultMetricsReporter::Report() { RAY_LOG(INFO) << "Default Reporter"; }

DefaultMetricsReporter::~DefaultMetricsReporter() {
  RAY_LOG(INFO) << "Destroy default reporter";
};

MetricsReporterDecorator::MetricsReporterDecorator(
    std::shared_ptr<StreamingMetricsReporter> reporter) {
  reporter_ = reporter;
}

void MetricsReporterDecorator::Report() {
  if (reporter_) {
    reporter_->Report();
  }
}

KmonitorReporter::KmonitorReporter(std::shared_ptr<StreamingMetricsReporter> reporter)
    : MetricsReporterDecorator(reporter) {}

void KmonitorReporter::Report() {
  MetricsReporterDecorator::Report();
  RAY_LOG(INFO) << "Kmonitor Reporter";
}

KmonitorReporter::~KmonitorReporter() { RAY_LOG(INFO) << "Destroy kmonitor reporter"; }

PrometheusReporter::PrometheusReporter(std::shared_ptr<StreamingMetricsReporter> reporter)
    : MetricsReporterDecorator(reporter) {}

void PrometheusReporter::Report() {
  MetricsReporterDecorator::Report();
  RAY_LOG(INFO) << "Promethues Reporter";
}

PrometheusReporter::~PrometheusReporter() {
  RAY_LOG(INFO) << "Destroy prometheus reporter";
}

}  // namespace streaming
}  // namespace ray
