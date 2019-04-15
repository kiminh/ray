//
// Created by ashione on 2019/4/1.
//

#include "streaming_metrics.h"
namespace ray {
namespace streaming {

void DefaultMetricsReporter::Report() {
  std::cout << "Default Reporter" << std::endl;
}

MetricsReporterDecorator::MetricsReporterDecorator(std::shared_ptr<StreamingMetricsReporter> reporter) {
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
  std::cout << "Kmonitor Reporter" << std::endl;
}

PrometheusReporter::PrometheusReporter(std::shared_ptr<StreamingMetricsReporter> reporter)
  : MetricsReporterDecorator(reporter) {}

void PrometheusReporter::Report() {
  MetricsReporterDecorator::Report();
  std::cout << "Promethues Reporter" << std::endl;
}

}
}
