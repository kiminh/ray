#include "ray/util/logging.h"
#include "streaming_metrics.h"

#include "gtest/gtest.h"
using namespace ray::streaming;

TEST(metrics, multiReporter) {
  std::shared_ptr<StreamingMetricsReporter> reporter(new DefaultMetricsReporter());
  RAY_LOG(INFO) << "first decorator : ";
  reporter->Report();
  // reporter = std::make_shared<StreamingMetricsReporter>(new KmonitorReporter(reporter));
  reporter = std::make_shared<KmonitorReporter>(reporter);
  RAY_LOG(INFO) << "second decorator : ";
  reporter->Report();
  reporter = std::make_shared<PrometheusReporter>(reporter);
  RAY_LOG(INFO) << "third decorator : ";
  reporter->Report();
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
