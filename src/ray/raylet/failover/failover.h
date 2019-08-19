#pragma once

#include "ray/protobuf/failover.pb.h"
#include "src/ray/raylet/worker.h"

namespace ray {
namespace raylet {

class Failover {
 public:
  virtual ~Failover() = default;
  virtual void OnWorkerExit(Worker *worker) = 0;
  virtual ray::Status OnResetState(const ray::rpc::ResetStateRequest &request,
                                   ray::rpc::ResetStateReply *reply) = 0;
};

}  // namespace raylet
}  // namespace ray
