#pragma once

namespace ray {

namespace failure_detector {

class FailureDetector {
 public:
  virtual ~FailureDetector() = default;

 public:
  virtual void PauseOnMaster() = 0;
  virtual void ResumeOnMaster() = 0;
};

}

}

