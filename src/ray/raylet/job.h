#ifndef RAY_RAYLET_JOB_H
#define RAY_RAYLET_JOB_H

#include <inttypes.h>

#include "ray/raylet/format/node_manager_generated.h"
#include "ray/id.h"

namespace ray {

namespace raylet {

/// \class Job
///
/// A Job equals to a driver in Ray concept.
class Job {
 public:
  /// Deserialize a job from a flatbuffer.
  ///
  /// \param string A serialized jobs flatbuffer.
  Job(const flatbuffers::String &string);


  Job(const JobID &job_id, const std::string &owner, const std::string &name,
      const std::string &host_server, const ObjectID &executable_id,
      protocol::JobState state, double create_time, double start_time, double end_time);

  ~Job() {}

  /// Serialize the job to a flatbuffer.
  ///
  /// \param fbb The flatbuffer builder to serialize with.
  /// \return An offset to the serialized job.
  flatbuffers::Offset<protocol::Job> ToFlatbuffer(
      flatbuffers::FlatBufferBuilder &fbb) const;

  /// Interfaces to access the fields of this job.
  JobID Id() const;
  std::string Owner() const;
  std::string Name() const;
  std::string HostServer() const;
  ObjectID ExecutableId() const;
  protocol::JobState State() const;
  double CreateTime() const;
  double StartTime() const;
  double EndTime() const;

 private:
  protocol::JobT job_;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_JOB_H
