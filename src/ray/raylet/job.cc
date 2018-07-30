#include "job.h"
#include "common.h"
#include "common_protocol.h"

namespace ray {

namespace raylet {

Job::Job(
  const JobID &job_id, const std::string &owner, const std::string &name,
  const std::string &host_server, const ObjectID &executable_id,
  protocol::JobState state, double create_time, double start_time, double end_time) {
  job_.id = job_id.binary();
  job_.owner = owner;
  job_.name = name;
  job_.host_server = host_server;
  job_.executable_id = executable_id.binary();
  job_.state = state;
  job_.create_time = create_time;
  job_.start_time = start_time;
  job_.end_time = end_time;
}

flatbuffers::Offset<protocol::Job> Job::ToFlatbuffer(
    flatbuffers::FlatBufferBuilder &fbb) const {
  fbb.ForceDefaults(true);
  return ray::protocol::Job::Pack(fbb, &job_);
}

JobID Job::Id() const {
  return JobID::from_binary(job_.id);
}

std::string Job::Owner() const {
  return job_.owner;
}

std::string Job::Name() const {
  return job_.name;
}

std::string Job::HostServer() const {
  return job_.host_server;
}

ObjectID Job::ExecutableId() const {
  return JobID::from_binary(job_.executable_id);
}

protocol::JobState Job::State() const {
  return job_.state;
}

double Job::CreateTime() const {
  return job_.create_time;
}

double Job::StartTime() const {
  return job_.start_time;
}

double Job::EndTime() const {
  return job_.end_time;
}

}  // namespace raylet

}  // namespace ray
