#include <sstream>

#include "ray/common/task/task_execution_spec.h"

namespace ray {

size_t TaskExecutionSpecification::NumForwards() const {
  return message_->num_forwards();
}

void TaskExecutionSpecification::IncrementNumForwards() {
  message_->set_num_forwards(message_->num_forwards() + 1);
}

std::string TaskExecutionSpecification::DebugString() const {
  std::ostringstream stream;
  stream << "num_forwards=" << message_->num_forwards();
  return stream.str();
}

rapidjson::Document TaskExecutionSpecification::ToJson(rapidjson::Document::AllocatorType* allocator) const {
  rapidjson::Document doc(rapidjson::kObjectType, allocator);
  rapidjson::Document::AllocatorType &alloc = doc.GetAllocator();

  doc.AddMember("num forwards", message_->num_forwards(), alloc);

  return doc;
}

}  // namespace ray
