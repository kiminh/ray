#include "timeout_manager.h"
#include "common.h"

namespace ray {

  namespace raylet {
    void TimeoutManager::AddTimeoutEntry(const TaskID &task_id,
                                         int64_t timeout_budget,
                                         int64_t last_updated) {
      TimeoutEntry entry(timeout_budget, last_updated);
      task_timeout_info_.insert({task_id, entry});
    }

    void TimeoutManager::RemoveTimeoutEntry(const TaskID &task_id) {
      auto it = task_timeout_info_.find(task_id);
      if (it == task_timeout_info_.end()) {
        return ;
      }

      task_timeout_info_.erase(it);
    }

    void TimeoutManager::UpdateTimeoutBudget(const TaskID &task_id,
                                             int64_t timeout_millis,
                                             int64_t now_millis) {

      auto it = task_timeout_info_.find(task_id);
      RAY_CHECK(it != task_timeout_info_.end());

      //Never timeout.
      if (timeout_millis <= 0) {
        return ;
      }

      auto past_time = now_millis - it->second.last_updated;
      auto reaming_time = it->second.timeout_budget - past_time;

      if (reaming_time <= 0) {
        //Timeout
        it->second.timeout_budget = 0;
      } else {
        //Not timeout
        it->second.timeout_budget = reaming_time;
      }

      it->second.last_updated = now_millis;
    }

    bool TimeoutManager::Timeout(const TaskID &task_id) const {
      auto it = task_timeout_info_.find(task_id);
      RAY_CHECK(it != task_timeout_info_.end());

      return it->second.timeout_budget == 0;
    }

    int64_t TimeoutManager::TimeoutBudgetMillis(const TaskID &task_id) const {
      auto it = task_timeout_info_.find(task_id);
      RAY_CHECK(it != task_timeout_info_.end());

      return it->second.timeout_budget;
    }

    int64_t TimeoutManager::LastUpdatedMillis(const TaskID &task_id) const {
      auto it = task_timeout_info_.find(task_id);
      RAY_CHECK(it != task_timeout_info_.end());

      return it->second.last_updated;
    }

    bool TimeoutManager::TimeoutEntryExists(const TaskID &task_id) const {
      auto it = task_timeout_info_.find(task_id);
      return it != task_timeout_info_.end();
    }

    void TimeoutManager::UpdateTimeoutBudget(const Task &task, int64_t now_millis) {
      UpdateTimeoutBudget(task.GetTaskSpecification().TaskId(),
                          task.GetTaskSpecification().TimeoutMillis(),
                          now_millis);
    }
  } //namespace raylet

} //namespace ray