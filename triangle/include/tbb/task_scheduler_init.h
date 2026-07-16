#ifndef EMPTYHEADED_TBB_TASK_SCHEDULER_INIT_COMPAT_H
#define EMPTYHEADED_TBB_TASK_SCHEDULER_INIT_COMPAT_H

#include <cstddef>
#include <memory>

#include <tbb/global_control.h>

// EmptyHeaded predates oneTBB. Preserve the small portion of the removed
// task_scheduler_init API used while building tries.
namespace tbb {
class task_scheduler_init {
 public:
  explicit task_scheduler_init(int threads)
      : control_(new global_control(
            global_control::max_allowed_parallelism,
            static_cast<std::size_t>(threads))) {}

 private:
  std::unique_ptr<global_control> control_;
};
}  // namespace tbb

#endif
