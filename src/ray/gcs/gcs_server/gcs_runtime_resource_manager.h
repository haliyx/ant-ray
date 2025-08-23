// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once
#include <memory>
#include <thread>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/asio/asio_util.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/periodical_runner.h"
// #include "ray/common/task/scheduling_resources.h"
#include "ray/gcs/gcs_server/gcs_init_data.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"
#include "ray/raylet/scheduling/cluster_resource_manager.h"
#include "ray/raylet/scheduling/policy/scheduling_context.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"
// #include "ray/util/thread_pool.h"
#include "ray/util/util.h"

namespace ray {
// using raylet_scheduling_policy::NodegroupScheduleOptions;
namespace gcs {

// struct WindowContext {
//   std::vector<ResourceRequest> resources_window;
//   std::shared_ptr<const NodegroupScheduleOptions> schedule_options;
//   int64_t next_calc_time_ms = 0;
// };

class GcsTableStorage;
class GcsRuntimeResourceManager;
// class GcsJobDistribution;
class GcsRuntimeResourceManager : public rpc::RuntimeResourceHandler {
 public:
  explicit GcsRuntimeResourceManager(
      // gcs::GcsTableStorage &gcs_table_storage,
      ClusterResourceManager &cluster_resource_manager,
      std::function<void(const rpc::ReportClusterRuntimeResourcesRequest &)>
          cluster_runtime_resources_updated_callback);
      //   std::shared_ptr<GcsJobDistribution> gcs_job_distribution,

  virtual ~GcsRuntimeResourceManager() = default;


  //   void RecordJobShorttermRuntimeResources(
  //       absl::flat_hash_map<
  //           JobID,
  //           std::pair<ResourceRequest, std::shared_ptr<const
  //           NodegroupScheduleOptions>>> job_shortterm_runtime_resources);

  //   void CalcJobLongtermRuntimeResources();

  /// Record the inputs of runtime resources to each node's window.
  void RecordRuntimeResources(
      const std::shared_ptr<absl::flat_hash_map<NodeID, rpc::NodeRuntimeResources>>
          &node_runtime_resources_map);

 protected:
  void HandleReportClusterRuntimeResources(
      rpc::ReportClusterRuntimeResourcesRequest request,
      rpc::ReportClusterRuntimeResourcesReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;


 private:
  /// The gcs table storage.
  // gcs::GcsTableStorage &gcs_table_storage_;

  /// The cluster resource manager.
  ClusterResourceManager &cluster_resource_manager_;

  /// The callback for each update of the cluster runtime resources.
  std::function<void(const rpc::ReportClusterRuntimeResourcesRequest &)>
      cluster_runtime_resources_updated_callback_;


};

}  // namespace gcs
}  // namespace ray
