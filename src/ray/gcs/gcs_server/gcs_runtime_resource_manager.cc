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

#include "ray/gcs/gcs_server/gcs_runtime_resource_manager.h"

// #include "ray/gcs/gcs_server/gcs_job_distribution.h"
// #include "ray/util/resource_util.h"

namespace ray {
namespace gcs {

GcsRuntimeResourceManager::GcsRuntimeResourceManager(
    // gcs::GcsTableStorage &gcs_table_storage,
    ClusterResourceManager &cluster_resource_manager,
    std::function<void(const rpc::ReportClusterRuntimeResourcesRequest &)>
        cluster_runtime_resources_updated_callback)
    // : gcs_table_storage_(gcs_table_storage),
    : cluster_resource_manager_(cluster_resource_manager),
      cluster_runtime_resources_updated_callback_(
          std::move(cluster_runtime_resources_updated_callback)) {}

void GcsRuntimeResourceManager::HandleReportClusterRuntimeResources(
    rpc::ReportClusterRuntimeResourcesRequest request,
    rpc::ReportClusterRuntimeResourcesReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "HandleReportClusterRuntimeResources";
  cluster_runtime_resources_updated_callback_(request);
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());

  // record_service_.post(
  //     [this, node_runtime_resources_map] {
  //       RecordRuntimeResources(node_runtime_resources_map);
  //     },
  //     "gcsUpdateRuntimeResources");
}

void GcsRuntimeResourceManager::RecordRuntimeResources(
    const std::shared_ptr<absl::flat_hash_map<NodeID, rpc::NodeRuntimeResources>>
        &node_runtime_resources_map) {
  if (node_runtime_resources_map->empty()) {
    return;
  }
  auto cluster_node_resources_to_update =
      std::make_shared<absl::flat_hash_map<scheduling::NodeID,
                                           absl::flat_hash_map<int, ResourceRequest>>>();
  for (const auto &entry : *node_runtime_resources_map) {
    const auto &node_id = entry.first;
    scheduling::NodeID node_id_obj(node_id.Binary());
    const auto &resources_entry = entry.second;
    absl::flat_hash_map<int, ResourceRequest> worker_runtime_resources;
    for (const auto &worker : resources_entry.worker_stat_list()) {
      auto &worker_resources = worker_runtime_resources[worker.pid()];
      double mem_tail = 1.0 * worker.memory_tail() / 1ULL;
      worker_resources.Set(scheduling::ResourceID::RuntimeMemory(), mem_tail);
      worker_resources.Set(scheduling::ResourceID::RuntimeCPU(),
                           double(worker.cpu_tail() / 100.0));
    }
    (*cluster_node_resources_to_update)[node_id_obj] = worker_runtime_resources;
  }
  if (!cluster_node_resources_to_update->empty()) {
    cluster_resource_manager_.UpdateClusterRuntimeResources(
        std::move(*cluster_node_resources_to_update));
  }
}

}  // namespace gcs
}  // namespace ray