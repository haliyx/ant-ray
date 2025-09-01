// Copyright 2023 The Ray Authors.
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

#include "ray/raylet/scheduling/policy/runtime_resource_scheduling_policy.h"



namespace ray {
namespace raylet_scheduling_policy {

bool HybridSchedulingPolicy::IsNodeFeasible(
    const scheduling::NodeID &node_id,
    const NodeFilter &node_filter,
    const NodeResources &node_resources,
    const ResourceRequest &resource_request) const {
  if (!is_node_alive_(node_id)) {
    return false;
  }

  if (node_filter != NodeFilter::kAny) {
    const bool has_gpu = node_resources.total.Has(ResourceID::GPU());
    if (node_filter == NodeFilter::kGPU && !has_gpu) {
      return false;
    } else if (node_filter == NodeFilter::kNonGpu && has_gpu) {
      return false;
    }
  }

  return node_resources.IsFeasible(resource_request);
}

scheduling::NodeID RuntimeResourceSchedulingPolicy::ScheduleImpl(
    const ResourceRequest &resource_request,
    float spread_threshold,
    bool force_spillback,
    bool require_node_available,
    NodeFilter node_filter,
    const std::string &preferred_node,
    int32_t schedule_top_k_absolute,
    float scheduler_top_k_fraction,
    const SchedulingContext *scheduling_context) {
  // Nodes that are feasible and currently have available resources.
  std::vector<std::pair<scheduling::NodeID, float>> available_nodes;
  // Nodes that are feasible but currently do not have available resources.
  std::vector<std::pair<scheduling::NodeID, float>> feasible_and_unavailable_nodes;
  // Check whether the local node is available and feasible. We'll use this to
  // help prioritize the local node when force_spillback=false.
  bool preferred_node_is_available = false;
  bool preferred_node_is_feasible = false;
  scheduling::NodeID preferred_node_id = local_node_id_;
  if (!preferred_node.empty()) {
    auto new_id = scheduling::NodeID(preferred_node);
    if (nodes_.contains(new_id)) {
      preferred_node_id = new_id;
    }
  }
  for (const auto &pair : nodes_) {
    const auto &node_id = pair.first;
    const auto &node_resources = pair.second.GetLocalView();
    if (force_spillback && node_id == preferred_node_id) {
      continue;
    }
    if (!is_node_schedulable_(node_id, scheduling_context)) {
      continue;
    }
    if (IsNodeFeasible(node_id, node_filter, node_resources, resource_request)) {
      bool ignore_pull_manager_at_capacity = false;
      if (node_id == preferred_node_id) {
        // It's okay if the local node's pull manager is at
        // capacity because we will eventually spill the task
        // back from the waiting queue if its args cannot be
        // pulled.
        ignore_pull_manager_at_capacity = true;
        preferred_node_is_feasible = true;
      }
      bool is_available =
          node_resources.IsAvailable(resource_request, ignore_pull_manager_at_capacity);
      if (node_id == preferred_node_id && is_available) {
        preferred_node_is_available = true;
      }
      float node_score = ComputeNodeScoreImpl(node_resources, spread_threshold);
      RAY_LOG(DEBUG) << "Node " << node_id.ToInt() << " is "
                     << (is_available ? "available" : "not available") << " for request "
                     << resource_request.DebugString()
                     << " with critical resource utilization " << node_score
                     << " based on local view " << node_resources.DebugString();
      if (is_available) {
        available_nodes.push_back({node_id, node_score});
      } else {
        feasible_and_unavailable_nodes.push_back({node_id, node_score});
      }
    }
  }

  size_t num_candidate_nodes =
      std::max<int32_t>(schedule_top_k_absolute,
                        static_cast<int32_t>(nodes_.size() * scheduler_top_k_fraction));

  if (!available_nodes.empty()) {
    bool prioritize_preferred_node = !force_spillback && preferred_node_is_available;
    // First prioritize available nodes.
    return GetBestNode(available_nodes,
                       num_candidate_nodes,
                       prioritize_preferred_node
                           ? std::optional<scheduling::NodeID>(preferred_node_id)
                           : std::optional<scheduling::NodeID>(),
                       ComputeNodeScore(preferred_node_id, spread_threshold));
  } else if (!feasible_and_unavailable_nodes.empty() && !require_node_available) {
    bool prioritize_preferred_node = !force_spillback && preferred_node_is_feasible;
    // If there are no available nodes, and the caller is okay with an
    // unavailable node, check the feasible nodes next.
    return GetBestNode(feasible_and_unavailable_nodes,
                       num_candidate_nodes,
                       prioritize_preferred_node
                           ? std::optional<scheduling::NodeID>(preferred_node_id)
                           : std::optional<scheduling::NodeID>(),
                       ComputeNodeScore(preferred_node_id, spread_threshold));
  } else {
    return scheduling::NodeID::Nil();
  }
}

scheduling::NodeID RuntimeResourceSchedulingPolicy::Schedule(
    const ResourceRequest &resource_request, SchedulingOptions options) {
  RAY_CHECK(options.scheduling_type == SchedulingType:RUNTIME_RESOURCE);

  if (!options.avoid_gpu_nodes || resource_request.Has(ResourceID::GPU())) {
    return ScheduleImpl(resource_request,
                        options.spread_threshold,
                        options.avoid_local_node,
                        options.require_node_available,
                        NodeFilter::kAny,
                        options.preferred_node_id,
                        options.schedule_top_k_absolute,
                        options.scheduler_top_k_fraction,
                        options.scheduling_context.get());
  }

  // Try schedule on non-GPU nodes.
  auto best_node_id = ScheduleImpl(resource_request,
                                   options.spread_threshold,
                                   options.avoid_local_node,
                                   /*require_node_available*/ true,
                                   NodeFilter::kNonGpu,
                                   options.preferred_node_id,
                                   options.schedule_top_k_absolute,
                                   options.scheduler_top_k_fraction,
                                   options.scheduling_context.get());
  if (!best_node_id.IsNil()) {
    return best_node_id;
  }

  // If we cannot find any available node from non-gpu nodes, fallback to the original
  // scheduling
  return ScheduleImpl(resource_request,
                      options.spread_threshold,
                      options.avoid_local_node,
                      options.require_node_available,
                      NodeFilter::kAny,
                      options.preferred_node_id,
                      options.schedule_top_k_absolute,
                      options.scheduler_top_k_fraction,
                      options.scheduling_context.get());
}

scheduling::NodeID NodeLabelSchedulingPolicy::SelectBestNode(
    const absl::flat_hash_map<scheduling::NodeID, const Node *> &hard_match_nodes,
    const absl::flat_hash_map<scheduling::NodeID, const Node *>
        &hard_and_soft_match_nodes,
    const ResourceRequest &resource_request) {
  // 1. Match hard&soft and available nodes.
  if (!hard_and_soft_match_nodes.empty()) {
    auto available_soft_nodes =
        SelectAvailableNodes(hard_and_soft_match_nodes, resource_request);
    if (!available_soft_nodes.empty()) {
      return SelectRandomNode(available_soft_nodes);
    }
  }

  // 2. Match hard and available nodes.
  auto available_nodes = SelectAvailableNodes(hard_match_nodes, resource_request);
  if (!available_nodes.empty()) {
    return SelectRandomNode(available_nodes);
  }

  // 3. Match hard&soft and feasible nodes.
  if (!hard_and_soft_match_nodes.empty()) {
    return SelectRandomNode(hard_and_soft_match_nodes);
  }
  // 4. Match hard and feasible nodes.
  return SelectRandomNode(hard_match_nodes);
}

scheduling::NodeID NodeLabelSchedulingPolicy::SelectRandomNode(
    const absl::flat_hash_map<scheduling::NodeID, const Node *> &candidate_nodes) {
  std::uniform_int_distribution<int> distribution(0, candidate_nodes.size() - 1);
  int node_index = distribution(gen_);
  auto it = candidate_nodes.begin();
  std::advance(it, node_index);
  return it->first;
}

absl::flat_hash_map<scheduling::NodeID, const Node *>
NodeLabelSchedulingPolicy::SelectFeasibleNodes(
    const ResourceRequest &resource_request,
    const SchedulingContext *scheduling_context) const {
  absl::flat_hash_map<scheduling::NodeID, const Node *> candidate_nodes;
  for (const auto &pair : nodes_) {
    const auto &node_id = pair.first;
    const auto &node_resources = pair.second.GetLocalView();
    if (is_node_alive_(node_id) && is_node_schedulable_(node_id, scheduling_context) &&
        node_resources.IsFeasible(resource_request)) {
      candidate_nodes.emplace(node_id, &pair.second);
    }
  }
  return candidate_nodes;
}

absl::flat_hash_map<scheduling::NodeID, const Node *>
NodeLabelSchedulingPolicy::SelectAvailableNodes(
    const absl::flat_hash_map<scheduling::NodeID, const Node *> &candidate_nodes,
    const ResourceRequest &resource_request) const {
  absl::flat_hash_map<scheduling::NodeID, const Node *> available_nodes;
  for (const auto &pair : candidate_nodes) {
    const auto &node_id = pair.first;
    const auto &node_resources = pair.second->GetLocalView();
    if (node_resources.IsAvailable(resource_request)) {
      available_nodes.emplace(node_id, pair.second);
    }
  }
  return available_nodes;

}


}  // namespace raylet_scheduling_policy
}  // namespace ray
