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

#include <memory>

#include "gtest/gtest.h"
#include "scheduling_resources.h"
#include "ray/common/id.h"

namespace ray {
class SchedulingResourcesTest : public ::testing::Test {
public:
  void SetUp() override {
      resource_set =  std::make_shared<ResourceSet>();
      resource_id_set = std::make_shared<ResourceIdSet>();
  }

  protected:
  std::shared_ptr<ResourceSet>resource_set;
  std::shared_ptr<ResourceIdSet>resource_id_set;

}; 

TEST_F(SchedulingResourcesTest, AddBundleResources) {
    BundleID bundle_id = BundleID::FromRandom();
    std::vector<std::string> resource_labels = {"CPU"};
    std::vector<double> resource_capacity = {1.0};
    ResourceSet resource(resource_labels,resource_capacity);
    resource_set->AddBundleResources(bundle_id.Binary(),resource);
    resource_labels.pop_back();
    resource_labels.push_back(bundle_id.Binary() + "_" + "CPU");
    ResourceSet result_resource(resource_labels,resource_capacity);
    ASSERT_EQ(1, resource_set->IsEqual(result_resource));
}

TEST_F(SchedulingResourcesTest, AddBundleResource) {
    BundleID bundle_id = BundleID::FromRandom();
    std::string name = bundle_id.Binary() + "_" + "CPU";
    std::vector<int64_t> whole_ids = {1, 2, 3};
    ResourceIds resource_ids(whole_ids);
    resource_id_set->AddBundleResource(name, resource_ids);
    ASSERT_EQ(1, resource_id_set->AvailableResources().size());
    ASSERT_EQ(name, resource_id_set->AvailableResources().begin()->first);
}

TEST_F(SchedulingResourcesTest, ReturnBundleResources) {
    BundleID bundle_id = BundleID::FromRandom();
    std::vector<std::string> resource_labels = {"CPU"};
    std::vector<double> resource_capacity = {1.0};
    ResourceSet resource(resource_labels,resource_capacity);
    resource_set->AddBundleResources(bundle_id.Binary(),resource);
    resource_labels.pop_back();
    resource_labels.push_back(bundle_id.Binary() + "_" + "CPU");
    ResourceSet result_resource(resource_labels,resource_capacity);
    ASSERT_EQ(1, resource_set->IsEqual(result_resource));

    resource_set->ReturnBundleResources(bundle_id.Binary());
    ASSERT_EQ(1, resource_set->IsEqual(resource));
}

} // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
