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

#include "ray/util/url.h"

#include "gtest/gtest.h"

namespace ray {

template <class T>
static std::string to_str(const T &obj, bool include_scheme) {
  return endpoint_to_url(obj, include_scheme);
}

TEST(UrlTest, UrlIpTcpParseTest) {
  ASSERT_EQ(to_str(parse_url_endpoint("tcp://[::1]:1/", 0), false), "[::1]:1");
  ASSERT_EQ(to_str(parse_url_endpoint("tcp://[::1]/", 0), false), "[::1]:0");
  ASSERT_EQ(to_str(parse_url_endpoint("tcp://[::1]:1", 0), false), "[::1]:1");
  ASSERT_EQ(to_str(parse_url_endpoint("tcp://[::1]", 0), false), "[::1]:0");
  ASSERT_EQ(to_str(parse_url_endpoint("tcp://127.0.0.1:1/", 0), false), "127.0.0.1:1");
  ASSERT_EQ(to_str(parse_url_endpoint("tcp://127.0.0.1/", 0), false), "127.0.0.1:0");
  ASSERT_EQ(to_str(parse_url_endpoint("tcp://127.0.0.1:1", 0), false), "127.0.0.1:1");
  ASSERT_EQ(to_str(parse_url_endpoint("tcp://127.0.0.1", 0), false), "127.0.0.1:0");
  ASSERT_EQ(to_str(parse_url_endpoint("[::1]:1/", 0), false), "[::1]:1");
  ASSERT_EQ(to_str(parse_url_endpoint("[::1]/", 0), false), "[::1]:0");
  ASSERT_EQ(to_str(parse_url_endpoint("[::1]:1", 0), false), "[::1]:1");
  ASSERT_EQ(to_str(parse_url_endpoint("[::1]", 0), false), "[::1]:0");
  ASSERT_EQ(to_str(parse_url_endpoint("127.0.0.1:1/", 0), false), "127.0.0.1:1");
  ASSERT_EQ(to_str(parse_url_endpoint("127.0.0.1/", 0), false), "127.0.0.1:0");
  ASSERT_EQ(to_str(parse_url_endpoint("127.0.0.1:1", 0), false), "127.0.0.1:1");
  ASSERT_EQ(to_str(parse_url_endpoint("127.0.0.1", 0), false), "127.0.0.1:0");
#ifndef _WIN32
  ASSERT_EQ(to_str(parse_url_endpoint("unix:///tmp/sock"), false), "/tmp/sock");
  ASSERT_EQ(to_str(parse_url_endpoint("/tmp/sock"), false), "/tmp/sock");
#endif
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
