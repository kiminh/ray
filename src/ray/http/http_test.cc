#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "http_client.h"
#include "http_router.h"
#include "http_server.h"
#include "ray/util/json.h"

#include <memory>
#include <thread>
#include <vector>

using tcp = boost::asio::ip::tcp;

namespace ray {
class HttpTest : public ::testing::Test {
 protected:
  void StartServer() {
    // Only start once
    static auto t = new std::thread([=] {
      boost::asio::io_context ioc;
      std::make_shared<HttpServer>(ioc)->Start(host, port);
      ioc.run();
    });
    boost::ignore_unused(t);
  }

 protected:
  int port = 8080;
  std::string host = "0.0.0.0";
};

TEST_F(HttpTest, ServerParseParams) {
  {
    StartServer();
    HttpRouter::Register(
        "/test/parse/params",
        "get with any key value, e.g. http://127.0.0.1/test/parse/params?k1=v1&k2=v2",
        [](HttpParams &&params, const std::string &data, HttpReply &r) {
          rapidjson::Document doc(rapidjson::kObjectType);
          for (auto &param : params) {
            rapidjson::Value key;
            key.Set(param.first, doc.GetAllocator());

            rapidjson::Value val;
            val.Set(param.second, doc.GetAllocator());
            doc.AddMember(key, val, doc.GetAllocator());
          }
          r.SetJsonContent(rapidjson::to_string(doc));
        });
  }

  {
    HttpSyncClient client;
    client.Connect(host, port);

    std::unordered_map<std::string, std::string> expected{{"k1", "v1"}, {"k2", "v2"}};

    rapidjson::Document doc;
    doc.Parse(client.Get("/test/parse/params", expected));
    auto obj = doc.GetObject();

    ASSERT_EQ(expected.size(), obj.MemberCount());
    for (auto &item : obj) {
      ASSERT_TRUE(expected.count(item.name.GetString()));
      ASSERT_EQ(expected[item.name.GetString()], item.value.GetString());
    }
  }

  {
    boost::asio::io_context ioc;
    auto client = std::make_shared<HttpAsyncClient>(ioc);
    client->Connect(host, port);

    std::unordered_map<std::string, std::string> expected{{"k1", "v1"}, {"k2", "v2"}};
    std::vector<bool> finish(10, false);

    for (auto i = 0; i < finish.size(); ++i) {
      client->Get(
          "/test/parse/params", expected,
          [&expected, &finish, i](boost::system::error_code ec, const std::string &s) {
            if (ec) {
              throw ec.message();
            }
            rapidjson::Document doc;
            doc.Parse(s);
            auto obj = doc.GetObject();

            ASSERT_EQ(expected.size(), obj.MemberCount());
            for (auto &item : obj) {
              ASSERT_TRUE(expected.count(item.name.GetString()));
              ASSERT_EQ(expected[item.name.GetString()], item.value.GetString());
            }
            finish[i] = true;
          });
    }

    while (true) {
      ioc.run_one();
      if (std::all_of(finish.begin(), finish.end(), [](bool v) { return v; })) break;
    }
  }
}

TEST_F(HttpTest, ServerParseData) {
  {
    StartServer();
    HttpRouter::Register("/test/parse/data", "post any json string",
                         [](HttpParams &&params, const std::string &data, HttpReply &r) {
                           rapidjson::Document doc;
                           doc.Parse(data);
                           r.SetJsonContent(rapidjson::to_string(doc));
                         });
  }

  {
    HttpSyncClient client;
    client.Connect(host, port);

    std::string json_data = R"({"name":"ray","test":"post"})";

    rapidjson::Document doc;
    auto resp = client.Post("/test/parse/data", {}, std::move(json_data));
    doc.Parse(resp);

    ASSERT_TRUE(doc.IsObject());
    ASSERT_EQ("ray", std::string(doc["name"].GetString()));
    ASSERT_EQ("post", std::string(doc["test"].GetString()));
  }

  {
    boost::asio::io_context ioc;
    auto client = std::make_shared<HttpAsyncClient>(ioc);
    client->Connect(host, port);

    std::vector<bool> finish(10, false);

    for (auto i = 0; i < finish.size(); ++i) {
      client->Post("/test/parse/data", {}, R"({"name":"ray","test":"post"})",
                   [&finish, i](boost::system::error_code ec, const std::string &s) {
                     if (ec) {
                       throw ec.message();
                     }
                     rapidjson::Document doc;
                     doc.Parse(s);

                     ASSERT_TRUE(doc.IsObject());
                     ASSERT_EQ("ray", std::string(doc["name"].GetString()));
                     ASSERT_EQ("post", std::string(doc["test"].GetString()));
                     finish[i] = true;
                   });
    }

    while (true) {
      ioc.run_one();
      if (std::all_of(finish.begin(), finish.end(), [](bool v) { return v; })) break;
    }
  }
}
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}