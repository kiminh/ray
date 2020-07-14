#include <iostream>
#include <boost/dll.hpp>
#include "msgpack_codec.h"

struct client_t{
    template<typename... Args>
    msgpack::sbuffer call(std::string key, Args... args){
        return ant::msgpack_codec::pack_args(std::move(key), std::move(args)...);
    }

    void handle_response(std::string resp_str){
        try {
            ant::msgpack_codec codec;
            auto [code, result] = codec.unpack0<int>(resp_str.data(), resp_str.size());
            if(code == 0){
                std::cout<<result<<'\n';
            }
        }catch(std::exception& e){
            std::cout<<e.what()<<'\n';
        }
    }
};

struct server_t{
    server_t() : lib_("/Users/yu/Documents/myso/cmake-build-debug/libmyso.dylib"){
    }

    std::string handle_request(msgpack::sbuffer buf){
        //call function in so
        auto call_fn = lib_.get<std::string(const char*, size_t)>("call_in_so0");
        //get resutl and send to the client
        auto s = call_fn(buf.data(), buf.size());

        return s;
    }

private:
    boost::dll::shared_library lib_;
};

void plus(){}


template<typename F>
auto Test(F f, const std::string& name){
    return name;
}

template<typename F, typename U>
auto Test(F f, const std::string& name, U args){
    return name;
}

#define TEST(f, ...) \
Test(f, #f, ##__VA_ARGS__)

void test_function_name(){
    auto s = TEST([=](int a, int b){});
    auto ss = TEST([]{}, 2);
    auto s1 = TEST(plus);
}

int main() {
    //client call
    client_t client;
    auto buf = client.call("aa", 2);

    //send buf to the server
    //server handle request from a client
    server_t server;
    auto resp_str = server.handle_request(std::move(buf));

    //send response to the client
    //client handle response from the server
    client.handle_response(std::move(resp_str));

    test_function_name();

    return 0;
}
