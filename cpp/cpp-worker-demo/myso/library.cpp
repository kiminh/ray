#include <iostream>
#include <functional>
#include <boost/function.hpp>
#include "base.h"

class  plugin_t{
public:
    int test(int a){return a+2;}
};

int plus1(int a){ return a + 2; }

void test_router(){
    using namespace ant;
    auto& router = router::get();
    router.register_handler("a", [](int a){ return a + 2; });
    router.register_handler("b", [](int a, int b){return a+b;});
    router.register_handler("c", [](std::string s){ return s;});
    router.register_handler("d", plus1);
    router.register_handler("e", []{}); //void function

    plugin_t plugin;
    router.register_handler("f", &plugin_t::test, &plugin); //member function

    {
        auto buf = msgpack_codec::pack_args("a", 2);
        auto s = router.route(buf.data(), buf.size());

        msgpack_codec codec;
        auto [code, result] = codec.unpack0<int>(s.data(), s.size());
        if(code == result_code::OK){
            std::cout<<result<<'\n';
        }
    }

    {
        auto buf = msgpack_codec::pack_args("b", 2, 3);
        auto s = router.route(buf.data(), buf.size());

        msgpack_codec codec;
        auto [code, result] = codec.unpack0<int>(s.data(), s.size());
        if(code == result_code::OK){
            std::cout<<result<<'\n';
        }
    }

    {
        auto buf = msgpack_codec::pack_args("c", "hello");
        auto s = router.route(buf.data(), buf.size());

        msgpack_codec codec;
        auto [code, result] = codec.unpack0<std::string>(s.data(), s.size());
        if(code == result_code::OK){
            std::cout<<result<<'\n';
        }
    }

    {
        auto buf = msgpack_codec::pack_args("d", 4);
        auto s = router.route(buf.data(), buf.size());

        msgpack_codec codec;
        auto [code, result] = codec.unpack0<int>(s.data(), s.size());
        if(code == result_code::OK){
            std::cout<<result<<'\n';
        }
    }

    {
        auto buf = msgpack_codec::pack_args("e");
        auto s = router.route(buf.data(), buf.size());

        msgpack_codec codec;
        auto [code] = codec.unpack0<void>(s.data(), s.size());
        if(code == result_code::OK){
            std::cout<<"void resut ok"<<'\n';
        }
    }

    {
        auto buf = msgpack_codec::pack_args("f", 2);
        auto s = router.route(buf.data(), buf.size());

        msgpack_codec codec;
        auto [code, result] = codec.unpack0<int>(s.data(), s.size());
        if(code == result_code::OK){
            std::cout<<result<<'\n';
        }
    }

    //test exception
    {
        auto buf = msgpack_codec::pack_args("a", "invalid argument");
        auto s = router.route(buf.data(), buf.size());

        try {
            msgpack_codec codec;
            auto [code, result] = codec.unpack0<int>(s.data(), s.size());
            if(code == result_code::OK){
                std::cout<<result<<'\n';
            }
        }catch(std::exception& e){
            std::cout<<e.what()<<'\n';
        }
    }
}

int test(int a){
    return a+2;
}

int main(){
    Task("aa", plus1);
    Task("bb", test);

    //lambda
    Task("cc", [](int a){
        return a + 2;
    });

    //std::function
    std::function<int(int)> function = [](int a){ return a; };
    Task("dd", function);

    //member function
    plugin_t plugin;
    Task("dd", &plugin_t::test, &plugin);

    return 0;
}