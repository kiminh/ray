//
// Created by yu.qi on 2020/7/10.
//

#ifndef MYSO_BASE_H
#define MYSO_BASE_H

#include <map>
#include <string>
#include <boost/dll.hpp>
#include "router.h"

#define API extern "C" BOOST_SYMBOL_EXPORT

API std::map<std::string, std::function<int(int)>> g_map = {};
auto& g_router = ant::router::get();

API std::string call_in_so0(const char* data, std::size_t size){
    return g_router.route(data, size);
}

template<typename Function>
int Task(std::string const& name, Function f) {
    g_router.register_handler(name, std::move(f));

    return 0;
}

template<typename Function, typename Self>
int Task(std::string const& name, const Function& f, Self* self) {
    g_router.register_handler(name, f, self);

    return 0;
}

int main();

#define VAR() dummy_var##__FILE__##__LINE__
int VAR() = main();
#endif //MYSO_BASE_H
