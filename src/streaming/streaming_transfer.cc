//
// Created by ashione on 2019/4/1.
//

#include "streaming_transfer.h"
namespace ray {
namespace streaming {
std::queue<std::shared_ptr<StreamingMessage>> StreamingDefaultTransfer::message_store_;
}
}

