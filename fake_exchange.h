#pragma once

#include "nse_structs.h"
#include <unordered_map>
#include <vector>
#include <memory>
#include <cstdint>
#include <functional>
#include <map>
#include <set>

// Fake NSE Exchange
class FakeNSEExchange {
public:
    FakeNSEExchange();
    ~FakeNSEExchange();
    
    // Main parsing function
    size_t parse(const uint8_t* buf, size_t buflen, uint64_t ts, bool& error);
    
    // Set the message callback for sending responses
    void set_message_callback(std::function<void(const uint8_t*, size_t)> callback);

    // Message handlers
    void handle_signon_request(const MS_SIGNON_REQUEST_IN* req, uint64_t ts);
    void handle_system_info_request(const MS_SYSTEM_INFO_REQ* req, uint64_t ts);
    void handle_update_local_database(const MS_UPDATE_LOCAL_DATABASE* req, uint64_t ts);
    void handle_exchange_portfolio_request(const EXCH_PORTFOLIO_REQ* req, uint64_t ts);
    void handle_message_download(const MS_MESSAGE_DOWNLOAD* req, uint64_t ts);
    void handle_order_entry_request(const MS_OE_REQUEST* req, uint64_t ts);
    void handle_price_modification_request(const PRICE_MOD* req, uint64_t ts);
    void handle_spread_order_entry_request(const MS_SPD_OE_REQUEST* req, uint64_t ts);
    
private:
    std::set<int32_t> logged_in_traders_;

    std::function<void(const uint8_t*, size_t)> message_callback_;

    size_t try_parse_message(const uint8_t* buf, size_t remaining, uint64_t ts, bool& error);

    void send_signon_response(const MS_SIGNON_REQUEST_IN* req, uint64_t ts, int16_t error_code);
    void send_system_info_response(const MS_SYSTEM_INFO_REQ* req, uint64_t ts, int16_t error_code);

    template<typename ResponseType>
    void send_response_with_error(const ResponseType& response, int16_t error_code) {
        ResponseType error_response = response;
        error_response.Header.ErrorCode = error_code;
        
        if (message_callback_) {
            message_callback_(reinterpret_cast<const uint8_t*>(&error_response), sizeof(error_response));
        }
    }
};
