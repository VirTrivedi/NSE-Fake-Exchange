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

    // Market status management
    void set_markets_opening(bool opening) { markets_are_opening_ = opening; }
    void set_market_status(bool normal_open, bool oddlot_open, bool spot_open, bool auction_open);
    void get_current_market_status(ST_MARKET_STATUS& status, ST_EX_MARKET_STATUS& ex_status, ST_PL_MARKET_STATUS& pl_status) const;

    // Message handlers
    void handle_signon_request(const MS_SIGNON_REQUEST_IN* req, uint64_t ts);
    void handle_signoff_request(const MS_SIGNOFF* req, uint64_t ts);
    void handle_system_info_request(const MS_SYSTEM_INFO_REQ* req, uint64_t ts);
    void handle_update_local_database(const MS_UPDATE_LOCAL_DATABASE* req, uint64_t ts);
    void handle_exchange_portfolio_request(const EXCH_PORTFOLIO_REQ* req, uint64_t ts);
    void handle_message_download(const MS_MESSAGE_DOWNLOAD* req, uint64_t ts);
    void handle_order_entry_request(const MS_OE_REQUEST* req, uint64_t ts);
    void handle_price_modification_request(const PRICE_MOD* req, uint64_t ts);
    void handle_spread_order_entry_request(const MS_SPD_OE_REQUEST* req, uint64_t ts);
    
private:
    std::set<int32_t> logged_in_traders_;
    std::map<int32_t, int32_t> trader_last_logoff_time_;

    std::function<void(const uint8_t*, size_t)> message_callback_;

    ST_MARKET_STATUS current_market_status_;
    ST_EX_MARKET_STATUS current_ex_market_status_;
    ST_PL_MARKET_STATUS current_pl_market_status_;
    bool markets_are_opening_;

    size_t try_parse_message(const uint8_t* buf, size_t remaining, uint64_t ts, bool& error);

    void send_signon_response(const MS_SIGNON_REQUEST_IN* req, uint64_t ts, int16_t error_code);
    void send_signoff_response(const MS_SIGNOFF* req, uint64_t ts, int16_t error_code);
    void send_system_info_response(const MS_SYSTEM_INFO_REQ* req, uint64_t ts, int16_t error_code);
    void send_partial_system_info_for_ldb_request(const MS_UPDATE_LOCAL_DATABASE* req, uint64_t ts);
    void send_update_local_database_response(const MS_UPDATE_LOCAL_DATABASE* req, uint64_t ts, int16_t error_code);
    void send_exchange_portfolio_response(const EXCH_PORTFOLIO_REQ* req, uint64_t ts, int16_t error_code);
    void send_message_download_response(const MS_MESSAGE_DOWNLOAD* req, uint64_t ts, int16_t error_code);

    bool validate_trader_market_status(const MS_UPDATE_LOCAL_DATABASE* req);
};
