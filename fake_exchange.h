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

    // Broker management
    void set_broker_closeout_status(const std::string& broker_id, bool is_closeout);
    void set_broker_deactivated_status(const std::string& broker_id, bool is_deactivated);
    void set_broker_type(const std::string& broker_id, char broker_type);

    // Message handlers
    void handle_signon_request(const MS_SIGNON_REQUEST_IN* req, uint64_t ts);
    void handle_signoff_request(const MS_SIGNOFF* req, uint64_t ts);
    void handle_system_info_request(const MS_SYSTEM_INFO_REQ* req, uint64_t ts);
    void handle_update_local_database(const MS_UPDATE_LOCAL_DATABASE* req, uint64_t ts);
    void handle_exchange_portfolio_request(const EXCH_PORTFOLIO_REQ* req, uint64_t ts);
    void handle_message_download(const MS_MESSAGE_DOWNLOAD* req, uint64_t ts);
    void handle_order_entry_request(const MS_OE_REQUEST* req, uint64_t ts);
    void handle_price_modification_request(const PRICE_MOD* req, uint64_t ts);
    void handle_order_cancellation_request(const MS_OE_REQUEST* req, uint64_t ts);
    void handle_kill_switch_request(const MS_OE_REQUEST* req, uint64_t ts);
    void handle_spread_order_entry_request(const MS_SPD_OE_REQUEST* req, uint64_t ts);
    void handle_spread_order_modification_request(const MS_SPD_OE_REQUEST* req, uint64_t ts);
    void handle_spread_order_cancellation_request(const MS_SPD_OE_REQUEST* req, uint64_t ts);
    void handle_trade_modification_request(const MS_TRADE_INQ_DATA* req, uint64_t ts);
    void handle_trade_cancellation_request(const MS_TRADE_INQ_DATA* req, uint64_t ts);
    
    // Spread combination master broadcasts
    void broadcast_spread_combination_update(const MS_SPD_UPDATE_INFO& update_info, uint64_t ts);
    void broadcast_periodic_spread_combination_update(const MS_SPD_UPDATE_INFO& update_info, uint64_t ts);
    
    
private:
    std::set<int32_t> logged_in_traders_;
    std::map<int32_t, int32_t> trader_last_logoff_time_;

    std::function<void(const uint8_t*, size_t)> message_callback_;

    std::map<std::string, bool> broker_closeout_status_;
    std::map<std::string, bool> broker_deactivated_status_;
    std::map<std::string, char> broker_types_;

    std::map<double, MS_OE_REQUEST> active_orders_;
    std::map<double, MS_SPD_OE_REQUEST> active_spread_orders_;
    std::map<std::pair<int32_t, int32_t>, MS_SPD_UPDATE_INFO> spread_combinations_;

    std::map<int32_t, MS_TRADE_INQ_DATA> executed_trades_;
    std::set<std::string> trade_modification_requests_;
    std::set<std::string> trade_cancellation_requests_;

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
    void send_order_response(const MS_OE_REQUEST* req, uint64_t ts, int16_t transaction_code, int16_t error_code, int16_t reason_code = ReasonCodes::NORMAL_CONFIRMATION);
    void send_modification_response(const PRICE_MOD* req, uint64_t ts, int16_t transaction_code, int16_t error_code);
    void send_cancellation_response(const MS_OE_REQUEST* req, uint64_t ts, int16_t transaction_code, int16_t error_code);
    void send_kill_switch_response(const MS_OE_REQUEST* req, uint64_t ts, int16_t error_code, int32_t cancelled_count = 0);
    void send_trade_modification_response(const MS_TRADE_INQ_DATA* req, uint64_t ts, int16_t transaction_code, int16_t error_code);
    void send_trade_cancellation_response(const MS_TRADE_INQ_DATA* req, uint64_t ts, int16_t transaction_code, int16_t error_code);
    void send_spread_order_response(const MS_SPD_OE_REQUEST* req, uint64_t ts, int16_t transaction_code, int16_t error_code, int16_t reason_code = ReasonCodes::NORMAL_CONFIRMATION);

    // Helper methods
    bool validate_trader_market_status(const MS_UPDATE_LOCAL_DATABASE* req);
    bool is_broker_in_closeout(const std::string& broker_id) const;
    bool is_valid_closeout_order(const MS_OE_REQUEST* req) const;
    double generate_order_number(uint64_t ts);
    bool is_valid_modification(const MS_OE_REQUEST& original_order, const PRICE_MOD* modification) const;
    bool is_time_priority_lost(const MS_OE_REQUEST* original_order, const PRICE_MOD* modification) const;
    uint64_t generate_activity_reference(uint64_t ts);
    void process_successful_modification(MS_OE_REQUEST& original_order, const PRICE_MOD* req, uint64_t ts);
    bool is_broker_deactivated(const std::string& broker_id) const;
    bool can_cancel_order(const std::string& canceller_broker_id, const std::string& order_broker_id) const;
    bool is_valid_activity_reference(const MS_OE_REQUEST* order, const MS_OE_REQUEST* cancel_req) const;
    void process_successful_cancellation(MS_OE_REQUEST& original_order, const MS_OE_REQUEST* cancel_req, uint64_t ts);
    int32_t process_kill_switch_cancellation(const MS_OE_REQUEST* req, uint64_t ts);
    bool is_contract_match(const MS_OE_REQUEST* order, const CONTRACT_DESC* contract) const;
    bool is_valid_pro_order(int16_t pro_client_indicator, const std::string& account_number, const std::string& broker_id) const;
    bool is_valid_cli_order(int16_t pro_client_indicator, const std::string& account_number, const std::string& broker_id) const;
    std::string generate_trade_request_key(int32_t fill_number, int32_t trader_id, const std::string& operation);
    bool is_duplicate_trade_request(int32_t fill_number, int32_t trader_id, const std::string& operation);
    void mark_trade_request(int32_t fill_number, int32_t trader_id, const std::string& operation);
    bool is_trade_owner(const MS_TRADE_INQ_DATA& trade, int32_t trader_id, const std::string& broker_id);
    bool is_valid_spread_modification(const MS_SPD_OE_REQUEST& original_order, const MS_SPD_OE_REQUEST* modification) const;
    bool is_valid_spread_activity_reference(const MS_SPD_OE_REQUEST* order, const MS_SPD_OE_REQUEST* modify_req) const;
    void process_successful_spread_modification(MS_SPD_OE_REQUEST& original_order, const MS_SPD_OE_REQUEST* req, uint64_t ts);
    void add_spread_combination(int32_t token1, int32_t token2, const MS_SPD_UPDATE_INFO& combination_info);
    void update_spread_combination(int32_t token1, int32_t token2, const MS_SPD_UPDATE_INFO& updated_info, uint64_t ts);
    bool is_valid_spread_combination(int32_t token1, int32_t token2) const;
};
