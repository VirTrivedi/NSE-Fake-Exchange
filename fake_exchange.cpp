#include "fake_exchange.h"
#include <algorithm>
#include <cstring>
#include <chrono>
#include <iostream>
#include <map>

// FakeNSEExchange Implementation
FakeNSEExchange::FakeNSEExchange() {
    memset(&current_market_status_, 0, sizeof(current_market_status_));
    memset(&current_ex_market_status_, 0, sizeof(current_ex_market_status_));
    memset(&current_pl_market_status_, 0, sizeof(current_pl_market_status_));
    markets_are_opening_ = false;
}

// FakeNSEExchange Destructor
FakeNSEExchange::~FakeNSEExchange() = default;

// Set the message callback for sending responses
void FakeNSEExchange::set_message_callback(std::function<void(const uint8_t*, size_t)> callback) {
    message_callback_ = callback;
}

// Set the market status based on the provided parameters
void FakeNSEExchange::set_market_status(bool normal_open, bool oddlot_open, bool spot_open, bool auction_open) {
    current_market_status_.Normal = normal_open ? 1 : 0;
    current_market_status_.Oddlot = oddlot_open ? 1 : 0;
    current_market_status_.Spot = spot_open ? 1 : 0;
    current_market_status_.Auction = auction_open ? 1 : 0;
    
    // Mirror to other market status structures
    current_ex_market_status_.Normal = current_market_status_.Normal;
    current_ex_market_status_.Oddlot = current_market_status_.Oddlot;
    current_ex_market_status_.Spot = current_market_status_.Spot;
    current_ex_market_status_.Auction = current_market_status_.Auction;
    
    current_pl_market_status_.Normal = current_market_status_.Normal;
    current_pl_market_status_.Oddlot = current_market_status_.Oddlot;
    current_pl_market_status_.Spot = current_market_status_.Spot;
    current_pl_market_status_.Auction = current_market_status_.Auction;
    
    std::cout << "Exchange internal market status updated - Normal: " << current_market_status_.Normal 
              << ", Oddlot: " << current_market_status_.Oddlot 
              << ", Spot: " << current_market_status_.Spot 
              << ", Auction: " << current_market_status_.Auction << std::endl;
}

// Get the current market status
void FakeNSEExchange::get_current_market_status(ST_MARKET_STATUS& status, ST_EX_MARKET_STATUS& ex_status, ST_PL_MARKET_STATUS& pl_status) const {
    status = current_market_status_;
    ex_status = current_ex_market_status_;
    pl_status = current_pl_market_status_;
}

// Check if a broker is in closeout status
bool FakeNSEExchange::is_broker_in_closeout(const std::string& broker_id) const {
    auto it = broker_closeout_status_.find(broker_id);
    return (it != broker_closeout_status_.end() && it->second);
}

// Validate if the order is a closeout order based on broker status and order type
bool FakeNSEExchange::is_valid_closeout_order(const MS_OE_REQUEST* req) const {    
    bool is_normal_market = (current_market_status_.Normal == 1);
    bool is_regular_book = (req->BookType == 1);
    bool is_ioc_order = (req->OrderFlags.IOC == 1);
    
    return is_normal_market && is_regular_book && is_ioc_order;
}

// Generate a unique order number based on timestamp and sequence
double FakeNSEExchange::generate_order_number(uint64_t ts) {
    static uint64_t sequence_counter = 1;
    
    uint64_t stream_part = 1;
    uint64_t sequence_part = (ts % 100000000000000ULL) + sequence_counter++;
    double order_number = stream_part * 100000000000000.0 + sequence_part;
    
    return order_number;
}

// Set the closeout status for a broker
void FakeNSEExchange::set_broker_closeout_status(const std::string& broker_id, bool is_closeout) {
    broker_closeout_status_[broker_id] = is_closeout;
    std::cout << "Set broker " << broker_id << " closeout status to: " << (is_closeout ? "TRUE" : "FALSE") << std::endl;
}

// Check if the order will lose time priority based on modification rules
bool FakeNSEExchange::is_time_priority_lost(const MS_OE_REQUEST* original_order, const PRICE_MOD* modification) const {
    /*
        * According to NSE rules, order loses time priority if:
        * 1. Price is changed
        * 2. Quantity is increased
        * 3. For ATO or Market orders, any quantity change loses priority
    */
    if (original_order->Price != modification->Price) {
        return true;
    }
    if (modification->Volume > original_order->Volume) {
        return true;
    }
    if (original_order->OrderFlags.ATO || original_order->OrderFlags.Market) {
        if (modification->Volume != original_order->Volume) {
            return true;
        }
    }
    return false;
}

// Validate if the modification request is valid based on the original order
bool FakeNSEExchange::is_valid_modification(const MS_OE_REQUEST& original_order, const PRICE_MOD* modification) const {
    if (modification->Volume <= 0) {
        return false;
    }
    if (modification->Price <= 0 && !original_order.OrderFlags.Market) {
        return false;
    }
    return true;
}

// Generate a unique activity reference based on timestamp
uint64_t FakeNSEExchange::generate_activity_reference(uint64_t ts) {
    static uint64_t reference_counter = 1;
    return ts + reference_counter++;
}

// Check if a broker is deactivated
bool FakeNSEExchange::is_broker_deactivated(const std::string& broker_id) const {
    auto it = broker_deactivated_status_.find(broker_id);
    return (it != broker_deactivated_status_.end() && it->second);
}

// Check if a broker can cancel an order based on hierarchy rules
bool FakeNSEExchange::can_cancel_order(const std::string& canceller_broker_id, const std::string& order_broker_id) const {
    // Same broker can always cancel their own orders
    if (canceller_broker_id == order_broker_id) {
        return true;
    }
    
    // Get broker types
    auto canceller_type_it = broker_types_.find(canceller_broker_id);
    auto order_type_it = broker_types_.find(order_broker_id);
    
    // If broker types not set, assume same level
    if (canceller_type_it == broker_types_.end() || order_type_it == broker_types_.end()) {
        return true;
    }
    
    char canceller_type = canceller_type_it->second;
    char order_type = order_type_it->second;
    
    // CM > BM > DL hierarchy
    switch (canceller_type) {
        case BrokerTypes::CORPORATE_MANAGER:
            return true;
            
        case BrokerTypes::BRANCH_MANAGER:
            return (order_type == BrokerTypes::DEALER);
            
        case BrokerTypes::DEALER:
            return false;
            
        default:
            return true;
    }
}

bool FakeNSEExchange::is_valid_activity_reference(const MS_OE_REQUEST* order, const MS_OE_REQUEST* cancel_req) const {
    // Check if the LastActivityReference in cancellation request matches the order's reference
    return (cancel_req->LastActivityReference == order->LastActivityReference);
}

void FakeNSEExchange::set_broker_deactivated_status(const std::string& broker_id, bool is_deactivated) {
    broker_deactivated_status_[broker_id] = is_deactivated;
    std::cout << "Set broker " << broker_id << " deactivated status to: " << (is_deactivated ? "TRUE" : "FALSE") << std::endl;
}

void FakeNSEExchange::set_broker_type(const std::string& broker_id, char broker_type) {
    broker_types_[broker_id] = broker_type;
    std::string type_name;
    switch (broker_type) {
        case BrokerTypes::CORPORATE_MANAGER: type_name = "Clearing Member (CM)"; break;
        case BrokerTypes::BRANCH_MANAGER: type_name = "Broker Member (BM)"; break;
        case BrokerTypes::DEALER: type_name = "Dealer (DL)"; break;
        default: type_name = "Unknown"; break;
    }
    std::cout << "Set broker " << broker_id << " type to: " << type_name << std::endl;
}

// Check if the order matches the specified contract details
bool FakeNSEExchange::is_contract_match(const MS_OE_REQUEST* order, const CONTRACT_DESC* contract) const {
    // Compare contract details
    if (strncmp(order->ContractDesc.Symbol, contract->Symbol, sizeof(contract->Symbol)) != 0) {
        return false;
    }
    if (contract->InstrumentName[0] != '\0' && 
        strncmp(order->ContractDesc.InstrumentName, contract->InstrumentName, sizeof(contract->InstrumentName)) != 0) {
        return false;
    }
    if (contract->ExpiryDate != 0 && order->ContractDesc.ExpiryDate != contract->ExpiryDate) {
        return false;
    }
    if (contract->StrikePrice != 0 && order->ContractDesc.StrikePrice != contract->StrikePrice) {
        return false;
    }
    if (contract->OptionType[0] != '\0' &&
        strncmp(order->ContractDesc.OptionType, contract->OptionType, sizeof(contract->OptionType)) != 0) {
        return false;
    }
    return true;
}

std::string FakeNSEExchange::generate_trade_request_key(int32_t fill_number, int32_t trader_id, const std::string& operation) {
    return operation + "_" + std::to_string(fill_number) + "_" + std::to_string(trader_id);
}

bool FakeNSEExchange::is_duplicate_trade_request(int32_t fill_number, int32_t trader_id, const std::string& operation) {
    std::string key = generate_trade_request_key(fill_number, trader_id, operation);
    
    if (operation == "modify") {
        return trade_modification_requests_.find(key) != trade_modification_requests_.end();
    } else if (operation == "cancel") {
        return trade_cancellation_requests_.find(key) != trade_cancellation_requests_.end();
    }
    
    return false;
}

void FakeNSEExchange::mark_trade_request(int32_t fill_number, int32_t trader_id, const std::string& operation) {
    std::string key = generate_trade_request_key(fill_number, trader_id, operation);
    
    if (operation == "modify") {
        trade_modification_requests_.insert(key);
    } else if (operation == "cancel") {
        trade_cancellation_requests_.insert(key);
    }
}

bool FakeNSEExchange::is_trade_owner(const MS_TRADE_INQ_DATA& trade, int32_t trader_id, const std::string& broker_id) {
    // Check if trader ID matches (as per documentation)
    if (trade.TraderId == trader_id) {
        return true;
    }
    
    // Check if broker ID matches (for hierarchy as per documentation)
    std::string trade_buy_broker(trade.BuyBrokerId, 5);
    std::string trade_sell_broker(trade.SellBrokerId, 5);
    trade_buy_broker.erase(trade_buy_broker.find_last_not_of(' ') + 1);
    trade_sell_broker.erase(trade_sell_broker.find_last_not_of(' ') + 1);
    
    return (broker_id == trade_buy_broker || broker_id == trade_sell_broker);
}

bool FakeNSEExchange::is_valid_pro_order(int16_t pro_client_indicator, const std::string& account_number, const std::string& broker_id) const {
    if (pro_client_indicator != 2) { // Not a PRO order
        return true;
    }
    
    // For PRO orders, account number should be empty or same as broker ID
    return (account_number.empty() || account_number == broker_id);
}

bool FakeNSEExchange::is_valid_cli_order(int16_t pro_client_indicator, const std::string& account_number, const std::string& broker_id) const {
    if (pro_client_indicator != 1) { // Not a CLI order
        return true;
    }
    
    // For CLI orders, account number cannot be the broker ID
    return (!account_number.empty() && account_number != broker_id);
}

bool FakeNSEExchange::is_valid_trade_modification(const MS_TRADE_INQ_DATA* req) const {
    // Basic validation of trade modification request
    
    // FillNumber must be positive
    if (req->FillNumber <= 0) {
        std::cout << "Invalid FillNumber: " << req->FillNumber << std::endl;
        return false;
    }
    
    // FillQuantity must be positive
    if (req->FillQuantity <= 0) {
        std::cout << "Invalid FillQuantity: " << req->FillQuantity << std::endl;
        return false;
    }
    
    // FillPrice must be positive
    if (req->FillPrice <= 0) {
        std::cout << "Invalid FillPrice: " << req->FillPrice << std::endl;
        return false;
    }
    
    // TokenNo must be valid
    if (req->TokenNo <= 0) {
        std::cout << "Invalid TokenNo: " << req->TokenNo << std::endl;
        return false;
    }
    
    // MktType must be valid (1-4)
    if (req->MktType < '1' || req->MktType > '4') {
        std::cout << "Invalid MktType: " << req->MktType << std::endl;
        return false;
    }
    
    // Validate Open/Close indicators
    if (req->BuyOpenClose != 'O' && req->BuyOpenClose != 'C') {
        std::cout << "Invalid BuyOpenClose: " << req->BuyOpenClose << std::endl;
        return false;
    }
    
    if (req->SellOpenClose != 'O' && req->SellOpenClose != 'C') {
        std::cout << "Invalid SellOpenClose: " << req->SellOpenClose << std::endl;
        return false;
    }
    
    return true;
}

// Parses the incoming buffer and dispatches messages to appropriate handlers
size_t FakeNSEExchange::parse(const uint8_t* buf, size_t buflen, uint64_t ts, bool& error) {
    error = false;
    size_t total_seen = 0;
    
    while (total_seen < buflen) {
        size_t remaining = buflen - total_seen;
        
        size_t seen = try_parse_message(buf + total_seen, remaining, ts, error);
        
        if (seen == 0) {
            break;
        }
        
        if (error) {
            break;
        }
        
        total_seen += seen;
    }
    
    return total_seen;
}

// Try to parse a single message from the buffer
size_t FakeNSEExchange::try_parse_message(const uint8_t* buf, size_t remaining, uint64_t ts, bool& error) {
    error = false;
    
    if (remaining < sizeof(int16_t)) {
        return 0;
    }
    
    const int16_t* transaction_code_ptr = reinterpret_cast<const int16_t*>(buf);
    int16_t transaction_code = *transaction_code_ptr;
    
    if (transaction_code == TransactionCodes::ORDER_ENTRY_REQUEST_TR ||
        transaction_code == TransactionCodes::ORDER_MODIFY_REQUEST_TR) {
        
        return 0; // TR messages will be implemented later
    }
    
    if (remaining < sizeof(MESSAGE_HEADER)) {
        return 0;
    }
    
    const MESSAGE_HEADER* header = reinterpret_cast<const MESSAGE_HEADER*>(buf);
    
    if (header->MessageLength < sizeof(MESSAGE_HEADER) || 
        header->MessageLength > remaining) {
        return 0;
    }
    
    switch (header->TransactionCode) {
        case TransactionCodes::SIGNON_REQUEST_IN: {
            if (header->MessageLength < sizeof(MS_SIGNON_REQUEST_IN)) {
                error = true;
                return 0;
            }
            const MS_SIGNON_REQUEST_IN* req = reinterpret_cast<const MS_SIGNON_REQUEST_IN*>(buf);
            handle_signon_request(req, ts);
            break;
        }

        case TransactionCodes::SIGN_OFF_REQUEST_IN: {
            if (header->MessageLength < sizeof(MS_SIGNOFF)) {
                error = true;
                return 0;
            }
            const MS_SIGNOFF* req = reinterpret_cast<const MS_SIGNOFF*>(buf);
            handle_signoff_request(req, ts);
            break;
        }

        case TransactionCodes::SYSTEM_INFO_REQUEST: {
            if (header->MessageLength < sizeof(MS_SYSTEM_INFO_REQ)) {
                error = true;
                return 0;
            }
            const MS_SYSTEM_INFO_REQ* req = reinterpret_cast<const MS_SYSTEM_INFO_REQ*>(buf);
            handle_system_info_request(req, ts);
            break;
        }
        
        case TransactionCodes::UPDATE_LOCAL_DATABASE: {
            if (header->MessageLength < sizeof(MS_UPDATE_LOCAL_DATABASE)) {
                error = true;
                return 0;
            }
            const MS_UPDATE_LOCAL_DATABASE* req = reinterpret_cast<const MS_UPDATE_LOCAL_DATABASE*>(buf);
            handle_update_local_database(req, ts);
            break;
        }
        
        case TransactionCodes::EXCHANGE_PORTFOLIO_REQUEST: {
            if (header->MessageLength < sizeof(EXCH_PORTFOLIO_REQ)) {
                error = true;
                return 0;
            }
            const EXCH_PORTFOLIO_REQ* req = reinterpret_cast<const EXCH_PORTFOLIO_REQ*>(buf);
            handle_exchange_portfolio_request(req, ts);
            break;
        }
        
        case TransactionCodes::MESSAGE_DOWNLOAD: {
            if (header->MessageLength < sizeof(MS_MESSAGE_DOWNLOAD)) {
                error = true;
                return 0;
            }
            const MS_MESSAGE_DOWNLOAD* req = reinterpret_cast<const MS_MESSAGE_DOWNLOAD*>(buf);
            handle_message_download(req, ts);
            break;
        }
        
        case TransactionCodes::ORDER_ENTRY_REQUEST: {
            if (header->MessageLength < sizeof(MS_OE_REQUEST)) {
                error = true;
                return 0;
            }
            const MS_OE_REQUEST* req = reinterpret_cast<const MS_OE_REQUEST*>(buf);
            handle_order_entry_request(req, ts);
            break;
        }
        
        case TransactionCodes::PRICE_MODIFICATION_REQUEST: {
            if (header->MessageLength < sizeof(PRICE_MOD)) {
                error = true;
                return 0;
            }
            const PRICE_MOD* req = reinterpret_cast<const PRICE_MOD*>(buf);
            handle_price_modification_request(req, ts);
            break;
        }

        case TransactionCodes::ORDER_CANCEL_IN: {
            if (header->MessageLength < sizeof(MS_OE_REQUEST)) {
                error = true;
                return 0;
            }
            const MS_OE_REQUEST* req = reinterpret_cast<const MS_OE_REQUEST*>(buf);
            handle_order_cancellation_request(req, ts);
            break;
        }
        
        case TransactionCodes::KILL_SWITCH_IN: {
            if (header->MessageLength < sizeof(MS_OE_REQUEST)) {
                error = true;
                return 0;
            }
            const MS_OE_REQUEST* req = reinterpret_cast<const MS_OE_REQUEST*>(buf);
            handle_kill_switch_request(req, ts);
            break;
        }

        case TransactionCodes::TRADE_MOD_IN: {
            if (header->MessageLength < sizeof(MS_TRADE_INQ_DATA)) {
                error = true;
                return 0;
            }
            const MS_TRADE_INQ_DATA* req = reinterpret_cast<const MS_TRADE_INQ_DATA*>(buf);
            handle_trade_modification_request(req, ts);
            break;
        }

        case TransactionCodes::TRADE_CANCEL_IN: {
            if (header->MessageLength < sizeof(MS_TRADE_INQ_DATA)) {
                error = true;
                return 0;
            }
            const MS_TRADE_INQ_DATA* req = reinterpret_cast<const MS_TRADE_INQ_DATA*>(buf);
            handle_trade_cancellation_request(req, ts);
            break;
        }

        case TransactionCodes::SP_BOARD_LOT_IN: {
            if (header->MessageLength < sizeof(MS_SPD_OE_REQUEST)) {
                error = true;
                return 0;
            }
            const MS_SPD_OE_REQUEST* req = reinterpret_cast<const MS_SPD_OE_REQUEST*>(buf);
            handle_spread_order_entry_request(req, ts);
            break;
        }

        case TransactionCodes::SP_ORDER_MOD_IN: {
            if (header->MessageLength < sizeof(MS_SPD_OE_REQUEST)) {
                error = true;
                return 0;
            }
            const MS_SPD_OE_REQUEST* req = reinterpret_cast<const MS_SPD_OE_REQUEST*>(buf);
            handle_spread_order_modification_request(req, ts);
            break;
        }

        case TransactionCodes::SP_ORDER_CANCEL_IN: {
            if (header->MessageLength < sizeof(MS_SPD_OE_REQUEST)) {
                error = true;
                return 0;
            }
            const MS_SPD_OE_REQUEST* req = reinterpret_cast<const MS_SPD_OE_REQUEST*>(buf);
            handle_spread_order_cancellation_request(req, ts);
            break;
        }

        case TransactionCodes::TWOL_BOARD_LOT_IN:
        case TransactionCodes::TXN_EXT_TWOL_BOARD_LOT_ACK_IN: {
            if (header->MessageLength < sizeof(MS_SPD_OE_REQUEST)) {
                error = true;
                return 0;
            }
            const MS_SPD_OE_REQUEST* req = reinterpret_cast<const MS_SPD_OE_REQUEST*>(buf);
            handle_2l_order_entry_request(req, ts);
            break;
        }

        case TransactionCodes::THRL_BOARD_LOT_IN:
        case TransactionCodes::TXN_EXT_THRL_BOARD_LOT_ACK_IN: {
            if (header->MessageLength < sizeof(MS_SPD_OE_REQUEST)) {
                error = true;
                return 0;
            }
            const MS_SPD_OE_REQUEST* req = reinterpret_cast<const MS_SPD_OE_REQUEST*>(buf);
            handle_3l_order_entry_request(req, ts);
            break;
        }

        default:
            std::cout << "Unknown transaction code: " << header->TransactionCode << std::endl;
            break;
    }
    
    return header->MessageLength;
} 

void FakeNSEExchange::handle_signon_request(const MS_SIGNON_REQUEST_IN* req, uint64_t ts) {
    std::cout << "Sign-on request from trader: " << req->Header.TraderId 
              << ", UserID: " << req->UserID 
              << ", BrokerID: " << std::string(req->BrokerID, 5) << std::endl;
    
    // Check if this trader had a previous logoff
    auto logoff_iter = trader_last_logoff_time_.find(req->Header.TraderId);
    if (logoff_iter != trader_last_logoff_time_.end()) {
        std::cout << "Trader " << req->Header.TraderId 
                  << " had previous logoff at time: " << logoff_iter->second 
                  << " - sending logoff confirmation" << std::endl;
        
        // Send logoff confirmation first
        SIGNOFF_OUT logoff_confirmation;
        memset(&logoff_confirmation, 0, sizeof(logoff_confirmation));
        
        logoff_confirmation.Header = req->Header;
        logoff_confirmation.Header.TransactionCode = TransactionCodes::SIGN_OFF_REQUEST_OUT;
        logoff_confirmation.Header.ErrorCode = ErrorCodes::SUCCESS;
        logoff_confirmation.Header.MessageLength = sizeof(SIGNOFF_OUT);
        logoff_confirmation.UserId = req->Header.TraderId;
         
        if (message_callback_) {
            message_callback_(reinterpret_cast<const uint8_t*>(&logoff_confirmation), sizeof(logoff_confirmation));
        }
        
        // Clear the stored logoff time
        trader_last_logoff_time_.erase(logoff_iter);
    }
    
    // For now, make every sign-on request successful
    bool sign_on_successful = true;
    
    if (sign_on_successful) {
        logged_in_traders_.insert(req->Header.TraderId);
        
        // Send successful sign-on response
        send_signon_response(req, ts, ErrorCodes::SUCCESS);
    } else {
        // Send error sign-on response
        send_signon_response(req, ts, ErrorCodes::USER_NOT_FOUND);
    }
}

void FakeNSEExchange::send_signon_response(const MS_SIGNON_REQUEST_IN* req, uint64_t ts, int16_t error_code) {
    MS_SIGNON_REQUEST_OUT response;
    memset(&response, 0, sizeof(response));
    
    // Copy header and set transaction code
    response.Header = req->Header;
    response.Header.TransactionCode = TransactionCodes::SIGNON_REQUEST_OUT;
    response.Header.ErrorCode = error_code;
    response.Header.MessageLength = sizeof(MS_SIGNON_REQUEST_OUT);

    if (error_code == ErrorCodes::SUCCESS) {
        // Success case        
        // Copy fields from request to response
        response.UserID = req->UserID;
        strncpy(response.BrokerID, req->BrokerID, sizeof(response.BrokerID));
        strncpy(response.TraderName, req->TraderName, sizeof(response.TraderName));
        response.BranchID = req->BranchID;
        response.VersionNumber = req->VersionNumber;
        response.UserType = req->UserType;
        response.SequenceNumber = req->SequenceNumber;
        response.BrokerEligibilityPerMarket = req->BrokerEligibilityPerMarket;
        response.MemberType = req->MemberType;
        strncpy(response.ClearingStatus, req->ClearingStatus, sizeof(response.ClearingStatus));
        strncpy(response.BrokerName, req->BrokerName, sizeof(response.BrokerName));
    
        response.EndTime = static_cast<int32_t>(ts / 1000000) + 28800;
        response.BrokerStatus[0] = '1';
        response.ShowIndex[0] = '1';
        
        std::cout << "Sending successful sign-on response to trader: " << req->Header.TraderId << std::endl;
    } else {
        // Error case
        std::cout << "Sending error sign-on response to trader: " << req->Header.TraderId 
                  << ", ErrorCode: " << error_code << std::endl;
    }

    // Send the response through the message callback
    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&response), sizeof(response));
    }
}

void FakeNSEExchange::handle_signoff_request(const MS_SIGNOFF* req, uint64_t ts) {
    std::cout << "Sign-off request from trader: " << req->Header.TraderId << std::endl;
    
    // Check if trader is logged in
    if (logged_in_traders_.find(req->Header.TraderId) == logged_in_traders_.end()) {
        std::cout << "Trader " << req->Header.TraderId << " not logged in for sign-off request" << std::endl;
        // Send error response
        send_signoff_response(req, ts, ErrorCodes::USER_NOT_FOUND); 
        return;
    }
    
    // Remove trader from logged in set and store the logoff time
    logged_in_traders_.erase(req->Header.TraderId);
    trader_last_logoff_time_[req->Header.TraderId] = static_cast<int32_t>(ts / 1000000);
    std::cout << "Trader " << req->Header.TraderId << " successfully logged off" << std::endl;
    
    // Send successful response
    send_signoff_response(req, ts, ErrorCodes::SUCCESS);
}

void FakeNSEExchange::send_signoff_response(const MS_SIGNOFF* req, uint64_t ts, int16_t error_code) {
    SIGNOFF_OUT response;
    memset(&response, 0, sizeof(response));
    
    // Copy header and set transaction code
    response.Header = req->Header;
    response.Header.TransactionCode = TransactionCodes::SIGN_OFF_REQUEST_OUT;
    response.Header.ErrorCode = error_code;
    response.Header.MessageLength = sizeof(SIGNOFF_OUT);
    
    if (error_code == ErrorCodes::SUCCESS) {
        // Success case
        response.UserId = req->Header.TraderId;        
        std::cout << "Sending successful sign-off response to trader: " << req->Header.TraderId << std::endl;
    } else {
        // Error case
        response.UserId = 0;
        std::cout << "Sending sign-off error response to trader: " << req->Header.TraderId 
                  << ", ErrorCode: " << error_code << std::endl;
    }
    
    // Send response via callback
    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&response), sizeof(response));
    }
}

void FakeNSEExchange::handle_system_info_request(const MS_SYSTEM_INFO_REQ* req, uint64_t ts) {
    std::cout << "System info request from trader: " << req->Header.TraderId << std::endl;
    
    // Check if trader is logged in
    if (logged_in_traders_.find(req->Header.TraderId) == logged_in_traders_.end()) {
        std::cout << "Trader " << req->Header.TraderId << " not logged in for system info request" << std::endl;
        // Send system info response with error code
        send_system_info_response(req, ts, ErrorCodes::USER_NOT_FOUND);
        return;
    }
    
    // Send normal system info response with success code
    send_system_info_response(req, ts, ErrorCodes::SUCCESS);
}

void FakeNSEExchange::send_system_info_response(const MS_SYSTEM_INFO_REQ* req, uint64_t ts, int16_t error_code) {
    MS_SYSTEM_INFO_DATA response;
    memset(&response, 0, sizeof(response));
    
    // Copy header and set transaction code
    response.Header = req->Header;
    response.Header.TransactionCode = TransactionCodes::SYSTEM_INFO_DATA;
    response.Header.ErrorCode = error_code;
    response.Header.MessageLength = sizeof(MS_SYSTEM_INFO_DATA);
    
    if (error_code == ErrorCodes::SUCCESS) {
        // Success case
        // Set market status
        response.MarketStatus.Normal = 1;
        response.MarketStatus.Oddlot = 1;
        response.MarketStatus.Spot = 1;
        response.MarketStatus.Auction = 1;
        
        response.ExMarketStatus.Normal = 1;
        response.ExMarketStatus.Oddlot = 1;
        response.ExMarketStatus.Spot = 1;
        response.ExMarketStatus.Auction = 1;
        
        response.PlMarketStatus.Normal = 1;
        response.PlMarketStatus.Oddlot = 1;
        response.PlMarketStatus.Spot = 1;
        response.PlMarketStatus.Auction = 1;
        
        // Set other system parameters
        response.UpdatePortfolio = 'Y';
        response.MarketIndex = 1;
        
        response.DefaultSettlementPeriod_Normal = 1;
        response.DefaultSettlementPeriod_Spot = 1;
        response.DefaultSettlementPeriod_Auction = 1;
        
        response.CompetitorPeriod = 1;
        response.SolicitorPeriod = 1;
        response.WarningPercent = 1;
        response.VolumeFreezePercent = 1;
        response.SnapQuoteTime = 1;
        
        response.BoardLotQuantity = 1;
        response.TickSize = 1;
        response.MaximumGtcDays = 1;
        
        response.StockEligibleIndicators.BooksMerged = 1;
        response.StockEligibleIndicators.MinimumFill = 1;
        response.StockEligibleIndicators.AON = 1;
        
        response.DisclosedQuantityPercentAllowed = 1;
        response.RiskFreeInterestRate = 1;
        
        std::cout << "Sending successful system info response to trader: " << req->Header.TraderId << std::endl;
    } else {
        // Error case
        std::cout << "Sending system info error response to trader: " << req->Header.TraderId 
                  << ", ErrorCode: " << error_code << std::endl;
    }

    // Send the response through the message callback
    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&response), sizeof(response));
    }
}

// Compare the market status in the request with our current market status
bool FakeNSEExchange::validate_trader_market_status(const MS_UPDATE_LOCAL_DATABASE* req) {
    
    // Check if any market status fields differ
    if (req->MarketStatus.Normal != current_market_status_.Normal ||
        req->MarketStatus.Oddlot != current_market_status_.Oddlot ||
        req->MarketStatus.Spot != current_market_status_.Spot ||
        req->MarketStatus.Auction != current_market_status_.Auction) {
        std::cout << "Market status differs - Trader has outdated information" << std::endl;
        return true;
    }
    
    if (req->ExMarketStatus.Normal != current_ex_market_status_.Normal ||
        req->ExMarketStatus.Oddlot != current_ex_market_status_.Oddlot ||
        req->ExMarketStatus.Spot != current_ex_market_status_.Spot ||
        req->ExMarketStatus.Auction != current_ex_market_status_.Auction) {
        std::cout << "Market status differs - Trader has outdated information" << std::endl;        
        return true;
    }
    
    if (req->PlMarketStatus.Normal != current_pl_market_status_.Normal ||
        req->PlMarketStatus.Oddlot != current_pl_market_status_.Oddlot ||
        req->PlMarketStatus.Spot != current_pl_market_status_.Spot ||
        req->PlMarketStatus.Auction != current_pl_market_status_.Auction) {
        std::cout << "Market status differs - Trader has outdated information" << std::endl;
        return true;
    }

    // If all statuses match, return false indicating no difference
    return false;
}

void FakeNSEExchange::handle_update_local_database(const MS_UPDATE_LOCAL_DATABASE* req, uint64_t ts) {
    std::cout << "Update local database request from trader: " << req->Header.TraderId 
              << " - Security time: " << req->LastUpdateSecurityTime 
              << ", Participant time: " << req->LastUpdateParticipantTime << std::endl;
    
    // Check if trader is logged in
    if (logged_in_traders_.find(req->Header.TraderId) == logged_in_traders_.end()) {
        std::cout << "Trader " << req->Header.TraderId << " not logged in for update local database request" << std::endl;
        // Send error response
        send_update_local_database_response(req, ts, ErrorCodes::USER_NOT_FOUND);
        return;
    }
    
    // Check if trader has outdated market status or markets are opening
    bool trader_has_outdated_status = validate_trader_market_status(req);
    
    if (trader_has_outdated_status || markets_are_opening_) {
        std::cout << "Trader " << req->Header.TraderId 
                  << " has outdated market status or markets are opening - sending partial system info" << std::endl;
        
        // Send partial system information
        send_partial_system_info_for_ldb_request(req, ts);
        return;
    }
    
    // Send normal database download response
    send_update_local_database_response(req, ts, ErrorCodes::SUCCESS);
}

void FakeNSEExchange::send_partial_system_info_for_ldb_request(const MS_UPDATE_LOCAL_DATABASE* req, uint64_t ts) {
    MS_SYSTEM_INFO_DATA response;
    memset(&response, 0, sizeof(response));
    
    // Copy header and set transaction code
    response.Header = req->Header;
    response.Header.TransactionCode = TransactionCodes::PARTIAL_SYSTEM_INFORMATION;
    response.Header.ErrorCode = ErrorCodes::SUCCESS;
    response.Header.MessageLength = sizeof(MS_SYSTEM_INFO_DATA);
    
    // Set the latest market status from the exchange
    response.MarketStatus = current_market_status_;
    response.ExMarketStatus = current_ex_market_status_;
    response.PlMarketStatus = current_pl_market_status_;
    
    std::cout << "Sending PARTIAL_SYSTEM_INFORMATION (7321) to trader: " << req->Header.TraderId 
              << " - Market status update required" << std::endl;
    std::cout << "Sending current market status - Normal: " << current_market_status_.Normal 
              << ", Oddlot: " << current_market_status_.Oddlot 
              << ", Spot: " << current_market_status_.Spot 
              << ", Auction: " << current_market_status_.Auction << std::endl;
    
    // Send the response through the message callback
    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&response), sizeof(response));
    }
}

void FakeNSEExchange::send_update_local_database_response(const MS_UPDATE_LOCAL_DATABASE* req, uint64_t ts, int16_t error_code) {
    // First, send UPDATE_LDB_HEADER
    UPDATE_LDB_HEADER header_response;
    memset(&header_response, 0, sizeof(header_response));
    
    // Set header
    header_response.Header = req->Header;
    header_response.Header.TransactionCode = TransactionCodes::UPDATE_LOCAL_DATABASE_HEADER;
    header_response.Header.ErrorCode = error_code;
    header_response.Header.MessageLength = sizeof(UPDATE_LDB_HEADER);
    
    std::cout << "Sending UPDATE_LDB_HEADER to trader: " << req->Header.TraderId 
              << ", ErrorCode: " << error_code << std::endl;
    
    // Send header
    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&header_response), sizeof(header_response));
    }
    
    // Only send data response if no error
    if (error_code == ErrorCodes::SUCCESS) {
        // Second, send UPDATE_LDB_DATA
        UPDATE_LDB_DATA data_response;
        memset(&data_response, 0, sizeof(data_response));
        
        // Set main header
        data_response.Header = req->Header;
        data_response.Header.TransactionCode = TransactionCodes::UPDATE_LOCAL_DATABASE_DATA;
        data_response.Header.ErrorCode = ErrorCodes::SUCCESS;
        data_response.Header.MessageLength = sizeof(UPDATE_LDB_DATA);
        
        // Set inner header
        data_response.InnerHeader.TraderId = req->Header.TraderId;
        data_response.InnerHeader.LogTime = req->Header.LogTime;
        strncpy(data_response.InnerHeader.AlphaChar, req->Header.AlphaChar, sizeof(data_response.InnerHeader.AlphaChar));
        data_response.InnerHeader.TransactionCode = TransactionCodes::BCAST_PART_MSTR_CHG;
        data_response.InnerHeader.ErrorCode = ErrorCodes::SUCCESS;
        data_response.InnerHeader.Timestamp = req->Header.Timestamp;
        
        // Data field is kept empty for this example
        
        std::cout << "Sending UPDATE_LDB_DATA to trader: " << req->Header.TraderId 
                  << " (data field empty)" << std::endl;
        
        // Send data response
        if (message_callback_) {
            message_callback_(reinterpret_cast<const uint8_t*>(&data_response), sizeof(data_response));
        }
    } else {
        std::cout << "Skipping UPDATE_LDB_DATA due to error code: " << error_code << std::endl;
    }
}

void FakeNSEExchange::handle_exchange_portfolio_request(const EXCH_PORTFOLIO_REQ* req, uint64_t ts) {
    std::cout << "Exchange portfolio request from trader: " << req->Header.TraderId 
              << " - Last update: " << req->LastUpdateDtTime << std::endl;
    
    // Check if trader is logged in
    if (logged_in_traders_.find(req->Header.TraderId) == logged_in_traders_.end()) {
        std::cout << "Trader " << req->Header.TraderId << " not logged in for portfolio request" << std::endl;
        // Send error response
        send_exchange_portfolio_response(req, ts, ErrorCodes::USER_NOT_FOUND);
        return;
    }
    
    // Send successful response
    send_exchange_portfolio_response(req, ts, ErrorCodes::SUCCESS);
}

void FakeNSEExchange::send_exchange_portfolio_response(const EXCH_PORTFOLIO_REQ* req, uint64_t ts, int16_t error_code) {
    EXCH_PORTFOLIO_RESP response;
    memset(&response, 0, sizeof(response));
    
    // Copy header and set transaction code
    response.Header = req->Header;
    response.Header.TransactionCode = TransactionCodes::EXCHANGE_PORTFOLIO_RESPONSE;
    response.Header.ErrorCode = error_code;
    response.Header.MessageLength = sizeof(EXCH_PORTFOLIO_RESP);
    
    if (error_code == ErrorCodes::SUCCESS) {
        // Success case
        response.NoOfRecords = 1;
        response.MoreRecords = 'N';
        response.Filler = 0;
        
        // Set sample portfolio data
        strncpy(response.PortfolioData.Portfolio, "DEMO", sizeof(response.PortfolioData.Portfolio) - 1);
        response.PortfolioData.Token = 1;
        response.PortfolioData.LastUpdateDtTime = static_cast<int32_t>(ts / 1000000);
        response.PortfolioData.DeleteFlag = 'N';
        
        std::cout << "Sending successful portfolio response to trader: " << req->Header.TraderId 
                  << " with " << response.NoOfRecords << " portfolio record(s)" << std::endl;
    } else {
        // Error case
        response.NoOfRecords = 0;
        response.MoreRecords = 'N';
        response.Filler = 0;
        
        std::cout << "Sending portfolio error response to trader: " << req->Header.TraderId 
                  << ", ErrorCode: " << error_code << std::endl;
    }
    
    // Send response via callback
    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&response), sizeof(response));
    }
}

void FakeNSEExchange::handle_message_download(const MS_MESSAGE_DOWNLOAD* req, uint64_t ts) {
    std::cout << "Message download request from trader: " << req->Header.TraderId 
              << " - Sequence number: " << req->SequenceNumber << std::endl;
    
    // Check if trader is logged in
    if (logged_in_traders_.find(req->Header.TraderId) == logged_in_traders_.end()) {
        std::cout << "Trader " << req->Header.TraderId << " not logged in for message download request" << std::endl;
        // Send error response
        send_message_download_response(req, ts, ErrorCodes::USER_NOT_FOUND);
        return;
    }
    
    // Send successful sequence
    send_message_download_response(req, ts, ErrorCodes::SUCCESS);
}

void FakeNSEExchange::send_message_download_response(const MS_MESSAGE_DOWNLOAD* req, uint64_t ts, int16_t error_code) {
    if (error_code != ErrorCodes::SUCCESS) {
        // Error case
        MS_MESSAGE_DOWNLOAD_HEADER header_response;
        memset(&header_response, 0, sizeof(header_response));
        
        header_response.Header = req->Header;
        header_response.Header.TransactionCode = TransactionCodes::MESSAGE_DOWNLOAD_HEADER;
        header_response.Header.ErrorCode = error_code;
        header_response.Header.MessageLength = sizeof(MS_MESSAGE_DOWNLOAD_HEADER);
        
        std::cout << "Sending message download header with error to trader: " << req->Header.TraderId 
                  << ", ErrorCode: " << error_code << std::endl;
        
        if (message_callback_) {
            message_callback_(reinterpret_cast<const uint8_t*>(&header_response), sizeof(header_response));
        }
        return;
    }
    
    // First, send header
    std::cout << "Sending message download header to trader: " << req->Header.TraderId << std::endl;
    
    MS_MESSAGE_DOWNLOAD_HEADER header_response;
    memset(&header_response, 0, sizeof(header_response));
    
    header_response.Header = req->Header;
    header_response.Header.TransactionCode = TransactionCodes::MESSAGE_DOWNLOAD_HEADER;
    header_response.Header.ErrorCode = ErrorCodes::SUCCESS;
    header_response.Header.MessageLength = sizeof(MS_MESSAGE_DOWNLOAD_HEADER);
    
    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&header_response), sizeof(header_response));
    }
    
    // Second, send data
    std::cout << "Sending message download data to trader: " << req->Header.TraderId << std::endl;
    
    MS_MESSAGE_DOWNLOAD_DATA data_response;
    memset(&data_response, 0, sizeof(data_response));
    
    // Outer header
    data_response.Header = req->Header;
    data_response.Header.TransactionCode = TransactionCodes::MESSAGE_DOWNLOAD_DATA;
    data_response.Header.ErrorCode = ErrorCodes::SUCCESS;
    data_response.Header.MessageLength = sizeof(MS_MESSAGE_DOWNLOAD_DATA);
    
    // Inner header
    data_response.InnerHeader = req->Header;
    data_response.InnerHeader.TransactionCode = TransactionCodes::MESSAGE_DOWNLOAD_DATA;
    data_response.InnerHeader.ErrorCode = ErrorCodes::SUCCESS;
    data_response.InnerHeader.MessageLength = sizeof(MESSAGE_HEADER);
    
    // Sample inner data
    const char* sample_message = "Sample trader message data for download";
    strncpy(data_response.InnerData, sample_message, sizeof(data_response.InnerData) - 1);
    
    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&data_response), sizeof(data_response));
    }
    
    // Third, send trailer
    std::cout << "Sending message download trailer to trader: " << req->Header.TraderId << std::endl;
    
    MS_MESSAGE_DOWNLOAD_TRAILER trailer_response;
    memset(&trailer_response, 0, sizeof(trailer_response));
    
    trailer_response.Header = req->Header;
    trailer_response.Header.TransactionCode = TransactionCodes::MESSAGE_DOWNLOAD_TRAILER;
    trailer_response.Header.ErrorCode = ErrorCodes::SUCCESS;
    trailer_response.Header.MessageLength = sizeof(MS_MESSAGE_DOWNLOAD_TRAILER);
    
    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&trailer_response), sizeof(trailer_response));
    }
    
    std::cout << "Message download sequence completed for trader: " << req->Header.TraderId << std::endl;
}

void FakeNSEExchange::handle_order_entry_request(const MS_OE_REQUEST* req, uint64_t ts) {
    std::cout << "Order entry request from trader: " << req->Header.TraderId 
              << " - Token: " << req->TokenNo 
              << ", Symbol: " << std::string(req->ContractDesc.Symbol, 10)
              << ", BuySell: " << req->BuySellIndicator
              << ", Volume: " << req->Volume
              << ", Price: " << req->Price << std::endl;
    
    // Check if trader is logged in
    if (logged_in_traders_.find(req->Header.TraderId) == logged_in_traders_.end()) {
        std::cout << "Trader " << req->Header.TraderId << " not logged in for order entry" << std::endl;
        send_order_response(req, ts, TransactionCodes::ORDER_ERROR_OUT, ErrorCodes::USER_NOT_FOUND);
        return;
    }
    
    // Get broker ID from request
    std::string broker_id(req->BrokerId, 5);
    broker_id.erase(broker_id.find_last_not_of(' ') + 1);
    
    // Check if broker is in closeout
    bool broker_in_closeout = is_broker_in_closeout(broker_id);
    
    if (broker_in_closeout) {
        std::cout << "Broker " << broker_id << " is in closeout status - validating order restrictions" << std::endl;
        
        // Validate closeout rules
        if (!is_valid_closeout_order(req)) {
            std::cout << "Order rejected - invalid for closeout status" << std::endl;
            send_order_response(req, ts, TransactionCodes::ORDER_ERROR_OUT, ErrorCodes::CLOSEOUT_NOT_ALLOWED);
            return;
        }
        
        // Check if it's a participant order
        if (req->ParticipantType == 'P') {
            std::cout << "Participant order rejected - broker in closeout status" << std::endl;
            send_order_response(req, ts, TransactionCodes::ORDER_ERROR_OUT, ErrorCodes::CLOSEOUT_ORDER_REJECT);
            return;
        }
    }
    
    // Simulate different order scenarios
    
    // Check if market is open
    if (req->OrderFlags.Market && current_market_status_.Normal == 1) {
        std::cout << "Market is open - sending price confirmation first" << std::endl;
        // Send price confirmation response
        send_order_response(req, ts, TransactionCodes::PRICE_CONFIRMATION, ErrorCodes::SUCCESS);
    }
    
    // Simulate order processing outcomes (randomly for demo)
    int outcome = rand() % 100;
    
    if (outcome < 70) {
        std::cout << "Order confirmed normally" << std::endl;
        send_order_response(req, ts, TransactionCodes::ORDER_CONFIRMATION_OUT, ErrorCodes::SUCCESS, ReasonCodes::NORMAL_CONFIRMATION);
        
    } else if (outcome < 85) {
        std::cout << "Order frozen - awaiting exchange approval" << std::endl;
        
        // Determine freeze type
        int16_t freeze_reason = (outcome % 2 == 0) ? ReasonCodes::PRICE_FREEZE : ReasonCodes::QUANTITY_FREEZE;
        send_order_response(req, ts, TransactionCodes::FREEZE_TO_CONTROL, ErrorCodes::SUCCESS, freeze_reason);
        
        // Simulate approval/rejection after freeze
        bool freeze_approved = (rand() % 2 == 0);
        if (freeze_approved) {
            std::cout << "Freeze approved - sending confirmation" << std::endl;
            send_order_response(req, ts, TransactionCodes::ORDER_CONFIRMATION_OUT, ErrorCodes::SUCCESS, freeze_reason);
        } else {
            std::cout << "Freeze rejected - sending error" << std::endl;
            if (freeze_reason == ReasonCodes::PRICE_FREEZE) {
                send_order_response(req, ts, TransactionCodes::ORDER_ERROR_OUT, ErrorCodes::OE_PRICE_FREEZE_CAN, freeze_reason);
            } else {
                send_order_response(req, ts, TransactionCodes::ORDER_ERROR_OUT, ErrorCodes::OE_QTY_FREEZE_CAN, freeze_reason);
            }
        }
        
    } else {
        std::cout << "Order rejected due to validation error" << std::endl;
        send_order_response(req, ts, TransactionCodes::ORDER_ERROR_OUT, ErrorCodes::INVALID_ORDER);
    }
}

void FakeNSEExchange::send_order_response(const MS_OE_REQUEST* req, uint64_t ts, int16_t transaction_code, int16_t error_code, int16_t reason_code) {
    MS_OE_REQUEST response;
    memset(&response, 0, sizeof(response));
    
    // Copy original request structure and modify header
    response = *req;
    response.Header.TransactionCode = transaction_code;
    response.Header.ErrorCode = error_code;
    response.Header.MessageLength = sizeof(MS_OE_REQUEST);
    
    // Set reason code
    response.ReasonCode = reason_code;
    
    // Set entry date time for confirmed orders
    if (transaction_code == TransactionCodes::ORDER_CONFIRMATION_OUT || 
        transaction_code == TransactionCodes::PRICE_CONFIRMATION) {
        response.EntryDateTime = static_cast<int32_t>(ts / 1000000);
    }
    
    // Generate order number and activity reference for confirmed orders
    if (transaction_code == TransactionCodes::ORDER_CONFIRMATION_OUT) {
        response.OrderNumber = generate_order_number(ts);
        response.LastActivityReference = generate_activity_reference(ts);
        response.LastModified = static_cast<int32_t>(ts / 1000000);
        
        // Store order
        active_orders_[response.OrderNumber] = response;
        std::cout << "Stored order " << response.OrderNumber << std::endl;
    }
    
    // Handle market order pricing
    if (transaction_code == TransactionCodes::PRICE_CONFIRMATION && req->OrderFlags.Market) {
        int32_t market_price = 10000 + (rand() % 1000);
        
        if (req->BuySellIndicator == 1) {
            response.Price = -market_price;
        } else {
            response.Price = market_price;
        }
        
        // Clear market flag and set the price
        response.OrderFlags.Market = 0;
        
        std::cout << "Market order priced at: " << (response.Price < 0 ? -response.Price : response.Price) 
                  << " (Buy: negative, Sell: positive)" << std::endl;
    }
    
    // Set closeout flag
    if (transaction_code == TransactionCodes::ORDER_CONFIRMATION_OUT || 
        transaction_code == TransactionCodes::ORDER_CANCEL_CONFIRMATION ||
        transaction_code == TransactionCodes::ORDER_ERROR_OUT) {
        
        std::string broker_id(req->BrokerId, 5);
        broker_id.erase(broker_id.find_last_not_of(' ') + 1);
        
        if (is_broker_in_closeout(broker_id)) {
            response.CloseoutFlag = 'C';
        }
    }
    
    // Log the response
    std::cout << "Sending order response: TransactionCode=" << transaction_code 
              << ", ErrorCode=" << error_code 
              << ", ReasonCode=" << reason_code;
    
    if (transaction_code == TransactionCodes::ORDER_CONFIRMATION_OUT) {
        std::cout << ", OrderNumber=" << response.OrderNumber;
    }
    
    if (response.CloseoutFlag == 'C') {
        std::cout << ", CloseoutFlag=C";
    }
    
    std::cout << std::endl;
    
    // Send response via callback
    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&response), sizeof(response));
    }
}

void FakeNSEExchange::handle_price_modification_request(const PRICE_MOD* req, uint64_t ts) {
    std::cout << "Price modification request from trader: " << req->Header.TraderId 
              << " - OrderNumber: " << req->OrderNumber 
              << ", New Price: " << req->Price 
              << ", New Volume: " << req->Volume << std::endl;
    
    // Check if trader is logged in
    if (logged_in_traders_.find(req->Header.TraderId) == logged_in_traders_.end()) {
        std::cout << "Trader " << req->Header.TraderId << " not logged in for order modification" << std::endl;
        send_modification_response(req, ts, TransactionCodes::ORDER_MOD_REJ_OUT, ErrorCodes::USER_NOT_FOUND);
        return;
    }
    
    // Check if the order exists
    auto order_iter = active_orders_.find(req->OrderNumber);
    if (order_iter == active_orders_.end()) {
        std::cout << "Order " << req->OrderNumber << " not found for modification" << std::endl;
        send_modification_response(req, ts, TransactionCodes::ORDER_MOD_REJ_OUT, ErrorCodes::ERR_INVALID_ORDER_NUMBER);
        return;
    }
    
    MS_OE_REQUEST& original_order = order_iter->second;
    
    // Validate that the trader owns this order
    if (original_order.Header.TraderId != req->Header.TraderId) {
        std::cout << "Order " << req->OrderNumber << " does not belong to trader " << req->Header.TraderId << std::endl;
        send_modification_response(req, ts, TransactionCodes::ORDER_MOD_REJ_OUT, ErrorCodes::e$not_your_order);
        return;
    }
    
    // Check broker closeout status
    std::string broker_id(original_order.BrokerId, 5);
    broker_id.erase(broker_id.find_last_not_of(' ') + 1);
    
    if (is_broker_in_closeout(broker_id)) {
        std::cout << "Order modification restricted - broker " << broker_id << " in closeout status" << std::endl;
        send_modification_response(req, ts, TransactionCodes::ORDER_MOD_REJ_OUT, ErrorCodes::CLOSEOUT_TRDMOD_REJECT);
        return;
    }
    
    // Validate modification constraints
    if (!is_valid_modification(original_order, req)) {
        std::cout << "Invalid modification parameters" << std::endl;
        send_modification_response(req, ts, TransactionCodes::ORDER_MOD_REJ_OUT, ErrorCodes::OE_ORD_CANNOT_MODIFY);
        return;
    }
    
    // Simulate if modification will result in freeze
    bool will_freeze = (rand() % 100) < 20;
    
    if (will_freeze) {
        std::cout << "Order modification frozen - awaiting exchange approval" << std::endl;
        
        // Send freeze response
        send_modification_response(req, ts, TransactionCodes::FREEZE_TO_CONTROL, ErrorCodes::SUCCESS);
        
        // Simulate approval/rejection
        bool freeze_approved = (rand() % 2 == 0);
        if (freeze_approved) {
            std::cout << "Modification freeze approved - processing modification" << std::endl;
            process_successful_modification(original_order, req, ts);
        } else {
            std::cout << "Modification freeze rejected" << std::endl;
            send_modification_response(req, ts, TransactionCodes::ORDER_MOD_REJ_OUT, ErrorCodes::OE_ORD_CANNOT_MODIFY);
        }
    } else {
        // Process successful modification
        std::cout << "Order modification accepted" << std::endl;
        process_successful_modification(original_order, req, ts);
    }
}

void FakeNSEExchange::process_successful_modification(MS_OE_REQUEST& original_order, const PRICE_MOD* req, uint64_t ts) {
    // Check if time priority is lost
    bool loses_priority = is_time_priority_lost(&original_order, req);
    
    if (loses_priority) {
        std::cout << "Order will lose time priority due to modification" << std::endl;
    }
    
    // Update the original order with new parameters
    original_order.Price = req->Price;
    original_order.Volume = req->Volume;
    original_order.LastModified = static_cast<int32_t>(ts / 1000000);
    original_order.LastActivityReference = generate_activity_reference(ts);
    
    // Send successful modification response
    send_modification_response(req, ts, TransactionCodes::ORDER_MOD_CONFIRM_OUT, ErrorCodes::SUCCESS);
}

void FakeNSEExchange::send_modification_response(const PRICE_MOD* req, uint64_t ts, int16_t transaction_code, int16_t error_code) {
    MS_OE_REQUEST response;
    memset(&response, 0, sizeof(response));
    
    // Get the original order for response
    if (transaction_code == TransactionCodes::ORDER_MOD_CONFIRM_OUT) {
        auto order_iter = active_orders_.find(req->OrderNumber);
        if (order_iter != active_orders_.end()) {
            response = order_iter->second;
        }
    }
    
    // Set header fields
    response.Header = req->Header;
    response.Header.TransactionCode = transaction_code;
    response.Header.ErrorCode = error_code;
    response.Header.MessageLength = sizeof(MS_OE_REQUEST);
    
    if (transaction_code == TransactionCodes::ORDER_MOD_CONFIRM_OUT && error_code == ErrorCodes::SUCCESS) {
        // Success case
        response.OrderNumber = req->OrderNumber;
        response.Price = req->Price;
        response.Volume = req->Volume;
        response.LastModified = static_cast<int32_t>(ts / 1000000);
        response.LastActivityReference = generate_activity_reference(ts);
        
        // Set closeout flag if applicable
        std::string broker_id(response.BrokerId, 5);
        broker_id.erase(broker_id.find_last_not_of(' ') + 1);
        if (is_broker_in_closeout(broker_id)) {
            response.CloseoutFlag = 'C';
        }
        
        std::cout << "Sending successful modification confirmation to trader: " << req->Header.TraderId 
                  << ", OrderNumber: " << req->OrderNumber 
                  << ", New Price: " << req->Price 
                  << ", New Volume: " << req->Volume << std::endl;
    } else {
        // Error case
        response.OrderNumber = req->OrderNumber;
        
        std::cout << "Sending modification rejection to trader: " << req->Header.TraderId 
                  << ", OrderNumber: " << req->OrderNumber 
                  << ", ErrorCode: " << error_code << std::endl;
    }
    
    // Log additional details
    if (transaction_code == TransactionCodes::FREEZE_TO_CONTROL) {
        std::cout << "Modification frozen for order: " << req->OrderNumber << std::endl;
    }
    
    // Send response via callback
    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&response), sizeof(response));
    }
}

void FakeNSEExchange::handle_order_cancellation_request(const MS_OE_REQUEST* req, uint64_t ts) {
    std::cout << "Order cancellation request from trader: " << req->Header.TraderId 
              << " - OrderNumber: " << req->OrderNumber 
              << ", LastModified: " << req->LastModified
              << ", LastActivityReference: " << req->LastActivityReference << std::endl;
    
    // Check if trader is logged in
    if (logged_in_traders_.find(req->Header.TraderId) == logged_in_traders_.end()) {
        std::cout << "Trader " << req->Header.TraderId << " not logged in for order cancellation" << std::endl;
        send_cancellation_response(req, ts, TransactionCodes::ORDER_CXL_REJ_OUT, ErrorCodes::USER_NOT_FOUND);
        return;
    }
    
    // Check if the order exists
    auto order_iter = active_orders_.find(req->OrderNumber);
    if (order_iter == active_orders_.end()) {
        std::cout << "Order " << req->OrderNumber << " not found for cancellation" << std::endl;
        send_cancellation_response(req, ts, TransactionCodes::ORDER_CXL_REJ_OUT, ErrorCodes::ERR_INVALID_ORDER_NUMBER);
        return;
    }
    
    MS_OE_REQUEST& original_order = order_iter->second;
    
    // Get broker IDs
    std::string canceller_broker_id(req->BrokerId, 5);
    canceller_broker_id.erase(canceller_broker_id.find_last_not_of(' ') + 1);
    
    std::string order_broker_id(original_order.BrokerId, 5);
    order_broker_id.erase(order_broker_id.find_last_not_of(' ') + 1);
    
    // Check if canceller broker is deactivated
    if (is_broker_deactivated(canceller_broker_id)) {
        std::cout << "Deactivated broker " << canceller_broker_id << " cannot cancel orders" << std::endl;
        send_cancellation_response(req, ts, TransactionCodes::ORDER_CXL_REJ_OUT, ErrorCodes::OE_IS_NOT_ACTIVE);
        return;
    }
    
    // Check cancellation hierarchy rules (CM > BM > DL)
    if (!can_cancel_order(canceller_broker_id, order_broker_id)) {
        std::cout << "Broker " << canceller_broker_id << " does not have privileges to cancel order from broker " << order_broker_id << std::endl;
        send_cancellation_response(req, ts, TransactionCodes::ORDER_CXL_REJ_OUT, ErrorCodes::OE_ORD_CANNOT_CANCEL);
        return;
    }
    
    // Validate LastActivityReference if provided
    if (req->LastActivityReference != 0 && !is_valid_activity_reference(&original_order, req)) {
        std::cout << "Invalid LastActivityReference for order " << req->OrderNumber << std::endl;
        send_cancellation_response(req, ts, TransactionCodes::ORDER_CXL_REJ_OUT, ErrorCodes::OE_ORD_CANNOT_CANCEL);
        return;
    }
    
    // Check if order is already cancelled or fully executed
    if (original_order.Volume == 0) {
        std::cout << "Order " << req->OrderNumber << " is already cancelled or fully executed" << std::endl;
        send_cancellation_response(req, ts, TransactionCodes::ORDER_CXL_REJ_OUT, ErrorCodes::OE_ORD_CANNOT_CANCEL);
        return;
    }
    
    // Simulate cancellation outcomes
    int outcome = rand() % 100;
    
    if (outcome < 85) {
        std::cout << "Order cancellation accepted" << std::endl;
        process_successful_cancellation(original_order, req, ts);
        
    } else {
        std::cout << "Order cancellation rejected - order may be partially executed or locked" << std::endl;
        send_cancellation_response(req, ts, TransactionCodes::ORDER_CXL_REJ_OUT, ErrorCodes::OE_ORD_CANNOT_CANCEL);
    }
}

void FakeNSEExchange::process_successful_cancellation(MS_OE_REQUEST& original_order, const MS_OE_REQUEST* cancel_req, uint64_t ts) {
    // Update the original order
    original_order.LastModified = static_cast<int32_t>(ts / 1000000);
    original_order.LastActivityReference = generate_activity_reference(ts);
    
    // Mark order as cancelled
    int32_t cancelled_volume = original_order.Volume;
    original_order.Volume = 0;
    
    std::cout << "Cancelled " << cancelled_volume << " shares for order " << original_order.OrderNumber << std::endl;
    
    // Send successful response
    send_cancellation_response(cancel_req, ts, TransactionCodes::ORDER_CANCEL_CONFIRM_OUT, ErrorCodes::SUCCESS);
}

void FakeNSEExchange::send_cancellation_response(const MS_OE_REQUEST* req, uint64_t ts, int16_t transaction_code, int16_t error_code) {
    MS_OE_REQUEST response;
    memset(&response, 0, sizeof(response));
    
    // Get the original order for response
    if (transaction_code == TransactionCodes::ORDER_CANCEL_CONFIRM_OUT) {
        auto order_iter = active_orders_.find(req->OrderNumber);
        if (order_iter != active_orders_.end()) {
            response = order_iter->second;
        }
    } else {
        response = *req;
    }
    
    // Set header fields
    response.Header = req->Header;
    response.Header.TransactionCode = transaction_code;
    response.Header.ErrorCode = error_code;
    response.Header.MessageLength = sizeof(MS_OE_REQUEST);
    
    if (transaction_code == TransactionCodes::ORDER_CANCEL_CONFIRM_OUT && error_code == ErrorCodes::SUCCESS) {
        // Success case
        response.OrderNumber = req->OrderNumber;
        response.LastModified = static_cast<int32_t>(ts / 1000000);
        response.LastActivityReference = generate_activity_reference(ts);
        response.Volume = 0;
        
        // Set closeout flag if applicable
        std::string broker_id(response.BrokerId, 5);
        broker_id.erase(broker_id.find_last_not_of(' ') + 1);
        if (is_broker_in_closeout(broker_id)) {
            response.CloseoutFlag = 'C';
        }
        
        std::cout << "Sending successful cancellation confirmation to trader: " << req->Header.TraderId 
                  << ", OrderNumber: " << req->OrderNumber << std::endl;
    } else {
        // Error case
        response.OrderNumber = req->OrderNumber;
        
        std::cout << "Sending cancellation rejection to trader: " << req->Header.TraderId 
                  << ", OrderNumber: " << req->OrderNumber 
                  << ", ErrorCode: " << error_code << std::endl;
    }
    
    // Send response via callback
    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&response), sizeof(response));
    }
}

void FakeNSEExchange::handle_kill_switch_request(const MS_OE_REQUEST* req, uint64_t ts) {
    std::cout << "Kill switch request from trader: " << req->Header.TraderId 
              << " - User: " << req->TraderId 
              << ", Token: " << req->TokenNo << std::endl;
    
    // Check if trader is logged in
    if (logged_in_traders_.find(req->Header.TraderId) == logged_in_traders_.end()) {
        std::cout << "Trader " << req->Header.TraderId << " not logged in for kill switch" << std::endl;
        send_kill_switch_response(req, ts, ErrorCodes::USER_NOT_FOUND);
        return;
    }
    
    // Validate TraderId field
    if (req->TraderId == 0) {
        std::cout << "Invalid TraderId in kill switch request" << std::endl;
        send_kill_switch_response(req, ts, ErrorCodes::ERR_INVALID_TRADER_ID);
        return;
    }
    
    // Check broker deactivation status
    std::string broker_id(req->BrokerId, 5);
    broker_id.erase(broker_id.find_last_not_of(' ') + 1);
    
    if (is_broker_deactivated(broker_id)) {
        std::cout << "Deactivated broker " << broker_id << " cannot use kill switch" << std::endl;
        send_kill_switch_response(req, ts, ErrorCodes::OE_IS_NOT_ACTIVE);
        return;
    }
    
    // Process the kill switch cancellation
    int32_t cancelled_count = process_kill_switch_cancellation(req, ts);
    
    if (cancelled_count == 0) {
        std::cout << "No orders found to cancel for kill switch request" << std::endl;
        send_kill_switch_response(req, ts, ErrorCodes::OE_ORD_CANNOT_CANCEL);
    } else {
        std::cout << "Kill switch processed successfully - cancelled " << cancelled_count << " orders" << std::endl;
        send_kill_switch_response(req, ts, ErrorCodes::SUCCESS, cancelled_count);
    }
}

int32_t FakeNSEExchange::process_kill_switch_cancellation(const MS_OE_REQUEST* req, uint64_t ts) {
    int32_t cancelled_count = 0;
    std::vector<double> orders_to_cancel;
    
    // Determine cancellation scope
    bool cancel_all_orders = (req->TokenNo == -1);
    bool cancel_specific_contract = !cancel_all_orders;
    
    if (cancel_all_orders) {
        std::cout << "Kill switch: Cancelling ALL orders for trader " << req->TraderId << std::endl;
    } else {
        std::cout << "Kill switch: Cancelling orders for token " << req->TokenNo 
                  << " and symbol " << std::string(req->ContractDesc.Symbol, 10) << std::endl;
    }
    
    // Find orders to cancel
    for (auto& order_pair : active_orders_) {
        MS_OE_REQUEST& order = order_pair.second;
        
        // Skip already cancelled orders
        if (order.Volume == 0) {
            continue;
        }
        
        // Check if this order belongs to the specified trader
        if (order.TraderId != req->TraderId && order.Header.TraderId != req->Header.TraderId) {
            continue;
        }
        
        // Get order broker ID for privilege check
        std::string order_broker_id(order.BrokerId, 5);
        order_broker_id.erase(order_broker_id.find_last_not_of(' ') + 1);
        
        std::string canceller_broker_id(req->BrokerId, 5);
        canceller_broker_id.erase(canceller_broker_id.find_last_not_of(' ') + 1);
        
        // Check if canceller has privilege to cancel this order
        if (!can_cancel_order(canceller_broker_id, order_broker_id)) {
            std::cout << "Kill switch: Skipping order " << order.OrderNumber 
                      << " - insufficient privileges" << std::endl;
            continue;
        }
        
        bool should_cancel = false;
        
        if (cancel_all_orders) {
            // Cancel all orders for the user
            should_cancel = true;
        } else if (cancel_specific_contract) {
            // Cancel orders for specific contract
            should_cancel = is_contract_match(&order, &req->ContractDesc);
        }
        
        if (should_cancel) {
            orders_to_cancel.push_back(order_pair.first);
            std::cout << "Kill switch: Marking order " << order.OrderNumber << " for cancellation" << std::endl;
        }
    }
    
    // Cancel the identified orders
    for (double order_number : orders_to_cancel) {
        auto order_iter = active_orders_.find(order_number);
        if (order_iter != active_orders_.end()) {
            MS_OE_REQUEST& order = order_iter->second;
            
            // Update order cancellation details
            int32_t cancelled_volume = order.Volume;
            order.Volume = 0;
            order.LastModified = static_cast<int32_t>(ts / 1000000);
            order.LastActivityReference = generate_activity_reference(ts);
            
            cancelled_count++;
            
            std::cout << "Kill switch: Cancelled order " << order.OrderNumber 
                      << " with volume " << cancelled_volume << std::endl;
            
            // Send individual cancellation confirmation for each order
            send_cancellation_response(&order, ts, TransactionCodes::ORDER_CANCEL_CONFIRM_OUT, ErrorCodes::SUCCESS);
        }
    }
    
    return cancelled_count;
}

void FakeNSEExchange::send_kill_switch_response(const MS_OE_REQUEST* req, uint64_t ts, int16_t error_code, int32_t cancelled_count) {
    if (error_code == ErrorCodes::SUCCESS) {
        std::cout << "Kill switch completed successfully for trader: " << req->Header.TraderId 
                  << ", cancelled " << cancelled_count << " orders" << std::endl;
        return;
    }
    
    // For error cases, send ORDER_ERROR_OUT response
    MS_OE_REQUEST response;
    memset(&response, 0, sizeof(response));
    
    // Copy from request and set response details
    response = *req;
    response.Header.TransactionCode = TransactionCodes::ORDER_ERROR_OUT;
    response.Header.ErrorCode = error_code;
    response.Header.MessageLength = sizeof(MS_OE_REQUEST);
    
    std::cout << "Sending kill switch error response to trader: " << req->Header.TraderId 
              << ", ErrorCode: " << error_code << std::endl;
    
    // Send error response via callback
    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&response), sizeof(response));
    }
}

void FakeNSEExchange::handle_trade_modification_request(const MS_TRADE_INQ_DATA* req, uint64_t ts) {
    std::cout << "Trade modification request from trader: " << req->Header.TraderId 
              << " - FillNumber: " << req->FillNumber 
              << ", RequestedBy: " << static_cast<int>(req->RequestedBy) << std::endl;
    
    // Check if trader is logged in
    if (logged_in_traders_.find(req->Header.TraderId) == logged_in_traders_.end()) {
        std::cout << "Trader " << req->Header.TraderId << " not logged in for trade modification" << std::endl;
        send_trade_modification_response(req, ts, ErrorCodes::USER_NOT_FOUND);
        return;
    }
    
    // Check for duplicate request
    if (is_duplicate_trade_request(req->FillNumber, req->Header.TraderId, "modify")) {
        std::cout << "Duplicate trade modification request for FillNumber: " << req->FillNumber << std::endl;
        send_trade_modification_response(req, ts, ErrorCodes::e_dup_request);
        return;
    }
    
    // Check if trade exists
    auto trade_iter = executed_trades_.find(req->FillNumber);
    if (trade_iter == executed_trades_.end()) {
        std::cout << "Trade " << req->FillNumber << " not found for modification" << std::endl;
        send_trade_modification_response(req, ts, ErrorCodes::E_invalid_fill_number);
        return;
    }
    
    MS_TRADE_INQ_DATA& existing_trade = trade_iter->second;
    
    // Check if trader owns this trade
    std::string broker_id(req->BuyBrokerId, 5);
    if (!is_trade_owner(existing_trade, req->Header.TraderId, broker_id)) {
        std::cout << "Trade " << req->FillNumber << " does not belong to trader " << req->Header.TraderId << std::endl;
        send_trade_modification_response(req, ts, ErrorCodes::E_not_your_fill);
        return;
    }
    
    // Check if user is allowed to modify trades (documentation validation)
    // The documentation doesn't specify additional user-level restrictions beyond ownership
    // so we just check basic ownership and broker status
    
    // Check broker closeout status
    std::string buy_broker_id(req->BuyBrokerId, 5);
    buy_broker_id.erase(buy_broker_id.find_last_not_of(' ') + 1);
    
    if (is_broker_in_closeout(buy_broker_id)) {
        std::cout << "Trade modification restricted - broker " << buy_broker_id << " in closeout status" << std::endl;
        send_trade_modification_response(req, ts, ErrorCodes::CLOSEOUT_TRDMOD_REJECT);
        return;
    }
    
    // Validate modification request
    if (req->RequestedBy != '1' && req->RequestedBy != '2' && req->RequestedBy != '3') {
        std::cout << "Invalid RequestedBy field: " << static_cast<int>(req->RequestedBy) << std::endl;
        send_trade_modification_response(req, ts, ErrorCodes::INVALID_ORDER);
        return;
    }
    
    // Check if quantities match (NSE requirement for trade modification)
    if (req->FillQuantity != existing_trade.FillQuantity) {
        std::cout << "Trade modification with different quantities not allowed" << std::endl;
        send_trade_modification_response(req, ts, ErrorCodes::OE_DIFF_TRD_MOD_VOL);
        return;
    }
    
    // Check if only client account modification is being attempted
    bool buy_account_changed = (strncmp(req->BuyAccountNumber, existing_trade.BuyAccountNumber, 10) != 0);
    bool sell_account_changed = (strncmp(req->SellAccountNumber, existing_trade.SellAccountNumber, 10) != 0);
    
    if (!buy_account_changed && !sell_account_changed) {
        std::cout << "No account number changes detected in trade modification request" << std::endl;
        send_trade_modification_response(req, ts, ErrorCodes::ERR_DATA_NOT_CHANGED);
        return;
    }
    
    // Simulate successful modification
    std::cout << "Trade modification accepted for FillNumber: " << req->FillNumber << std::endl;
    
    // Update the trade record
    if (req->RequestedBy == '1' || req->RequestedBy == '3') { // Buy side or both
        strncpy(existing_trade.BuyAccountNumber, req->BuyAccountNumber, sizeof(existing_trade.BuyAccountNumber));
    }
    if (req->RequestedBy == '2' || req->RequestedBy == '3') { // Sell side or both
        strncpy(existing_trade.SellAccountNumber, req->SellAccountNumber, sizeof(existing_trade.SellAccountNumber));
    }
    
    // Mark request as processed
    mark_trade_request(req->FillNumber, req->Header.TraderId, "modify");
    
    // Send successful response
    send_trade_modification_response(req, ts, ErrorCodes::SUCCESS);
}

void FakeNSEExchange::send_trade_modification_response(const MS_TRADE_INQ_DATA* req, uint64_t ts, int16_t error_code) {
    MS_TRADE_INQ_DATA response;
    memset(&response, 0, sizeof(response));
    
    // Copy request and modify header
    response = *req;
    
    if (error_code == ErrorCodes::SUCCESS) {
        response.Header.TransactionCode = TransactionCodes::TRADE_MOD_IN; // Success uses same code
        response.Header.ErrorCode = ErrorCodes::SUCCESS;
        
        std::cout << "Sending successful trade modification response to trader: " << req->Header.TraderId 
                  << ", FillNumber: " << req->FillNumber << std::endl;
    } else {
        response.Header.TransactionCode = TransactionCodes::TRADE_ERROR;
        response.Header.ErrorCode = error_code;
        
        std::cout << "Sending trade modification error response to trader: " << req->Header.TraderId 
                  << ", FillNumber: " << req->FillNumber 
                  << ", ErrorCode: " << error_code << std::endl;
    }
    
    response.Header.MessageLength = sizeof(MS_TRADE_INQ_DATA);
    
    // Send response via callback
    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&response), sizeof(response));
    }
}

void FakeNSEExchange::handle_trade_cancellation_request(const MS_TRADE_INQ_DATA* req, uint64_t ts) {
    std::cout << "Trade cancellation request from trader: " << req->Header.TraderId 
              << " - FillNumber: " << req->FillNumber << std::endl;
    
    // Check if trader is logged in
    if (logged_in_traders_.find(req->Header.TraderId) == logged_in_traders_.end()) {
        std::cout << "Trader " << req->Header.TraderId << " not logged in for trade cancellation" << std::endl;
        send_trade_cancellation_response(req, ts, ErrorCodes::USER_NOT_FOUND);
        return;
    }
    
    // Check for duplicate request
    if (is_duplicate_trade_request(req->FillNumber, req->Header.TraderId, "cancel")) {
        std::cout << "Duplicate trade cancellation request for FillNumber: " << req->FillNumber << std::endl;
        send_trade_cancellation_response(req, ts, ErrorCodes::e_dup_trd_cxl_request);
        return;
    }
    
    // Check if trade exists
    auto trade_iter = executed_trades_.find(req->FillNumber);
    if (trade_iter == executed_trades_.end()) {
        std::cout << "Trade " << req->FillNumber << " not found for cancellation" << std::endl;
        send_trade_cancellation_response(req, ts, ErrorCodes::E_invalid_fill_number);
        return;
    }
    
    MS_TRADE_INQ_DATA& existing_trade = trade_iter->second;
    
    // Check if trader owns this trade
    std::string broker_id(req->BuyBrokerId, 5);
    if (!is_trade_owner(existing_trade, req->Header.TraderId, broker_id)) {
        std::cout << "Trade " << req->FillNumber << " does not belong to trader " << req->Header.TraderId << std::endl;
        send_trade_cancellation_response(req, ts, ErrorCodes::E_not_your_fill);
        return;
    }
    
    // Note: Trade cancellation requires both parties to request it
    // This is a simplified implementation - in reality, you'd need to track
    // both party requests and only cancel when both have requested
    
    std::cout << "Trade cancellation request logged for FillNumber: " << req->FillNumber << std::endl;
    std::cout << "Note: Both parties must request cancellation for it to be processed" << std::endl;
    
    // Mark request as processed
    mark_trade_request(req->FillNumber, req->Header.TraderId, "cancel");
    
    // Send acknowledgment response
    send_trade_cancellation_response(req, ts, ErrorCodes::SUCCESS);
}

void FakeNSEExchange::send_trade_cancellation_response(const MS_TRADE_INQ_DATA* req, uint64_t ts, int16_t error_code) {
    MS_TRADE_INQ_DATA response;
    memset(&response, 0, sizeof(response));
    
    // Copy request and modify header
    response = *req;
    
    if (error_code == ErrorCodes::SUCCESS) {
        response.Header.TransactionCode = TransactionCodes::TRADE_CANCEL_OUT;
        response.Header.ErrorCode = ErrorCodes::SUCCESS;
        
        std::cout << "Sending trade cancellation acknowledgment to trader: " << req->Header.TraderId 
                  << ", FillNumber: " << req->FillNumber << std::endl;
    } else {
        response.Header.TransactionCode = TransactionCodes::TRADE_ERROR;
        response.Header.ErrorCode = error_code;
        
        std::cout << "Sending trade cancellation error response to trader: " << req->Header.TraderId 
                  << ", FillNumber: " << req->FillNumber 
                  << ", ErrorCode: " << error_code << std::endl;
    }
    
    response.Header.MessageLength = sizeof(MS_TRADE_INQ_DATA);
    
    // Send response via callback
    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&response), sizeof(response));
    }
}

void FakeNSEExchange::handle_spread_order_entry_request(const MS_SPD_OE_REQUEST* req, uint64_t ts) {
    std::cout << "Spread order entry request from trader: " << req->Header.TraderId 
              << " - Token1: " << req->Token1 
              << ", Token2: " << req->MS_SPD_LEG_INFO_leg2.Token2 << std::endl;
    
    // User is unable to log into the trading system
    if (logged_in_traders_.find(req->Header.TraderId) == logged_in_traders_.end()) {
        std::cout << "Trader " << req->Header.TraderId << " not logged in" << std::endl;
        send_spread_order_response(req, ts, TransactionCodes::SP_ORDER_ERROR, ErrorCodes::USER_NOT_FOUND);
        return;
    }
    
    // Order is of GTC or GTD order type
    if (req->OrderFlags.GTC || req->GoodTillDate1 != 0) {
        std::cout << "GTC/GTD orders not allowed for spread orders" << std::endl;
        send_spread_order_response(req, ts, TransactionCodes::SP_ORDER_ERROR, 16229); // e$gtc_gtd_ord_not_allowed_pclose
        return;
    }
    
    // Markets are closed
    if (current_market_status_.Normal != 1) {
        std::cout << "Market is not open for spread orders" << std::endl;
        send_spread_order_response(req, ts, TransactionCodes::SP_ORDER_ERROR, 16000); // MARKET_CLOSED
        return;
    }
    
    // Get broker ID
    std::string broker_id(req->BrokerId1, 5);
    broker_id.erase(broker_id.find_last_not_of(' ') + 1);
    
    // Broker is suspended
    if (is_broker_in_closeout(broker_id)) {
        std::cout << "Broker " << broker_id << " is suspended" << std::endl;
        send_spread_order_response(req, ts, TransactionCodes::SP_ORDER_ERROR, ErrorCodes::CLOSEOUT_ORDER_REJECT);
        return;
    }
    
    // Broker is deactivated
    if (is_broker_deactivated(broker_id)) {
        std::cout << "Broker " << broker_id << " is deactivated" << std::endl;
        send_spread_order_response(req, ts, TransactionCodes::SP_ORDER_ERROR, ErrorCodes::OE_IS_NOT_ACTIVE);
        return;
    }
    
    // IOC orders not allowed for spreads
    if (req->OrderFlags.IOC) {
        std::cout << "IOC orders not allowed for spread orders" << std::endl;
        send_spread_order_response(req, ts, TransactionCodes::SP_ORDER_ERROR, ErrorCodes::INVALID_ORDER);
        return;
    }
    
    // Disclosed quantity not allowed for spreads
    if (req->DisclosedVol1 > 0 || req->MS_SPD_LEG_INFO_leg2.DisclosedVol2 > 0) {
        std::cout << "Disclosed quantity not allowed for spread orders" << std::endl;
        send_spread_order_response(req, ts, TransactionCodes::SP_ORDER_ERROR, ErrorCodes::INVALID_ORDER);
        return;
    }
    
    // Both contracts cannot have same expiry date
    if (req->ContractDesc.ExpiryDate == req->MS_SPD_LEG_INFO_leg2.ContractDesc.ExpiryDate) {
        std::cout << "Both legs cannot have same expiry date" << std::endl;
        send_spread_order_response(req, ts, TransactionCodes::SP_ORDER_ERROR, 16627); // e$invalid_contract_comb
        return;
    }
    
    // Validate spread combination is allowed (check against spread combination master)
    if (!is_valid_spread_combination(req->Token1, req->MS_SPD_LEG_INFO_leg2.Token2)) {
        std::cout << "Invalid spread combination: Token1=" << req->Token1 
                  << ", Token2=" << req->MS_SPD_LEG_INFO_leg2.Token2 << std::endl;
        send_spread_order_response(req, ts, TransactionCodes::SP_ORDER_ERROR, 16627); // e$invalid_contract_comb
        return;
    }
    
    // Get account number
    std::string account(req->AccountNumber1, 10);
    account.erase(account.find_last_not_of(' ') + 1);
    
    // PRO order validation
    if (!is_valid_pro_order(req->ProClient1, account, broker_id)) {
        std::cout << "Invalid PRO order configuration" << std::endl;
        send_spread_order_response(req, ts, TransactionCodes::SP_ORDER_ERROR, 16414); // e$invalid_pro_client
        return;
    }
    
    // CLI order validation
    if (!is_valid_cli_order(req->ProClient1, account, broker_id)) {
        std::cout << "Invalid CLI order configuration" << std::endl;
        send_spread_order_response(req, ts, TransactionCodes::SP_ORDER_ERROR, 16632); // e$invalid_cli_ac
        return;
    }
    
    // Quantity validation - must be multiple of regular lot
    const int32_t REGULAR_LOT = 1;
    if (req->Volume1 % REGULAR_LOT != 0 || req->MS_SPD_LEG_INFO_leg2.Volume2 % REGULAR_LOT != 0) {
        std::cout << "Quantity must be multiple of regular lot" << std::endl;
        send_spread_order_response(req, ts, TransactionCodes::SP_ORDER_ERROR, 16328); // OE_QUANTITY_NOT_MULT_RL
        return;
    }
    
    // Price difference validation - must be within operating range
    const int32_t MAX_PRICE_DIFF = 99999999;
    if (abs(req->PriceDiff) > MAX_PRICE_DIFF) {
        std::cout << "Price difference beyond operating range" << std::endl;
        send_spread_order_response(req, ts, TransactionCodes::SP_ORDER_ERROR, 16713); // e$price_diff_out_of_range
        return;
    }
    
    // Continue with order processing...
    int outcome = rand() % 100;
    if (outcome < 70) {
        std::cout << "Spread order confirmed normally" << std::endl;
        
        // Generate order number and store the order
        double order_number = generate_order_number(ts);
        MS_SPD_OE_REQUEST stored_order = *req;
        stored_order.OrderNumber1 = order_number;
        stored_order.EntryDateTime1 = static_cast<int32_t>(ts / 1000000);
        stored_order.LastModified1 = static_cast<int32_t>(ts / 1000000);
        stored_order.LastActivityReference = generate_activity_reference(ts);
        
        // Store the order in active spread orders
        active_spread_orders_[order_number] = stored_order;
        
        send_spread_order_response(req, ts, TransactionCodes::SP_ORDER_CONFIRMATION, ErrorCodes::SUCCESS);
    } else if (outcome < 85) {
        std::cout << "Spread order frozen - awaiting exchange approval" << std::endl;
        send_spread_order_response(req, ts, TransactionCodes::FREEZE_TO_CONTROL, ErrorCodes::SUCCESS, ReasonCodes::PRICE_FREEZE);
    } else {
        std::cout << "Spread order rejected due to validation error" << std::endl;
        send_spread_order_response(req, ts, TransactionCodes::SP_ORDER_ERROR, ErrorCodes::INVALID_ORDER);
    }
}

void FakeNSEExchange::send_spread_order_response(const MS_SPD_OE_REQUEST* req, uint64_t ts, int16_t transaction_code, int16_t error_code, int16_t reason_code) {
    MS_SPD_OE_REQUEST response;
    memset(&response, 0, sizeof(response));
    
    // Copy original request structure and modify header
    response = *req;
    response.Header.TransactionCode = transaction_code;
    response.Header.ErrorCode = error_code;
    response.Header.MessageLength = sizeof(MS_SPD_OE_REQUEST);
    
    // Set reason code
    response.ReasonCode1 = reason_code;
    
    // Set entry date time for confirmed orders
    if (transaction_code == TransactionCodes::SP_ORDER_CONFIRMATION) {
        response.EntryDateTime1 = static_cast<int32_t>(ts / 1000000);
    }
    
    // Generate order number and activity reference for confirmed orders
    if (transaction_code == TransactionCodes::SP_ORDER_CONFIRMATION) {
        response.OrderNumber1 = generate_order_number(ts);
        response.LastActivityReference = generate_activity_reference(ts);
        response.LastModified1 = static_cast<int32_t>(ts / 1000000);
        
        std::cout << "Generated spread order number: " << response.OrderNumber1 << std::endl;
    }
    
    // Note: MS_SPD_OE_REQUEST does not have CloseoutFlag field
    // Closeout status is handled through the broker validation logic
    
    // Log the response
    std::cout << "Sending spread order response: TransactionCode=" << transaction_code 
              << ", ErrorCode=" << error_code 
              << ", ReasonCode=" << reason_code;
    
    if (transaction_code == TransactionCodes::SP_ORDER_CONFIRMATION) {
        std::cout << ", OrderNumber=" << response.OrderNumber1;
    }
    
    // CloseoutFlag not available in MS_SPD_OE_REQUEST structure
    
    std::cout << std::endl;
    
    // Send response via callback
    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&response), sizeof(response));
    }
}

void FakeNSEExchange::handle_spread_order_modification_request(const MS_SPD_OE_REQUEST* req, uint64_t ts) {
    std::cout << "Spread order modification request from trader: " << req->Header.TraderId 
              << " - OrderNumber: " << req->OrderNumber1 << std::endl;
    
    // User is unable to log into the trading system
    if (logged_in_traders_.find(req->Header.TraderId) == logged_in_traders_.end()) {
        std::cout << "Trader " << req->Header.TraderId << " not logged in" << std::endl;
        send_spread_order_response(req, ts, TransactionCodes::SP_ORDER_MOD_REJ_OUT, ErrorCodes::USER_NOT_FOUND);
        return;
    }
    
    // Markets are closed
    if (current_market_status_.Normal != 1) {
        std::cout << "Market is not open for spread order modifications" << std::endl;
        send_spread_order_response(req, ts, TransactionCodes::SP_ORDER_MOD_REJ_OUT, ErrorCodes::MARKET_CLOSED);
        return;
    }
    
    // Find the original order
    auto order_iter = active_spread_orders_.find(req->OrderNumber1);
    if (order_iter == active_spread_orders_.end()) {
        std::cout << "Spread order not found: " << req->OrderNumber1 << std::endl;
        send_spread_order_response(req, ts, TransactionCodes::SP_ORDER_MOD_REJ_OUT, ErrorCodes::ERR_INVALID_ORDER_NUMBER);
        return;
    }
    
    MS_SPD_OE_REQUEST& original_order = order_iter->second;
    
    // Check if this trader owns the order
    if (original_order.Header.TraderId != req->Header.TraderId) {
        std::cout << "Trader " << req->Header.TraderId << " does not own order " << req->OrderNumber1 << std::endl;
        send_spread_order_response(req, ts, TransactionCodes::SP_ORDER_MOD_REJ_OUT, ErrorCodes::e$not_your_order);
        return;
    }
    
    // Get broker ID
    std::string broker_id(req->BrokerId1, 5);
    broker_id.erase(broker_id.find_last_not_of(' ') + 1);
    
    // Broker is suspended
    if (is_broker_in_closeout(broker_id)) {
        std::cout << "Broker " << broker_id << " is suspended" << std::endl;
        send_spread_order_response(req, ts, TransactionCodes::SP_ORDER_MOD_REJ_OUT, ErrorCodes::CLOSEOUT_ORDER_REJECT);
        return;
    }
    
    // Broker is deactivated
    if (is_broker_deactivated(broker_id)) {
        std::cout << "Broker " << broker_id << " is deactivated" << std::endl;
        send_spread_order_response(req, ts, TransactionCodes::SP_ORDER_MOD_REJ_OUT, ErrorCodes::OE_IS_NOT_ACTIVE);
        return;
    }
    
    // Check if the order can be modified
    if (original_order.OrderFlags.Frozen) {
        std::cout << "Cannot modify frozen spread order" << std::endl;
        send_spread_order_response(req, ts, TransactionCodes::SP_ORDER_MOD_REJ_OUT, ErrorCodes::OE_ORD_CANNOT_MODIFY);
        return;
    }
    
    // Validate the modification
    if (!is_valid_spread_modification(original_order, req)) {
        std::cout << "Invalid spread order modification" << std::endl;
        send_spread_order_response(req, ts, TransactionCodes::SP_ORDER_MOD_REJ_OUT, ErrorCodes::INVALID_ORDER);
        return;
    }
    
    // Validate activity reference
    if (!is_valid_spread_activity_reference(&original_order, req)) {
        std::cout << "Invalid activity reference for spread order modification" << std::endl;
        send_spread_order_response(req, ts, TransactionCodes::SP_ORDER_MOD_REJ_OUT, ErrorCodes::INVALID_ORDER);
        return;
    }
    
    // Cannot modify spread day order to IOC
    if (req->OrderFlags.IOC && !original_order.OrderFlags.IOC) {
        std::cout << "Cannot modify spread day order to IOC" << std::endl;
        send_spread_order_response(req, ts, TransactionCodes::SP_ORDER_MOD_REJ_OUT, ErrorCodes::INVALID_ORDER);
        return;
    }
    
    // Process successful modification
    std::cout << "Spread order modification accepted" << std::endl;
    process_successful_spread_modification(original_order, req, ts);
    send_spread_order_response(req, ts, TransactionCodes::SP_ORDER_MOD_CON_OUT, ErrorCodes::SUCCESS);
}

void FakeNSEExchange::handle_spread_order_cancellation_request(const MS_SPD_OE_REQUEST* req, uint64_t ts) {
    std::cout << "Spread order cancellation request from trader: " << req->Header.TraderId 
              << " - OrderNumber: " << req->OrderNumber1 << std::endl;
    
    // User is unable to log into the trading system
    if (logged_in_traders_.find(req->Header.TraderId) == logged_in_traders_.end()) {
        std::cout << "Trader " << req->Header.TraderId << " not logged in" << std::endl;
        send_spread_order_response(req, ts, TransactionCodes::SP_ORDER_CXL_REJ_OUT, ErrorCodes::USER_NOT_FOUND);
        return;
    }
    
    // Find the original order
    auto order_iter = active_spread_orders_.find(req->OrderNumber1);
    if (order_iter == active_spread_orders_.end()) {
        std::cout << "Spread order not found: " << req->OrderNumber1 << std::endl;
        send_spread_order_response(req, ts, TransactionCodes::SP_ORDER_CXL_REJ_OUT, ErrorCodes::ERR_INVALID_ORDER_NUMBER);
        return;
    }
    
    MS_SPD_OE_REQUEST& original_order = order_iter->second;
    
    // Check if this trader owns the order
    if (original_order.Header.TraderId != req->Header.TraderId) {
        std::cout << "Trader " << req->Header.TraderId << " does not own order " << req->OrderNumber1 << std::endl;
        send_spread_order_response(req, ts, TransactionCodes::SP_ORDER_CXL_REJ_OUT, ErrorCodes::e$not_your_order);
        return;
    }
    
    // Get broker ID
    std::string broker_id(req->BrokerId1, 5);
    broker_id.erase(broker_id.find_last_not_of(' ') + 1);
    
    // Broker is suspended
    if (is_broker_in_closeout(broker_id)) {
        std::cout << "Broker " << broker_id << " is suspended" << std::endl;
        send_spread_order_response(req, ts, TransactionCodes::SP_ORDER_CXL_REJ_OUT, ErrorCodes::CLOSEOUT_ORDER_REJECT);
        return;
    }
    
    // Broker is deactivated
    if (is_broker_deactivated(broker_id)) {
        std::cout << "Broker " << broker_id << " is deactivated" << std::endl;
        send_spread_order_response(req, ts, TransactionCodes::SP_ORDER_CXL_REJ_OUT, ErrorCodes::OE_IS_NOT_ACTIVE);
        return;
    }
    
    // Validate activity reference
    if (!is_valid_spread_activity_reference(&original_order, req)) {
        std::cout << "Invalid activity reference for spread order cancellation" << std::endl;
        send_spread_order_response(req, ts, TransactionCodes::SP_ORDER_CXL_REJ_OUT, ErrorCodes::INVALID_ORDER);
        return;
    }
    
    // Remove the order from active orders
    active_spread_orders_.erase(order_iter);
    
    std::cout << "Spread order cancellation successful" << std::endl;
    send_spread_order_response(req, ts, TransactionCodes::SP_ORDER_CXL_CONFIRMATION, ErrorCodes::SUCCESS);
}

// Helper methods for spread order validation and processing
bool FakeNSEExchange::is_valid_spread_modification(const MS_SPD_OE_REQUEST& original_order, const MS_SPD_OE_REQUEST* modification) const {
    // Check if trying to change buy/sell direction (not allowed)
    if (original_order.BuySell1 != modification->BuySell1 || 
        original_order.MS_SPD_LEG_INFO_leg2.BuySell2 != modification->MS_SPD_LEG_INFO_leg2.BuySell2) {
        return false;
    }
    
    // Check if trying to change contract details (not allowed)
    if (memcmp(&original_order.ContractDesc, &modification->ContractDesc, sizeof(CONTRACT_DESC)) != 0 ||
        memcmp(&original_order.MS_SPD_LEG_INFO_leg2.ContractDesc, &modification->MS_SPD_LEG_INFO_leg2.ContractDesc, sizeof(CONTRACT_DESC)) != 0) {
        return false;
    }
    
    // Check if trying to change frozen order (not allowed)
    if (original_order.OrderFlags.Frozen) {
        return false;
    }
    
    // Quantities must be multiples of regular lot
    const int32_t REGULAR_LOT = 1;
    if (modification->Volume1 % REGULAR_LOT != 0 || modification->MS_SPD_LEG_INFO_leg2.Volume2 % REGULAR_LOT != 0) {
        return false;
    }
    
    // Price difference must be within operating range
    const int32_t MAX_PRICE_DIFF = 99999999;
    if (abs(modification->PriceDiff) > MAX_PRICE_DIFF) {
        return false;
    }
    
    return true;
}

bool FakeNSEExchange::is_valid_spread_activity_reference(const MS_SPD_OE_REQUEST* order, const MS_SPD_OE_REQUEST* modify_req) const {
    // For simplicity, accept any activity reference for now
    // In real implementation, this would validate the LastActivityReference field
    return modify_req->LastActivityReference != 0;
}

void FakeNSEExchange::process_successful_spread_modification(MS_SPD_OE_REQUEST& original_order, const MS_SPD_OE_REQUEST* req, uint64_t ts) {
    // Update modifiable fields
    original_order.Volume1 = req->Volume1;
    original_order.MS_SPD_LEG_INFO_leg2.Volume2 = req->MS_SPD_LEG_INFO_leg2.Volume2;
    original_order.PriceDiff = req->PriceDiff;
    
    // Update remaining volumes
    original_order.TotalVolRemaining1 = req->Volume1;
    original_order.MS_SPD_LEG_INFO_leg2.TotalVolRemaining2 = req->MS_SPD_LEG_INFO_leg2.Volume2;
    
    // Update modification timestamps
    original_order.LastModified1 = static_cast<int32_t>(ts / 1000000);
    original_order.LastActivityReference = generate_activity_reference(ts);
    
    // Mark as modified
    original_order.OrderFlags.Modified = 1;
    
    std::cout << "Spread order successfully modified - New Volume1: " << original_order.Volume1
              << ", New Volume2: " << original_order.MS_SPD_LEG_INFO_leg2.Volume2
              << ", New PriceDiff: " << original_order.PriceDiff << std::endl;
}

// Spread Combination Master Update Broadcast Implementation

void FakeNSEExchange::broadcast_spread_combination_update(const MS_SPD_UPDATE_INFO& update_info, uint64_t ts) {
    std::cout << "Broadcasting spread combination master update for tokens: " 
              << update_info.Token1 << " and " << update_info.Token2 << std::endl;
    
    // Create broadcast message with BCAST_HEADER
    struct BCAST_SPD_UPDATE {
        BCAST_HEADER Header;
        MS_SPD_UPDATE_INFO UpdateInfo;
    };
    
    BCAST_SPD_UPDATE broadcast;
    memset(&broadcast, 0, sizeof(broadcast));
    
    // Set up BCAST_HEADER
    broadcast.Header.LogTime = static_cast<int32_t>(ts / 1000000);
    broadcast.Header.TransactionCode = TransactionCodes::BCAST_SPD_MSTR_CHG;
    broadcast.Header.ErrorCode = ErrorCodes::SUCCESS;
    broadcast.Header.MessageLength = sizeof(BCAST_SPD_UPDATE);
    
    // Copy update info
    broadcast.UpdateInfo = update_info;
    
    std::cout << "Sending spread combination update - Token1: " << update_info.Token1 
              << ", Token2: " << update_info.Token2 
              << ", ReferencePrice: " << update_info.ReferencePrice
              << ", Eligibility: " << (int)update_info.SPDEligibility.Eligibility
              << ", DeleteFlag: " << update_info.DeleteFlag << std::endl;
    
    // Send broadcast via callback
    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&broadcast), sizeof(broadcast));
    }
}

void FakeNSEExchange::broadcast_periodic_spread_combination_update(const MS_SPD_UPDATE_INFO& update_info, uint64_t ts) {
    std::cout << "Broadcasting periodic spread combination master update for tokens: " 
              << update_info.Token1 << " and " << update_info.Token2 << std::endl;
    
    // Create broadcast message with BCAST_HEADER
    struct BCAST_SPD_UPDATE {
        BCAST_HEADER Header;
        MS_SPD_UPDATE_INFO UpdateInfo;
    };
    
    BCAST_SPD_UPDATE broadcast;
    memset(&broadcast, 0, sizeof(broadcast));
    
    // Set up BCAST_HEADER
    broadcast.Header.LogTime = static_cast<int32_t>(ts / 1000000);
    broadcast.Header.TransactionCode = TransactionCodes::BCAST_SPD_MSTR_CHG_PERIODIC;
    broadcast.Header.ErrorCode = ErrorCodes::SUCCESS;
    broadcast.Header.MessageLength = sizeof(BCAST_SPD_UPDATE);
    
    // Copy update info
    broadcast.UpdateInfo = update_info;
    
    std::cout << "Sending periodic spread combination update - Token1: " << update_info.Token1 
              << ", Token2: " << update_info.Token2 
              << ", DayLowPriceDiffRange: " << update_info.DayLowPriceDiffRange
              << ", DayHighPriceDiffRange: " << update_info.DayHighPriceDiffRange << std::endl;
    
    // Send broadcast via callback
    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&broadcast), sizeof(broadcast));
    }
}

// Helper methods for spread combination management
void FakeNSEExchange::add_spread_combination(int32_t token1, int32_t token2, const MS_SPD_UPDATE_INFO& combination_info) {
    std::pair<int32_t, int32_t> key = std::make_pair(token1, token2);
    spread_combinations_[key] = combination_info;
    
    std::cout << "Added spread combination: Token1=" << token1 << ", Token2=" << token2 
              << ", ReferencePrice=" << combination_info.ReferencePrice << std::endl;
    
    // Broadcast the new combination
    auto current_time = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    broadcast_spread_combination_update(combination_info, current_time);
}

void FakeNSEExchange::update_spread_combination(int32_t token1, int32_t token2, const MS_SPD_UPDATE_INFO& updated_info, uint64_t ts) {
    std::pair<int32_t, int32_t> key = std::make_pair(token1, token2);
    
    auto it = spread_combinations_.find(key);
    if (it != spread_combinations_.end()) {
        // Update existing combination
        MS_SPD_UPDATE_INFO& existing = it->second;
        
        // Update modifiable fields
        existing.ReferencePrice = updated_info.ReferencePrice;
        existing.DayLowPriceDiffRange = updated_info.DayLowPriceDiffRange;
        existing.DayHighPriceDiffRange = updated_info.DayHighPriceDiffRange;
        existing.OpLowPriceDiffRange = updated_info.OpLowPriceDiffRange;
        existing.OpHighPriceDiffRange = updated_info.OpHighPriceDiffRange;
        existing.SPDEligibility = updated_info.SPDEligibility;
        existing.DeleteFlag = updated_info.DeleteFlag;
        
        std::cout << "Updated spread combination: Token1=" << token1 << ", Token2=" << token2 
                  << ", New ReferencePrice=" << updated_info.ReferencePrice 
                  << ", New Eligibility=" << (int)updated_info.SPDEligibility.Eligibility << std::endl;
        
        // Broadcast the update
        broadcast_spread_combination_update(existing, ts);
    } else {
        std::cout << "Spread combination not found for update: Token1=" << token1 << ", Token2=" << token2 << std::endl;
        // Add as new combination
        add_spread_combination(token1, token2, updated_info);
    }
}

bool FakeNSEExchange::is_valid_spread_combination(int32_t token1, int32_t token2) const {
    std::pair<int32_t, int32_t> key = std::make_pair(token1, token2);
    auto it = spread_combinations_.find(key);
    
    if (it == spread_combinations_.end()) {
        return false;
    }
    
    const MS_SPD_UPDATE_INFO& combination = it->second;

    // Check if combination is eligible and not deleted
    bool is_eligible = (combination.SPDEligibility.Eligibility == 1);
    bool is_not_deleted = (combination.DeleteFlag != 'Y');

    return is_eligible && is_not_deleted;
}

// 2L/3L Order Helper Methods
bool FakeNSEExchange::are_quantities_matching(const MS_SPD_OE_REQUEST* req, bool is_3l) const {
    // All legs must have the same quantity
    if (req->Volume1 != req->MS_SPD_LEG_INFO_leg2.Volume2) {
        return false;
    }

    if (is_3l && req->Volume1 != req->MS_SPD_LEG_INFO_leg3.Volume2) {
        return false;
    }

    return true;
}

bool FakeNSEExchange::are_tokens_same_stream(int32_t token1, int32_t token2, int32_t token3, bool is_3l) const {
    // Extract stream from token (first 2 digits)
    int32_t stream1 = token1 / 100000000;
    int32_t stream2 = token2 / 100000000;

    if (stream1 != stream2) {
        return false;
    }

    if (is_3l) {
        int32_t stream3 = token3 / 100000000;
        if (stream1 != stream3) {
            return false;
        }
    }

    return true;
}

bool FakeNSEExchange::is_valid_2l_3l_order(const MS_SPD_OE_REQUEST* req, bool is_3l) const {
    // GTC/GTD not allowed for 2L/3L
    if (req->OrderFlags.GTC || req->GoodTillDate1 != 0) {
        return false;
    }

    // IOC must be set
    if (!req->OrderFlags.IOC) {
        return false;
    }

    // Disclosed quantity not allowed
    if (req->DisclosedVol1 > 0 || req->MS_SPD_LEG_INFO_leg2.DisclosedVol2 > 0) {
        return false;
    }

    if (is_3l && req->MS_SPD_LEG_INFO_leg3.DisclosedVol2 > 0) {
        return false;
    }

    // Contracts cannot be the same
    if (req->Token1 == req->MS_SPD_LEG_INFO_leg2.Token2) {
        return false;
    }

    if (is_3l) {
        if (req->Token1 == req->MS_SPD_LEG_INFO_leg3.Token2 ||
            req->MS_SPD_LEG_INFO_leg2.Token2 == req->MS_SPD_LEG_INFO_leg3.Token2) {
            return false;
        }
    }

    // Quantities must match
    if (!are_quantities_matching(req, is_3l)) {
        return false;
    }

    // Tokens must be from same stream
    if (!are_tokens_same_stream(req->Token1, req->MS_SPD_LEG_INFO_leg2.Token2,
                                 req->MS_SPD_LEG_INFO_leg3.Token2, is_3l)) {
        return false;
    }

    return true;
}

// 2L Order Entry Handler
void FakeNSEExchange::handle_2l_order_entry_request(const MS_SPD_OE_REQUEST* req, uint64_t ts) {
    std::cout << "2L order entry request from trader: " << req->Header.TraderId
              << " - Token1: " << req->Token1
              << ", Token2: " << req->MS_SPD_LEG_INFO_leg2.Token2 << std::endl;

    // Check if trader is logged in
    if (logged_in_traders_.find(req->Header.TraderId) == logged_in_traders_.end()) {
        std::cout << "Trader " << req->Header.TraderId << " not logged in" << std::endl;
        send_2l_order_response(req, ts, TransactionCodes::TWOL_ORDER_ERROR, ErrorCodes::USER_NOT_FOUND);
        return;
    }

    // Check if markets are open
    if (current_market_status_.Normal != 1) {
        std::cout << "Market is not open for 2L orders" << std::endl;
        send_2l_order_response(req, ts, TransactionCodes::TWOL_ORDER_ERROR, ErrorCodes::MARKET_CLOSED);
        return;
    }

    // Get broker ID
    std::string broker_id(req->BrokerId1, 5);
    broker_id.erase(broker_id.find_last_not_of(' ') + 1);

    // Check broker status
    if (is_broker_in_closeout(broker_id)) {
        std::cout << "Broker " << broker_id << " is in closeout" << std::endl;
        send_2l_order_response(req, ts, TransactionCodes::TWOL_ORDER_ERROR, ErrorCodes::CLOSEOUT_ORDER_REJECT);
        return;
    }

    if (is_broker_deactivated(broker_id)) {
        std::cout << "Broker " << broker_id << " is deactivated" << std::endl;
        send_2l_order_response(req, ts, TransactionCodes::TWOL_ORDER_ERROR, ErrorCodes::OE_IS_NOT_ACTIVE);
        return;
    }

    // Validate 2L order parameters
    if (!is_valid_2l_3l_order(req, false)) {
        std::cout << "Invalid 2L order parameters" << std::endl;

        // Check specific error conditions
        if (req->OrderFlags.GTC || req->GoodTillDate1 != 0) {
            send_2l_order_response(req, ts, TransactionCodes::TWOL_ORDER_ERROR, ErrorCodes::e$gtcgtd_not_allowed);
        } else if (!are_quantities_matching(req, false)) {
            send_2l_order_response(req, ts, TransactionCodes::TWOL_ORDER_ERROR, ErrorCodes::e$qty_should_be_same);
        } else if (!are_tokens_same_stream(req->Token1, req->MS_SPD_LEG_INFO_leg2.Token2, 0, false)) {
            send_2l_order_response(req, ts, TransactionCodes::TWOL_ORDER_ERROR, ErrorCodes::e$invalid_contract_comb);
        } else if (req->Token1 == req->MS_SPD_LEG_INFO_leg2.Token2) {
            send_2l_order_response(req, ts, TransactionCodes::TWOL_ORDER_ERROR, ErrorCodes::e$invalid_contract_comb);
        } else {
            send_2l_order_response(req, ts, TransactionCodes::TWOL_ORDER_ERROR, ErrorCodes::e$invalid_order_parameters);
        }
        return;
    }

    // Get account number
    std::string account(req->AccountNumber1, 10);
    account.erase(account.find_last_not_of(' ') + 1);

    // PRO order validation
    if (!is_valid_pro_order(req->ProClient1, account, broker_id)) {
        std::cout << "Invalid PRO order configuration" << std::endl;
        send_2l_order_response(req, ts, TransactionCodes::TWOL_ORDER_ERROR, ErrorCodes::e$invalid_pro_client);
        return;
    }

    // CLI order validation
    if (!is_valid_cli_order(req->ProClient1, account, broker_id)) {
        std::cout << "Invalid CLI order configuration" << std::endl;
        send_2l_order_response(req, ts, TransactionCodes::TWOL_ORDER_ERROR, ErrorCodes::e$invalid_cli_ac);
        return;
    }

    // Quantity validation - must be multiple of regular lot
    const int32_t REGULAR_LOT = 1;
    if (req->Volume1 % REGULAR_LOT != 0 || req->MS_SPD_LEG_INFO_leg2.Volume2 % REGULAR_LOT != 0) {
        std::cout << "Quantity must be multiple of regular lot" << std::endl;
        send_2l_order_response(req, ts, TransactionCodes::TWOL_ORDER_ERROR, ErrorCodes::OE_QUANTITY_NOT_MULT_RL);
        return;
    }

    // Process 2L order - IOC by default
    std::cout << "Processing 2L order as IOC" << std::endl;

    // Simulate order processing (70% full match, 20% partial match, 10% error)
    int outcome = rand() % 100;

    if (outcome < 70) {
        // Full match - send confirmation
        std::cout << "2L order fully matched" << std::endl;
        send_2l_order_response(req, ts, TransactionCodes::TWOL_ORDER_CONFIRMATION, ErrorCodes::SUCCESS);

    } else if (outcome < 90) {
        // Partial match - send confirmation then cancellation for unmatched
        std::cout << "2L order partially matched" << std::endl;
        send_2l_order_response(req, ts, TransactionCodes::TWOL_ORDER_CONFIRMATION, ErrorCodes::SUCCESS);

        // Send cancellation for unmatched portion
        std::cout << "Sending cancellation for unmatched portion" << std::endl;
        send_2l_order_response(req, ts, TransactionCodes::TWOL_ORDER_CXL_CONFIRMATION, ErrorCodes::SUCCESS);

    } else {
        // No match - send cancellation only
        std::cout << "2L order not matched - IOC cancellation" << std::endl;
        send_2l_order_response(req, ts, TransactionCodes::TWOL_ORDER_CXL_CONFIRMATION, ErrorCodes::SUCCESS);
    }
}

// 3L Order Entry Handler
void FakeNSEExchange::handle_3l_order_entry_request(const MS_SPD_OE_REQUEST* req, uint64_t ts) {
    std::cout << "3L order entry request from trader: " << req->Header.TraderId
              << " - Token1: " << req->Token1
              << ", Token2: " << req->MS_SPD_LEG_INFO_leg2.Token2
              << ", Token3: " << req->MS_SPD_LEG_INFO_leg3.Token2 << std::endl;

    // Check if trader is logged in
    if (logged_in_traders_.find(req->Header.TraderId) == logged_in_traders_.end()) {
        std::cout << "Trader " << req->Header.TraderId << " not logged in" << std::endl;
        send_3l_order_response(req, ts, TransactionCodes::THRL_ORDER_ERROR, ErrorCodes::USER_NOT_FOUND);
        return;
    }

    // Check if markets are open
    if (current_market_status_.Normal != 1) {
        std::cout << "Market is not open for 3L orders" << std::endl;
        send_3l_order_response(req, ts, TransactionCodes::THRL_ORDER_ERROR, ErrorCodes::MARKET_CLOSED);
        return;
    }

    // Get broker ID
    std::string broker_id(req->BrokerId1, 5);
    broker_id.erase(broker_id.find_last_not_of(' ') + 1);

    // Check broker status
    if (is_broker_in_closeout(broker_id)) {
        std::cout << "Broker " << broker_id << " is in closeout" << std::endl;
        send_3l_order_response(req, ts, TransactionCodes::THRL_ORDER_ERROR, ErrorCodes::CLOSEOUT_ORDER_REJECT);
        return;
    }

    if (is_broker_deactivated(broker_id)) {
        std::cout << "Broker " << broker_id << " is deactivated" << std::endl;
        send_3l_order_response(req, ts, TransactionCodes::THRL_ORDER_ERROR, ErrorCodes::OE_IS_NOT_ACTIVE);
        return;
    }

    // Validate 3L order parameters
    if (!is_valid_2l_3l_order(req, true)) {
        std::cout << "Invalid 3L order parameters" << std::endl;

        // Check specific error conditions
        if (req->OrderFlags.GTC || req->GoodTillDate1 != 0) {
            send_3l_order_response(req, ts, TransactionCodes::THRL_ORDER_ERROR, ErrorCodes::e$gtcgtd_not_allowed);
        } else if (!are_quantities_matching(req, true)) {
            send_3l_order_response(req, ts, TransactionCodes::THRL_ORDER_ERROR, ErrorCodes::e$qty_should_be_same);
        } else if (!are_tokens_same_stream(req->Token1, req->MS_SPD_LEG_INFO_leg2.Token2, req->MS_SPD_LEG_INFO_leg3.Token2, true)) {
            send_3l_order_response(req, ts, TransactionCodes::THRL_ORDER_ERROR, ErrorCodes::e$invalid_contract_comb);
        } else if (req->Token1 == req->MS_SPD_LEG_INFO_leg2.Token2 ||
                   req->Token1 == req->MS_SPD_LEG_INFO_leg3.Token2 ||
                   req->MS_SPD_LEG_INFO_leg2.Token2 == req->MS_SPD_LEG_INFO_leg3.Token2) {
            send_3l_order_response(req, ts, TransactionCodes::THRL_ORDER_ERROR, ErrorCodes::e$invalid_contract_comb);
        } else {
            send_3l_order_response(req, ts, TransactionCodes::THRL_ORDER_ERROR, ErrorCodes::e$invalid_order_parameters);
        }
        return;
    }

    // Get account number
    std::string account(req->AccountNumber1, 10);
    account.erase(account.find_last_not_of(' ') + 1);

    // PRO order validation
    if (!is_valid_pro_order(req->ProClient1, account, broker_id)) {
        std::cout << "Invalid PRO order configuration" << std::endl;
        send_3l_order_response(req, ts, TransactionCodes::THRL_ORDER_ERROR, ErrorCodes::e$invalid_pro_client);
        return;
    }

    // CLI order validation
    if (!is_valid_cli_order(req->ProClient1, account, broker_id)) {
        std::cout << "Invalid CLI order configuration" << std::endl;
        send_3l_order_response(req, ts, TransactionCodes::THRL_ORDER_ERROR, ErrorCodes::e$invalid_cli_ac);
        return;
    }

    // Quantity validation - must be multiple of regular lot
    const int32_t REGULAR_LOT = 1;
    if (req->Volume1 % REGULAR_LOT != 0 ||
        req->MS_SPD_LEG_INFO_leg2.Volume2 % REGULAR_LOT != 0 ||
        req->MS_SPD_LEG_INFO_leg3.Volume2 % REGULAR_LOT != 0) {
        std::cout << "Quantity must be multiple of regular lot" << std::endl;
        send_3l_order_response(req, ts, TransactionCodes::THRL_ORDER_ERROR, ErrorCodes::OE_QUANTITY_NOT_MULT_RL);
        return;
    }

    // Process 3L order - IOC by default
    std::cout << "Processing 3L order as IOC" << std::endl;

    // Simulate order processing (70% full match, 20% partial match, 10% error)
    int outcome = rand() % 100;

    if (outcome < 70) {
        // Full match - send confirmation
        std::cout << "3L order fully matched" << std::endl;
        send_3l_order_response(req, ts, TransactionCodes::THRL_ORDER_CONFIRMATION, ErrorCodes::SUCCESS);

    } else if (outcome < 90) {
        // Partial match - send confirmation then cancellation for unmatched
        std::cout << "3L order partially matched" << std::endl;
        send_3l_order_response(req, ts, TransactionCodes::THRL_ORDER_CONFIRMATION, ErrorCodes::SUCCESS);

        // Send cancellation for unmatched portion
        std::cout << "Sending cancellation for unmatched portion" << std::endl;
        send_3l_order_response(req, ts, TransactionCodes::THRL_ORDER_CXL_CONFIRMATION, ErrorCodes::SUCCESS);

    } else {
        // No match - send cancellation only
        std::cout << "3L order not matched - IOC cancellation" << std::endl;
        send_3l_order_response(req, ts, TransactionCodes::THRL_ORDER_CXL_CONFIRMATION, ErrorCodes::SUCCESS);
    }
}

// 2L Order Response Sender
void FakeNSEExchange::send_2l_order_response(const MS_SPD_OE_REQUEST* req, uint64_t ts, int16_t transaction_code, int16_t error_code, int16_t reason_code) {
    MS_SPD_OE_REQUEST response;
    memset(&response, 0, sizeof(response));

    // Copy original request structure and modify header
    response = *req;
    response.Header.TransactionCode = transaction_code;
    response.Header.ErrorCode = error_code;
    response.Header.MessageLength = sizeof(MS_SPD_OE_REQUEST);

    // Set reason code
    response.ReasonCode1 = reason_code;

    // Generate order number and activity reference for confirmed orders
    if (transaction_code == TransactionCodes::TWOL_ORDER_CONFIRMATION) {
        response.OrderNumber1 = generate_order_number(ts);
        response.EntryDateTime1 = static_cast<int32_t>(ts / 1000000);
        response.LastModified1 = static_cast<int32_t>(ts / 1000000);
        response.LastActivityReference = generate_activity_reference(ts);

        // Simulate partial fill (50% of cases when matched)
        if (rand() % 2 == 0) {
            response.VolumeFilledToday1 = response.Volume1 / 2;
            response.TotalVolRemaining1 = response.Volume1 - response.VolumeFilledToday1;
            response.MS_SPD_LEG_INFO_leg2.VolumeFilledToday2 = response.MS_SPD_LEG_INFO_leg2.Volume2 / 2;
            response.MS_SPD_LEG_INFO_leg2.TotalVolRemaining2 = response.MS_SPD_LEG_INFO_leg2.Volume2 - response.MS_SPD_LEG_INFO_leg2.VolumeFilledToday2;
            response.OrderFlags.Traded = 1;
        } else {
            // Full fill
            response.VolumeFilledToday1 = response.Volume1;
            response.TotalVolRemaining1 = 0;
            response.MS_SPD_LEG_INFO_leg2.VolumeFilledToday2 = response.MS_SPD_LEG_INFO_leg2.Volume2;
            response.MS_SPD_LEG_INFO_leg2.TotalVolRemaining2 = 0;
            response.OrderFlags.Traded = 1;
        }

        std::cout << "Generated 2L order number: " << response.OrderNumber1 << std::endl;
    }

    // Handle cancellation response
    if (transaction_code == TransactionCodes::TWOL_ORDER_CXL_CONFIRMATION) {
        response.LastModified1 = static_cast<int32_t>(ts / 1000000);
        response.TotalVolRemaining1 = 0;  // All cancelled
        response.MS_SPD_LEG_INFO_leg2.TotalVolRemaining2 = 0;
    }

    // Log the response
    std::cout << "Sending 2L order response: TransactionCode=" << transaction_code
              << ", ErrorCode=" << error_code
              << ", ReasonCode=" << reason_code;

    if (transaction_code == TransactionCodes::TWOL_ORDER_CONFIRMATION) {
        std::cout << ", OrderNumber=" << response.OrderNumber1;
    }

    std::cout << std::endl;

    // Send response via callback
    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&response), sizeof(response));
    }
}

// 3L Order Response Sender
void FakeNSEExchange::send_3l_order_response(const MS_SPD_OE_REQUEST* req, uint64_t ts, int16_t transaction_code, int16_t error_code, int16_t reason_code) {
    MS_SPD_OE_REQUEST response;
    memset(&response, 0, sizeof(response));

    // Copy original request structure and modify header
    response = *req;
    response.Header.TransactionCode = transaction_code;
    response.Header.ErrorCode = error_code;
    response.Header.MessageLength = sizeof(MS_SPD_OE_REQUEST);

    // Set reason code
    response.ReasonCode1 = reason_code;

    // Generate order number and activity reference for confirmed orders
    if (transaction_code == TransactionCodes::THRL_ORDER_CONFIRMATION) {
        response.OrderNumber1 = generate_order_number(ts);
        response.EntryDateTime1 = static_cast<int32_t>(ts / 1000000);
        response.LastModified1 = static_cast<int32_t>(ts / 1000000);
        response.LastActivityReference = generate_activity_reference(ts);

        // Simulate partial fill (50% of cases when matched)
        if (rand() % 2 == 0) {
            response.VolumeFilledToday1 = response.Volume1 / 2;
            response.TotalVolRemaining1 = response.Volume1 - response.VolumeFilledToday1;
            response.MS_SPD_LEG_INFO_leg2.VolumeFilledToday2 = response.MS_SPD_LEG_INFO_leg2.Volume2 / 2;
            response.MS_SPD_LEG_INFO_leg2.TotalVolRemaining2 = response.MS_SPD_LEG_INFO_leg2.Volume2 - response.MS_SPD_LEG_INFO_leg2.VolumeFilledToday2;
            response.MS_SPD_LEG_INFO_leg3.VolumeFilledToday2 = response.MS_SPD_LEG_INFO_leg3.Volume2 / 2;
            response.MS_SPD_LEG_INFO_leg3.TotalVolRemaining2 = response.MS_SPD_LEG_INFO_leg3.Volume2 - response.MS_SPD_LEG_INFO_leg3.VolumeFilledToday2;
            response.OrderFlags.Traded = 1;
        } else {
            // Full fill
            response.VolumeFilledToday1 = response.Volume1;
            response.TotalVolRemaining1 = 0;
            response.MS_SPD_LEG_INFO_leg2.VolumeFilledToday2 = response.MS_SPD_LEG_INFO_leg2.Volume2;
            response.MS_SPD_LEG_INFO_leg2.TotalVolRemaining2 = 0;
            response.MS_SPD_LEG_INFO_leg3.VolumeFilledToday2 = response.MS_SPD_LEG_INFO_leg3.Volume2;
            response.MS_SPD_LEG_INFO_leg3.TotalVolRemaining2 = 0;
            response.OrderFlags.Traded = 1;
        }

        std::cout << "Generated 3L order number: " << response.OrderNumber1 << std::endl;
    }

    // Handle cancellation response
    if (transaction_code == TransactionCodes::THRL_ORDER_CXL_CONFIRMATION) {
        response.LastModified1 = static_cast<int32_t>(ts / 1000000);
        response.TotalVolRemaining1 = 0;  // All cancelled
        response.MS_SPD_LEG_INFO_leg2.TotalVolRemaining2 = 0;
        response.MS_SPD_LEG_INFO_leg3.TotalVolRemaining2 = 0;
    }

    // Log the response
    std::cout << "Sending 3L order response: TransactionCode=" << transaction_code
              << ", ErrorCode=" << error_code
              << ", ReasonCode=" << reason_code;

    if (transaction_code == TransactionCodes::THRL_ORDER_CONFIRMATION) {
        std::cout << ", OrderNumber=" << response.OrderNumber1;
    }

    std::cout << std::endl;

    // Send response via callback
    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&response), sizeof(response));
    }
}

// ===== Chapter 7: Unsolicited Messages Implementation =====

// Send Stop Loss Notification (Transaction Code 2212)
void FakeNSEExchange::send_stop_loss_notification(const MS_OE_REQUEST& order, uint64_t ts) {
    MS_TRADE_CONFIRM notification;
    memset(&notification, 0, sizeof(notification));

    // Set header
    notification.Header.TransactionCode = TransactionCodes::ON_STOP_NOTIFICATION;
    notification.Header.LogTime = static_cast<int32_t>(ts / 1000000);
    notification.Header.TraderId = order.TraderId;
    notification.Header.ErrorCode = 0;
    notification.Header.Timestamp = ts;
    notification.Header.MessageLength = sizeof(MS_TRADE_CONFIRM);

    // Set order details
    notification.ResponseOrderNumber = order.OrderNumber;
    memcpy(notification.BrokerId, order.BrokerId, sizeof(notification.BrokerId));
    notification.TraderNumber = order.TraderId;
    memcpy(notification.AccountNumber, order.AccountNumber, sizeof(notification.AccountNumber));
    notification.BuySellIndicator = order.BuySellIndicator;
    notification.OriginalVolume = order.Volume;
    notification.DisclosedVolume = order.DisclosedVolume;
    notification.RemainingVolume = order.TotalVolumeRemaining;
    notification.DisclosedVolumeRemaining = order.DisclosedVolumeRemaining;
    notification.Price = order.Price;
    notification.OrderFlags = order.OrderFlags;
    notification.OrderFlags.SL = 1;  // Mark as Stop Loss trigger
    notification.GoodTillDate = order.GoodTillDate;
    notification.VolumeFilledToday = order.VolumeFilledToday;
    notification.ActivityType[0] = order.BuySellIndicator == 1 ? 'B' : 'S';
    notification.ActivityType[1] = '\0';
    notification.ActivityTime = static_cast<int32_t>(ts / 1000000);
    notification.Token = order.TokenNo;
    notification.ContractDesc = order.ContractDesc;
    notification.OpenClose = order.OpenClose;
    notification.BookType = order.BookType;
    memcpy(notification.Participant, order.Settlor, sizeof(notification.Participant));
    notification.AdditionalOrderFlags = order.AdditionalOrderFlags;
    memcpy(notification.PAN, order.PAN, sizeof(notification.PAN));
    notification.AlgoID = order.AlgoID;
    notification.LastActivityReference = ts;

    std::cout << "Sending Stop Loss notification for order " << order.OrderNumber << std::endl;

    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&notification), sizeof(notification));
    }
}

// Send Market If Touched Notification (Transaction Code 2212)
void FakeNSEExchange::send_mit_notification(const MS_OE_REQUEST& order, uint64_t ts) {
    MS_TRADE_CONFIRM notification;
    memset(&notification, 0, sizeof(notification));

    // Set header
    notification.Header.TransactionCode = TransactionCodes::ON_STOP_NOTIFICATION;
    notification.Header.LogTime = static_cast<int32_t>(ts / 1000000);
    notification.Header.TraderId = order.TraderId;
    notification.Header.ErrorCode = 0;
    notification.Header.Timestamp = ts;
    notification.Header.MessageLength = sizeof(MS_TRADE_CONFIRM);

    // Set order details
    notification.ResponseOrderNumber = order.OrderNumber;
    memcpy(notification.BrokerId, order.BrokerId, sizeof(notification.BrokerId));
    notification.TraderNumber = order.TraderId;
    memcpy(notification.AccountNumber, order.AccountNumber, sizeof(notification.AccountNumber));
    notification.BuySellIndicator = order.BuySellIndicator;
    notification.OriginalVolume = order.Volume;
    notification.DisclosedVolume = order.DisclosedVolume;
    notification.RemainingVolume = order.TotalVolumeRemaining;
    notification.DisclosedVolumeRemaining = order.DisclosedVolumeRemaining;
    notification.Price = order.Price;
    notification.OrderFlags = order.OrderFlags;
    notification.OrderFlags.MIT = 1;  // Mark as MIT trigger
    notification.GoodTillDate = order.GoodTillDate;
    notification.VolumeFilledToday = order.VolumeFilledToday;
    notification.ActivityType[0] = order.BuySellIndicator == 1 ? 'B' : 'S';
    notification.ActivityType[1] = '\0';
    notification.ActivityTime = static_cast<int32_t>(ts / 1000000);
    notification.Token = order.TokenNo;
    notification.ContractDesc = order.ContractDesc;
    notification.OpenClose = order.OpenClose;
    notification.BookType = order.BookType;
    memcpy(notification.Participant, order.Settlor, sizeof(notification.Participant));
    notification.AdditionalOrderFlags = order.AdditionalOrderFlags;
    memcpy(notification.PAN, order.PAN, sizeof(notification.PAN));
    notification.AlgoID = order.AlgoID;
    notification.LastActivityReference = ts;

    std::cout << "Sending MIT notification for order " << order.OrderNumber << std::endl;

    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&notification), sizeof(notification));
    }
}

// Send Freeze Approval (Transaction Code 2073 - ORDER_CONFIRMATION_OUT)
void FakeNSEExchange::send_freeze_approval(const MS_OE_REQUEST& order, uint64_t ts) {
    MS_OE_REQUEST response;
    memcpy(&response, &order, sizeof(MS_OE_REQUEST));

    // Update header for freeze approval
    response.Header.TransactionCode = TransactionCodes::ORDER_CONFIRMATION_OUT;
    response.Header.LogTime = static_cast<int32_t>(ts / 1000000);
    response.Header.ErrorCode = 0;
    response.Header.Timestamp = ts;
    response.LastModified = static_cast<int32_t>(ts / 1000000);
    response.LastActivityReference = ts;

    std::cout << "Sending freeze approval for order " << order.OrderNumber
              << " (reason: " << order.ReasonCode << ")" << std::endl;

    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&response), sizeof(response));
    }
}

// Send Trade Confirmation (Transaction Code 2222)
void FakeNSEExchange::send_trade_confirmation(const MS_TRADE_CONFIRM& trade, uint64_t ts) {
    MS_TRADE_CONFIRM confirmation;
    memcpy(&confirmation, &trade, sizeof(MS_TRADE_CONFIRM));

    // Set header for trade confirmation
    confirmation.Header.TransactionCode = TransactionCodes::TRADE_CONFIRMATION;
    confirmation.Header.LogTime = static_cast<int32_t>(ts / 1000000);
    confirmation.Header.ErrorCode = 0;
    confirmation.Header.Timestamp = ts;
    confirmation.Header.MessageLength = sizeof(MS_TRADE_CONFIRM);

    // Mark as traded
    confirmation.OrderFlags.Traded = 1;
    confirmation.ActivityType[0] = 'B';  // Or 'S' based on buy/sell
    confirmation.ActivityType[1] = '\0';
    confirmation.ActivityTime = static_cast<int32_t>(ts / 1000000);
    confirmation.LastActivityReference = ts;

    std::cout << "Sending trade confirmation: Fill #" << confirmation.FillNumber
              << ", Qty=" << confirmation.FillQuantity
              << ", Price=" << confirmation.FillPrice << std::endl;

    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&confirmation), sizeof(confirmation));
    }
}

// Send Trade Modification Confirmation (Transaction Code 2287)
void FakeNSEExchange::send_trade_modification_confirmation(const MS_TRADE_CONFIRM& trade, uint64_t ts) {
    MS_TRADE_CONFIRM confirmation;
    memcpy(&confirmation, &trade, sizeof(MS_TRADE_CONFIRM));

    confirmation.Header.TransactionCode = TransactionCodes::TRADE_MODIFY_CONFIRM;
    confirmation.Header.LogTime = static_cast<int32_t>(ts / 1000000);
    confirmation.Header.ErrorCode = 0;
    confirmation.Header.Timestamp = ts;
    confirmation.ActivityType[0] = 'T';
    confirmation.ActivityType[1] = 'M';
    confirmation.ActivityTime = static_cast<int32_t>(ts / 1000000);
    confirmation.LastActivityReference = ts;

    std::cout << "Sending trade modification confirmation for fill #" << confirmation.FillNumber << std::endl;

    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&confirmation), sizeof(confirmation));
    }
}

// Send Trade Modification Rejection (Transaction Code 2288)
void FakeNSEExchange::send_trade_modification_rejection(const MS_TRADE_CONFIRM& trade, int16_t error_code, uint64_t ts) {
    MS_TRADE_CONFIRM rejection;
    memcpy(&rejection, &trade, sizeof(MS_TRADE_CONFIRM));

    rejection.Header.TransactionCode = TransactionCodes::TRADE_MODIFY_REJECT;
    rejection.Header.LogTime = static_cast<int32_t>(ts / 1000000);
    rejection.Header.ErrorCode = error_code;
    rejection.Header.Timestamp = ts;

    std::cout << "Sending trade modification rejection for fill #" << rejection.FillNumber
              << " with error code " << error_code << std::endl;

    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&rejection), sizeof(rejection));
    }
}

// Send Trade Cancellation Confirmation (Transaction Code 2282)
void FakeNSEExchange::send_trade_cancellation_confirmation(const MS_TRADE_CONFIRM& trade, uint64_t ts) {
    MS_TRADE_CONFIRM confirmation;
    memcpy(&confirmation, &trade, sizeof(MS_TRADE_CONFIRM));

    confirmation.Header.TransactionCode = TransactionCodes::TRADE_CANCEL_CONFIRM;
    confirmation.Header.LogTime = static_cast<int32_t>(ts / 1000000);
    confirmation.Header.ErrorCode = 0;
    confirmation.Header.Timestamp = ts;
    confirmation.ActivityType[0] = 'T';
    confirmation.ActivityType[1] = 'C';
    confirmation.ActivityTime = static_cast<int32_t>(ts / 1000000);

    std::cout << "Sending trade cancellation confirmation for fill #" << confirmation.FillNumber << std::endl;

    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&confirmation), sizeof(confirmation));
    }
}

// Send Trade Cancellation Rejection (Transaction Code 2286)
void FakeNSEExchange::send_trade_cancellation_rejection(const MS_TRADE_CONFIRM& trade, int16_t error_code, uint64_t ts) {
    MS_TRADE_CONFIRM rejection;
    memcpy(&rejection, &trade, sizeof(MS_TRADE_CONFIRM));

    rejection.Header.TransactionCode = TransactionCodes::TRADE_CANCEL_REJECT;
    rejection.Header.LogTime = static_cast<int32_t>(ts / 1000000);
    rejection.Header.ErrorCode = error_code;
    rejection.Header.Timestamp = ts;

    std::cout << "Sending trade cancellation rejection for fill #" << rejection.FillNumber
              << " with error code " << error_code << std::endl;

    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&rejection), sizeof(rejection));
    }
}

// Send User Order Limit Update (Transaction Code 5731)
void FakeNSEExchange::send_user_order_limit_update(const MS_ORDER_VAL_LIMIT_DATA& limit_data, uint64_t ts) {
    MS_ORDER_VAL_LIMIT_DATA update;
    memcpy(&update, &limit_data, sizeof(MS_ORDER_VAL_LIMIT_DATA));

    update.Header.TransactionCode = TransactionCodes::USER_ORDER_LIMIT_UPDATE_OUT;
    update.Header.LogTime = static_cast<int32_t>(ts / 1000000);
    update.Header.ErrorCode = 0;
    update.Header.Timestamp = ts;
    update.Header.MessageLength = sizeof(MS_ORDER_VAL_LIMIT_DATA);

    std::cout << "Sending user order limit update for user " << update.UserId << std::endl;

    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&update), sizeof(update));
    }
}

// Send Dealer Limit Update (Transaction Code 5733)
void FakeNSEExchange::send_dealer_limit_update(const DEALER_ORD_LMT& limit_data, uint64_t ts) {
    DEALER_ORD_LMT update;
    memcpy(&update, &limit_data, sizeof(DEALER_ORD_LMT));

    update.Header.TransactionCode = TransactionCodes::DEALER_LIMIT_UPDATE_OUT;
    update.Header.LogTime = static_cast<int32_t>(ts / 1000000);
    update.Header.ErrorCode = 0;
    update.Header.Timestamp = ts;
    update.Header.MessageLength = sizeof(DEALER_ORD_LMT);

    std::cout << "Sending dealer limit update for user " << update.UserId << std::endl;

    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&update), sizeof(update));
    }
}

// Send Spread Order Limit Update (Transaction Code 5772)
void FakeNSEExchange::send_spread_order_limit_update(const SPD_ORD_LMT& limit_data, uint64_t ts) {
    SPD_ORD_LMT update;
    memcpy(&update, &limit_data, sizeof(SPD_ORD_LMT));

    update.Header.TransactionCode = TransactionCodes::SPD_ORD_LIMIT_UPDATE_OUT;
    update.Header.LogTime = static_cast<int32_t>(ts / 1000000);
    update.Header.ErrorCode = 0;
    update.Header.Timestamp = ts;
    update.Header.MessageLength = sizeof(SPD_ORD_LMT);

    std::cout << "Sending spread order limit update for user " << update.UserId << std::endl;

    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&update), sizeof(update));
    }
}

// Send Control Message to Trader (Transaction Code 5295)
void FakeNSEExchange::send_control_message(int32_t trader_id, const char* action_code, const std::string& message, uint64_t ts) {
    MS_TRADER_INT_MSG msg;
    memset(&msg, 0, sizeof(msg));

    msg.Header.TransactionCode = TransactionCodes::CTRL_MSG_TO_TRADER;
    msg.Header.LogTime = static_cast<int32_t>(ts / 1000000);
    msg.Header.TraderId = trader_id;
    msg.Header.ErrorCode = 0;
    msg.Header.Timestamp = ts;
    msg.Header.MessageLength = sizeof(MS_TRADER_INT_MSG);

    msg.TraderId = trader_id;
    strncpy(msg.ActionCode, action_code, 3);
    msg.BroadCastMessageLength = std::min(static_cast<int>(message.length()), 239);
    strncpy(msg.BroadCastMessage, message.c_str(), 239);

    std::cout << "Sending control message to trader " << trader_id
              << " (action: " << action_code << "): " << message << std::endl;

    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&msg), sizeof(msg));
    }
}

// Send Broadcast Message (Transaction Code 6501)
void FakeNSEExchange::send_broadcast_message(const char* broker_id, const char* action_code, const std::string& message, uint64_t ts) {
    MS_BCAST_MESSAGE msg;
    memset(&msg, 0, sizeof(msg));

    msg.Header.TransactionCode = TransactionCodes::BCAST_JRNL_VCT_MSG;
    msg.Header.LogTime = static_cast<int32_t>(ts / 1000000);
    msg.Header.ErrorCode = 0;
    msg.Header.MessageLength = sizeof(MS_BCAST_MESSAGE);

    memcpy(msg.BrokerNumber, broker_id, std::min(strlen(broker_id), size_t(5)));
    strncpy(msg.ActionCode, action_code, 3);
    msg.BCASTDestination.TraderWorkstation = 1;
    msg.BCASTDestination.JournalingRequired = 1;
    msg.BroadcastMessageLength = std::min(static_cast<int>(message.length()), 239);
    strncpy(msg.BroadcastMessage, message.c_str(), 239);

    std::cout << "Sending broadcast message (action: " << action_code << "): " << message << std::endl;

    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&msg), sizeof(msg));
    }
}

// Send Batch Order Cancel (Transaction Code 9002)
void FakeNSEExchange::send_batch_order_cancel(const MS_OE_REQUEST& order, uint64_t ts) {
    MS_OE_REQUEST response;
    memcpy(&response, &order, sizeof(MS_OE_REQUEST));

    response.Header.TransactionCode = TransactionCodes::BATCH_ORDER_CANCEL;
    response.Header.LogTime = static_cast<int32_t>(ts / 1000000);
    response.Header.ErrorCode = 0;
    response.Header.Timestamp = ts;
    response.LastModified = static_cast<int32_t>(ts / 1000000);
    response.LastActivityReference = ts;

    std::cout << "Sending batch order cancellation for order " << order.OrderNumber << std::endl;

    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&response), sizeof(response));
    }
}

// Send Batch Spread Cancel (Transaction Code 9004)
void FakeNSEExchange::send_batch_spread_cancel(const MS_SPD_OE_REQUEST& order, uint64_t ts) {
    MS_SPD_OE_REQUEST response;
    memcpy(&response, &order, sizeof(MS_SPD_OE_REQUEST));

    response.Header.TransactionCode = TransactionCodes::BATCH_SPREAD_CXL_OUT;
    response.Header.LogTime = static_cast<int32_t>(ts / 1000000);
    response.Header.ErrorCode = 0;
    response.Header.Timestamp = ts;
    response.LastModified1 = static_cast<int32_t>(ts / 1000000);
    response.LastActivityReference = ts;

    std::cout << "Sending batch spread cancellation for spread order " << order.OrderNumber1 << std::endl;

    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&response), sizeof(response));
    }
}
// ===== Chapter 8: Bhavcopy Implementation =====

// Send Bhavcopy Start Notification
void FakeNSEExchange::send_bhavcopy_start_notification(uint64_t ts, bool is_spread) {
    MS_BCAST_MESSAGE msg;
    memset(&msg, 0, sizeof(msg));

    msg.Header.TransactionCode = TransactionCodes::BCAST_JRNL_VCT_MSG;
    msg.Header.LogTime = static_cast<int32_t>(ts / 1000000);
    msg.Header.ErrorCode = 0;
    msg.Header.MessageLength = sizeof(MS_BCAST_MESSAGE);

    msg.BCASTDestination.TraderWorkstation = 1;
    msg.BCASTDestination.JournalingRequired = 1;

    std::string message = is_spread ?
        "Spread bhavcopy transmission will start now" :
        "Bhavcopy transmission will start now";

    msg.BroadcastMessageLength = std::min(static_cast<int>(message.length()), 239);
    strncpy(msg.BroadcastMessage, message.c_str(), 239);

    std::cout << "Sending bhavcopy start notification" << (is_spread ? " (spread)" : "") << std::endl;

    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&msg), sizeof(msg));
    }
}

// Send Bhavcopy Header
void FakeNSEExchange::send_bhavcopy_header(char session_type, int32_t report_date, uint64_t ts, bool is_spread) {
    MS_RP_HDR_RPRT_MARKET_STATS_OUT_RPT header;
    memset(&header, 0, sizeof(header));

    header.Header.TransactionCode = is_spread ?
        TransactionCodes::SPD_BC_JRNL_VCT_MSG :
        TransactionCodes::RPRT_MARKET_STATS_OUT_RPT;
    header.Header.LogTime = static_cast<int32_t>(ts / 1000000);
    header.Header.ErrorCode = 0;
    header.Header.Timestamp = ts;
    header.Header.MessageLength = sizeof(MS_RP_HDR_RPRT_MARKET_STATS_OUT_RPT);

    header.MessageType = session_type;
    header.ReportDate = report_date;
    header.UserType = -1;

    std::cout << "Sending bhavcopy header (session: " << session_type << ")" << std::endl;

    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&header), sizeof(header));
    }
}

// Send Bhavcopy Data (Regular or Enhanced)
void FakeNSEExchange::send_bhavcopy_data(char session_type, const std::vector<MKT_STATS_DATA>& stats, uint64_t ts, bool enhanced) {
    char data_type;
    switch(session_type) {
        case BhavcopyMessageTypes::HEADER_REGULAR:
            data_type = BhavcopyMessageTypes::DATA_REGULAR;
            break;
        case BhavcopyMessageTypes::HEADER_ADDITIONAL:
            data_type = BhavcopyMessageTypes::DATA_ADDITIONAL;
            break;
        case BhavcopyMessageTypes::HEADER_FINAL:
            data_type = BhavcopyMessageTypes::DATA_FINAL;
            break;
        default:
            data_type = BhavcopyMessageTypes::DATA_REGULAR;
    }

    if (enhanced) {
        size_t max_records = 4;
        for (size_t i = 0; i < stats.size(); i += max_records) {
            ENHNCD_MS_RP_MARKET_STATS packet;
            memset(&packet, 0, sizeof(packet));

            packet.Header.TransactionCode = TransactionCodes::ENHNCD_RPRT_MARKET_STATS_OUT_RPT;
            packet.Header.LogTime = static_cast<int32_t>(ts / 1000000);
            packet.Header.ErrorCode = 0;
            packet.Header.Timestamp = ts;
            packet.Header.MessageLength = sizeof(ENHNCD_MS_RP_MARKET_STATS);

            packet.MessageType = data_type;
            packet.NumberOfRecords = std::min(max_records, stats.size() - i);

            for (size_t j = 0; j < packet.NumberOfRecords && (i + j) < stats.size(); j++) {
                const MKT_STATS_DATA& src = stats[i + j];
                ENHNCD_MKT_STATS_DATA& dst = packet.MarketStatsData[j];

                dst.ContractDesc = src.ContractDesc;
                dst.MarketType = src.MarketType;
                dst.OpenPrice = src.OpenPrice;
                dst.HighPrice = src.HighPrice;
                dst.LowPrice = src.LowPrice;
                dst.ClosingPrice = src.ClosingPrice;
                dst.TotalQuantityTraded = src.TotalQuantityTraded;
                dst.TotalValueTraded = src.TotalValueTraded;
                dst.PreviousClosePrice = src.PreviousClosePrice;
                dst.OpenInterest = src.OpenInterest;
                dst.ChgOpenInterest = src.ChgOpenInterest;
                memcpy(dst.Indicator, src.Indicator, 4);
            }

            if (message_callback_) {
                message_callback_(reinterpret_cast<const uint8_t*>(&packet), sizeof(packet));
            }
        }
    } else {
        for (const auto& stat : stats) {
            MS_RP_MARKET_STATS packet;
            memset(&packet, 0, sizeof(packet));

            packet.Header.TransactionCode = TransactionCodes::RPRT_MARKET_STATS_OUT_RPT;
            packet.Header.LogTime = static_cast<int32_t>(ts / 1000000);
            packet.Header.ErrorCode = 0;
            packet.Header.Timestamp = ts;
            packet.Header.MessageLength = sizeof(MS_RP_MARKET_STATS);

            packet.MessageType = data_type;
            packet.NumberOfRecords = 1;
            packet.MarketStatsData = stat;

            if (message_callback_) {
                message_callback_(reinterpret_cast<const uint8_t*>(&packet), sizeof(packet));
            }
        }
    }

    std::cout << "Sent bhavcopy data: " << stats.size() << " records" << std::endl;
}

// Send Bhavcopy Trailer
void FakeNSEExchange::send_bhavcopy_trailer(char session_type, int32_t packet_count, uint64_t ts, bool is_spread) {
    MS_RP_TRAILER_RPRT_MARKET_STATS_OUT_RPT trailer;
    memset(&trailer, 0, sizeof(trailer));

    trailer.Header.TransactionCode = is_spread ?
        TransactionCodes::SPD_BC_JRNL_VCT_MSG :
        TransactionCodes::RPRT_MARKET_STATS_OUT_RPT;
    trailer.Header.LogTime = static_cast<int32_t>(ts / 1000000);
    trailer.Header.ErrorCode = 0;
    trailer.Header.Timestamp = ts;
    trailer.Header.MessageLength = sizeof(MS_RP_TRAILER_RPRT_MARKET_STATS_OUT_RPT);

    char trailer_type;
    switch(session_type) {
        case BhavcopyMessageTypes::HEADER_REGULAR:
            trailer_type = BhavcopyMessageTypes::TRAILER_REGULAR;
            break;
        case BhavcopyMessageTypes::HEADER_ADDITIONAL:
            trailer_type = BhavcopyMessageTypes::TRAILER_ADDITIONAL;
            break;
        case BhavcopyMessageTypes::HEADER_FINAL:
            trailer_type = BhavcopyMessageTypes::TRAILER_FINAL;
            break;
        default:
            trailer_type = BhavcopyMessageTypes::TRAILER_REGULAR;
    }

    trailer.MessageType = trailer_type;
    trailer.NumberOfPackets = packet_count;

    std::cout << "Sending bhavcopy trailer (packets: " << packet_count << ")" << std::endl;

    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&trailer), sizeof(trailer));
    }
}

// Send Spread Bhavcopy Data
void FakeNSEExchange::send_spread_bhavcopy_data(char session_type, const std::vector<SPD_STATS_DATA>& stats, uint64_t ts) {
    char data_type;
    switch(session_type) {
        case BhavcopyMessageTypes::HEADER_REGULAR:
            data_type = BhavcopyMessageTypes::DATA_REGULAR;
            break;
        case BhavcopyMessageTypes::HEADER_ADDITIONAL:
            data_type = BhavcopyMessageTypes::DATA_ADDITIONAL;
            break;
        case BhavcopyMessageTypes::HEADER_FINAL:
            data_type = BhavcopyMessageTypes::DATA_FINAL;
            break;
        default:
            data_type = BhavcopyMessageTypes::DATA_REGULAR;
    }

    size_t max_records = 3;
    for (size_t i = 0; i < stats.size(); i += max_records) {
        RP_SPD_MKT_STATS packet;
        memset(&packet, 0, sizeof(packet));

        packet.Header.TransactionCode = TransactionCodes::SPD_BC_JRNL_VCT_MSG;
        packet.Header.LogTime = static_cast<int32_t>(ts / 1000000);
        packet.Header.ErrorCode = 0;
        packet.Header.Timestamp = ts;
        packet.Header.MessageLength = sizeof(RP_SPD_MKT_STATS);

        packet.MessageType = data_type;
        packet.NoOfRecords = std::min(max_records, stats.size() - i);

        for (size_t j = 0; j < packet.NoOfRecords && (i + j) < stats.size(); j++) {
            memcpy(&packet.SPDStatsData + j, &stats[i + j], sizeof(SPD_STATS_DATA));
        }

        if (message_callback_) {
            message_callback_(reinterpret_cast<const uint8_t*>(&packet), sizeof(packet));
        }
    }

    std::cout << "Sent spread bhavcopy data: " << stats.size() << " records" << std::endl;
}

// Send Spread Bhavcopy Success
void FakeNSEExchange::send_spread_bhavcopy_success(uint64_t ts) {
    MS_BCAST_MESSAGE msg;
    memset(&msg, 0, sizeof(msg));

    msg.Header.TransactionCode = TransactionCodes::BCAST_JRNL_VCT_MSG;
    msg.Header.LogTime = static_cast<int32_t>(ts / 1000000);
    msg.Header.ErrorCode = 0;
    msg.Header.MessageLength = sizeof(MS_BCAST_MESSAGE);

    msg.BCASTDestination.TraderWorkstation = 1;
    msg.BCASTDestination.JournalingRequired = 1;

    std::string message = "Spread bhavcopy broadcasted successfully";
    msg.BroadcastMessageLength = std::min(static_cast<int>(message.length()), 239);
    strncpy(msg.BroadcastMessage, message.c_str(), 239);

    std::cout << "Sending spread bhavcopy success notification" << std::endl;

    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&msg), sizeof(msg));
    }
}

// Send Market Index Report
void FakeNSEExchange::send_market_index_report(const std::string& index_name, const MKT_INDEX& index_data, uint64_t ts) {
    MKT_IDX_RPT_DATA report;
    memset(&report, 0, sizeof(report));

    report.Header.TransactionCode = TransactionCodes::MKT_IDX_RPT_DATA;
    report.Header.LogTime = static_cast<int32_t>(ts / 1000000);
    report.Header.ErrorCode = 0;
    report.Header.Timestamp = ts;
    report.Header.MessageLength = sizeof(MKT_IDX_RPT_DATA);

    report.MessageType = BhavcopyMessageTypes::DATA_REGULAR;
    strncpy(report.IndexName, index_name.c_str(), 15);
    report.Index = index_data;

    std::cout << "Sending market index report: " << index_name << std::endl;

    if (message_callback_) {
        message_callback_(reinterpret_cast<const uint8_t*>(&report), sizeof(report));
    }
}

// Send Industry Index Report
void FakeNSEExchange::send_industry_index_report(const std::vector<INDUSTRY_INDEX>& industry_data, uint64_t ts) {
    size_t max_records = 10;
    for (size_t i = 0; i < industry_data.size(); i += max_records) {
        IND_IDX_RPT_DATA report;
        memset(&report, 0, sizeof(report));

        report.Header.TransactionCode = TransactionCodes::IND_IDX_RPT_DATA_CODE;
        report.Header.LogTime = static_cast<int32_t>(ts / 1000000);
        report.Header.ErrorCode = 0;
        report.Header.Timestamp = ts;
        report.Header.MessageLength = sizeof(IND_IDX_RPT_DATA);

        report.MessageType = BhavcopyMessageTypes::DATA_REGULAR;
        report.NumberOfIndustryRecords = std::min(max_records, industry_data.size() - i);

        if (report.NumberOfIndustryRecords > 0) {
            report.IndustryIndex = industry_data[i];
        }

        if (message_callback_) {
            message_callback_(reinterpret_cast<const uint8_t*>(&report), sizeof(report));
        }
    }

    std::cout << "Sent industry index report: " << industry_data.size() << " records" << std::endl;
}

// Send Sector Index Report
void FakeNSEExchange::send_sector_index_report(const std::string& industry_name, const std::vector<INDEX_DATA>& sector_data, uint64_t ts) {
    size_t max_records = 10;
    for (size_t i = 0; i < sector_data.size(); i += max_records) {
        SECT_IDX_RPT_DATA report;
        memset(&report, 0, sizeof(report));

        report.Header.TransactionCode = TransactionCodes::SECT_IDX_RPT_DATA_CODE;
        report.Header.LogTime = static_cast<int32_t>(ts / 1000000);
        report.Header.ErrorCode = 0;
        report.Header.Timestamp = ts;
        report.Header.MessageLength = sizeof(SECT_IDX_RPT_DATA);

        report.MessageType = BhavcopyMessageTypes::DATA_REGULAR;
        strncpy(report.IndustryName, industry_name.c_str(), 15);
        report.NumberOfIndustryRecords = std::min(max_records, sector_data.size() - i);

        if (report.NumberOfIndustryRecords > 0) {
            report.IndexData = sector_data[i];
        }

        if (message_callback_) {
            message_callback_(reinterpret_cast<const uint8_t*>(&report), sizeof(report));
        }
    }

    std::cout << "Sent sector index report for " << industry_name << ": " << sector_data.size() << " sectors" << std::endl;
}

// Generate and Broadcast Complete Bhavcopy
void FakeNSEExchange::generate_and_broadcast_bhavcopy(char session_type, uint64_t ts) {
    std::cout << "=== Generating Bhavcopy (Session: " << session_type << ") ===" << std::endl;

    send_bhavcopy_start_notification(ts, false);

    int32_t report_date = static_cast<int32_t>(ts / 1000000);
    send_bhavcopy_header(session_type, report_date, ts, false);

    std::vector<MKT_STATS_DATA> stats;
    for (const auto& pair : market_statistics_) {
        stats.push_back(pair.second);
    }

    if (!stats.empty()) {
        send_bhavcopy_data(session_type, stats, ts, false);
    }

    int32_t packet_count = stats.empty() ? 0 : stats.size();
    send_bhavcopy_trailer(session_type, packet_count, ts, false);

    for (const auto& pair : market_indices_) {
        send_market_index_report(pair.first, pair.second, ts);
    }

    for (const auto& pair : industry_indices_) {
        if (!pair.second.empty()) {
            send_industry_index_report(pair.second, ts);
        }
    }

    for (const auto& pair : sector_indices_) {
        if (!pair.second.empty()) {
            send_sector_index_report(pair.first, pair.second, ts);
        }
    }

    std::cout << "=== Bhavcopy Complete ===" << std::endl;
}

// Generate and Broadcast Spread Bhavcopy
void FakeNSEExchange::generate_and_broadcast_spread_bhavcopy(char session_type, uint64_t ts) {
    std::cout << "=== Generating Spread Bhavcopy (Session: " << session_type << ") ===" << std::endl;

    send_bhavcopy_start_notification(ts, true);

    int32_t report_date = static_cast<int32_t>(ts / 1000000);
    send_bhavcopy_header(session_type, report_date, ts, true);

    std::vector<SPD_STATS_DATA> stats;
    for (const auto& pair : spread_statistics_) {
        stats.push_back(pair.second);
    }

    if (!stats.empty()) {
        send_spread_bhavcopy_data(session_type, stats, ts);
    }

    int32_t packet_count = 0;
    send_bhavcopy_trailer(session_type, packet_count, ts, true);

    send_spread_bhavcopy_success(ts);

    std::cout << "=== Spread Bhavcopy Complete ===" << std::endl;
}
