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
    if (contract->OptionType != 0 && order->ContractDesc.OptionType != contract->OptionType) {
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