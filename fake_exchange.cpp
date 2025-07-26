#include "fake_exchange.h"
#include <algorithm>
#include <cstring>
#include <chrono>
#include <iostream>
#include <map>

// FakeNSEExchange Implementation
FakeNSEExchange::FakeNSEExchange() {}

// FakeNSEExchange Destructor
FakeNSEExchange::~FakeNSEExchange() = default;

// Set the message callback for sending responses
void FakeNSEExchange::set_message_callback(std::function<void(const uint8_t*, size_t)> callback) {
    message_callback_ = callback;
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
        
        case TransactionCodes::SPREAD_ORDER_ENTRY_REQUEST : {
            if (header->MessageLength < sizeof(MS_SPD_OE_REQUEST)) {
                error = true;
                return 0;
            }
            const MS_SPD_OE_REQUEST* req = reinterpret_cast<const MS_SPD_OE_REQUEST*>(buf);
            handle_spread_order_entry_request(req, ts);
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
    
    // For now, make every request successful
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
        if (message_callback_) {
            message_callback_(reinterpret_cast<const uint8_t*>(&response), sizeof(response));
        }
    } else {
        // Error case
        std::cout << "Sending error sign-on response to trader: " << req->Header.TraderId 
                  << ", ErrorCode: " << error_code << std::endl;
        send_response_with_error(response, error_code);
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
        if (message_callback_) {
            message_callback_(reinterpret_cast<const uint8_t*>(&response), sizeof(response));
        }
    } else {
        // Error case
        std::cout << "Sending system info error response to trader: " << req->Header.TraderId 
                  << ", ErrorCode: " << error_code << std::endl;
        send_response_with_error(response, error_code);
    }
}