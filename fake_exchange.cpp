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