#pragma once

#include <cstdint>
#include <cstring>

#pragma pack(push, 1)

struct MESSAGE_HEADER {
    int16_t TransactionCode;
    int32_t LogTime;
    char AlphaChar[2];
    int32_t TraderId;
    int16_t ErrorCode;
    int64_t Timestamp;
    char TimeStamp1[8];
    char TimeStamp2[8];
    int16_t MessageLength;
};


struct INNER_MESSAGE_HEADER {
    int32_t TraderId;
    int32_t LogTime;
    char AlphaChar[2];
    int16_t TransactionCode;
    int16_t ErrorCode;
    int64_t Timestamp;
};


struct BCAST_HEADER {
    char Reserved[2];
    int32_t LogTime;
    char AlphaChar[2];
    int16_t TransactionCode;
    int16_t ErrorCode;
    int32_t BCSeqNo;
    char Reserved[1];
    char Reserved[3];
    char TimeStamp2[8];
    char Filler2[8];
    int16_t MessageLength;
};


struct MS_ERROR_RESPONSE {
    MESSAGE_HEADER Header;
    char Key[14];
    char ErrorMessage[128];
};


struct ST_BROKER_ELIGIBILITY_PER_MKT_SMALL_ENDIAN {
    uint8_t Reserved : 4;
    uint8_t AuctionMarket : 1;
    uint8_t SpotMarket : 1;
    uint8_t OddlotMarket : 1;
    uint8_t NormalMarket : 1;
    uint8_t Reserved2 : 8;
};


struct ST_BROKER_ELIGIBILITY_PER_MKT_BIG_ENDIAN {
    uint8_t NormalMarket : 1;
    uint8_t OddlotMarket : 1;
    uint8_t SpotMarket : 1;
    uint8_t AuctionMarket : 1;
    uint8_t Reserved : 4;
    uint8_t Reserved2 : 8;
};


struct MS_SIGNON_REQUEST_IN {
    MESSAGE_HEADER Header;
    int32_t UserID;
    char Reserved[8];
    char Password[8];
    char Reserved[8];
    char NewPassword[8];
    char TraderName[26];
    int32_t LastPasswordChangeDate;
    char BrokerID[5];
    char Reserved[1];
    int16_t BranchID;
    int32_t VersionNumber;
    int32_t Batch2StartTime;
    char HostSwitchContext[1];
    char Colour[50];
    char Reserved[1];
    int16_t UserType;
    double SequenceNumber;
    char WsClassName[14];
    char BrokerStatus[1];
    char ShowIndex[1];
    ST_BROKER_ELIGIBILITY_PER_MKT_SMALL_ENDIAN BrokerEligibilityPerMarket;
    int16_t MemberType;
    char ClearingStatus[1];
    char BrokerName[25];
    char Reserved[16];
    char Reserved[16];
    char Reserved[16];
};


struct MS_SIGNON_REQUEST_OUT {
    MESSAGE_HEADER Header;
    int32_t UserID;
    char Reserved[8];
    char Password[8];
    char Reserved[8];
    char NewPassword[8];
    char TraderName[26];
    int32_t LastPasswordChangeDate;
    char BrokerID[5];
    char Reserved[1];
    int16_t BranchID;
    int32_t VersionNumber;
    int32_t EndTime;
    char Reserved[1];
    char Colour[50];
    char Reserved[1];
    int16_t UserType;
    double SequenceNumber;
    char Reserved[14];
    char BrokerStatus[1];
    char ShowIndex[1];
    ST_BROKER_ELIGIBILITY_PER_MKT_SMALL_ENDIAN BrokerEligibilityPerMarket;
    int16_t MemberType;
    char ClearingStatus[1];
    char BrokerName[25];
    char Reserved[16];
    char Reserved[16];
    char Reserved[16];
};


struct MS_SYSTEM_INFO_REQ {
    MESSAGE_HEADER Header;
    int32_t LastUpdatePortfolioTime;
};


struct ST_MARKET_STATUS {
    int16_t Normal;
    int16_t Oddlot;
    int16_t Spot;
    int16_t Auction;
};


struct ST_EX_MARKET_STATUS {
    int16_t Normal;
    int16_t Oddlot;
    int16_t Spot;
    int16_t Auction;
};


struct ST_PL_MARKET_STATUS {
    int16_t Normal;
    int16_t Oddlot;
    int16_t Spot;
    int16_t Auction;
};


struct ST_STOCK_ELIGIBLE_INDICATORS_SMALL_ENDIAN {
    uint8_t Reserved : 5;
    uint8_t BooksMerged : 1;
    uint8_t MinimumFill : 1;
    uint8_t AON : 1;
    uint8_t Reserved2 : 8;
};


struct ST_STOCK_ELIGIBLE_INDICATORS_BIG_ENDIAN {
    uint8_t AON : 1;
    uint8_t MinimumFill : 1;
    uint8_t BooksMerged : 1;    
    uint8_t Reserved : 5;
    uint8_t Reserved2 : 8;
};


struct MS_SYSTEM_INFO_DATA {
    MESSAGE_HEADER Header;
    ST_MARKET_STATUS MarketStatus;
    ST_EX_MARKET_STATUS ExMarketStatus;
    ST_PL_MARKET_STATUS PlMarketStatus;
    char UpdatePortfolio;
    int32_t MarketIndex;
    int16_t DefaultSettlementPeriod_Normal;
    int16_t DefaultSettlementPeriod_Spot;
    int16_t DefaultSettlementPeriod_Auction;
    int16_t CompetitorPeriod;
    int16_t SolicitorPeriod;
    int16_t WarningPercent;
    int16_t VolumeFreezePercent;
    int16_t SnapQuoteTime;
    char Reserved[2];
    int32_t BoardLotQuantity;
    int32_t TickSize;
    int16_t MaximumGtcDays;
    ST_STOCK_ELIGIBLE_INDICATORS_SMALL_ENDIAN StockEligibleIndicators;
    int16_t DisclosedQuantityPercentAllowed;
    int32_t RiskFreeInterestRate;
};


struct MS_UPDATE_LOCAL_DATABASE {
    MESSAGE_HEADER Header;
    int32_t LastUpdateSecurityTime;
    int32_t LastUpdateParticipantTime;
    int32_t LastUpdateInstrumentTime;
    int32_t LastUpdateIndexTime;
    char RequestForOpenOrders;
    char Reserved;
    ST_MARKET_STATUS MarketStatus;
    ST_EX_MARKET_STATUS ExMarketStatus;
    ST_PL_MARKET_STATUS PlMarketStatus;
};


struct UPDATE_LDB_HEADER {
    MESSAGE_HEADER Header;
    char Reserved[2];
};


struct UPDATE_LDB_DATA {
    MESSAGE_HEADER Header;
    INNER_MESSAGE_HEADER InnerHeader;
    char Data[432];
};

struct DOWNLOAD_INDEX_DETAILS {
    char IndexName[15];
    int32_t Token;
    int32_t LastUpdateDateTime;
};


struct MS_DOWNLOAD_INDEX {
    MESSAGE_HEADER Header;
    int16_t NoOfRecords;
    DOWNLOAD_INDEX_DETAILS IndexDetails[17];
};


struct BCAST_INDEX_MAP_DETAILS {
    char BcastName[26];
    char ChangedName[10];
    char DeleteFlag;
    int32_t LastUpdateDateTime;
};


struct MS_DOWNLOAD_INDEX_MAP {
    MESSAGE_HEADER Header;
    int16_t NoOfRecords;
    BCAST_INDEX_MAP_DETAILS BCASTIndexMapDetails[10];
};


struct UPDATE_LOCAL_DB_TRAILER {
    MESSAGE_HEADER Header;
    char Reserved[2];
};


struct EXCH_PORTFOLIO_REQ {
    MESSAGE_HEADER Header;
    int32_t LastUpdateDtTime;
};


struct PORTFOLIO_DATA {
    char Portfolio[10];
    int32_t Token;
    int32_t LastUpdateDtTime;
    char DeleteFlag;
};


struct EXCH_PORTFOLIO_RESP {
    MESSAGE_HEADER Header;
    int16_t NoOfRecords;
    char MoreRecords;
    char Filler;
    PORTFOLIO_DATA PortfolioData;
};


struct MS_MESSAGE_DOWNLOAD {
    MESSAGE_HEADER Header;
    double SequenceNumber;
};

struct MS_MESSAGE_DOWNLOAD_HEADER {
    MESSAGE_HEADER Header;
};

struct MS_MESSAGE_DOWNLOAD_DATA {
    MESSAGE_HEADER Header;
    MESSAGE_HEADER InnerHeader;
    char InnerData[400];
};

struct MS_MESSAGE_DOWNLOAD_TRAILER {
    MESSAGE_HEADER Header;
};

struct MS_SIGNOFF {
    MESSAGE_HEADER Header;
};

struct SIGNOFF_OUT {
    MESSAGE_HEADER Header;
    int32_t UserId;
    char Reserved[145];
};


struct CONTRACT_DESC {
    char InstrumentName[6];
    char Symbol[10];
    int32_t ExpiryDate;
    int32_t StrikePrice;
    char OptionType[2];
    int16_t CALevel;
};


struct ST_ORDER_FLAGS_SMALL_ENDIAN {
    uint8_t AON : 1;
    uint8_t IOC : 1;
    uint8_t GTC : 1;
    uint8_t Day : 1;
    uint8_t MIT : 1;
    uint8_t SL : 1;
    uint8_t Market : 1;
    uint8_t ATO : 1;
    uint8_t Reserved : 3;
    uint8_t Frozen : 1;
    uint8_t Modified : 1;
    uint8_t Traded : 1;
    uint8_t MatchedInd : 1;
    uint8_t MF : 1;
};


struct ST_ORDER_FLAGS_BIG_ENDIAN {
    uint8_t ATO : 1;
    uint8_t Market : 1;
    uint8_t SL : 1;
    uint8_t MIT : 1;
    uint8_t Day : 1;
    uint8_t GTC : 1;
    uint8_t IOC : 1;
    uint8_t AON : 1;
    uint8_t MF : 1;
    uint8_t MatchedInd : 1;
    uint8_t Traded : 1;
    uint8_t Modified : 1;
    uint8_t Frozen : 1;
    uint8_t Reserved : 3;
};


struct ADDITIONAL_ORDER_FLAGS_SMALL_ENDIAN {
    uint8_t BOC : 1;
    uint8_t COL : 1;
    uint8_t Reserved : 1;
    uint8_t Reserved2 : 1;
    uint8_t STPC : 1;
    uint8_t Reserved3 : 3;
};


struct ADDITIONAL_ORDER_FLAGS_BIG_ENDIAN {
    uint8_t Reserved : 3;
    uint8_t STPC : 1;
    uint8_t Reserved2 : 1;
    uint8_t Reserved3 : 1;
    uint8_t COL : 1;
    uint8_t BOC : 1;
};


struct MS_OE_REQUEST {
    MESSAGE_HEADER Header;
    char ParticipantType;
    char Reserved;
    int16_t CompetitorPeriod;
    int16_t SolicitorPeriod;
    char ModifiedCancelledBy;
    char Reserved2;
    int16_t ReasonCode;
    char Reserved3[4];
    int32_t TokenNo;
    CONTRACT_DESC ContractDesc;
    char CounterPartyBrokerId[5];
    char Reserved4;
    char Reserved5[2];
    char CloseoutFlag;
    char Reserved6;
    int16_t OrderType;
    double OrderNumber;
    char AccountNumber[10];
    int16_t BookType;
    int16_t BuySellIndicator;
    int32_t DisclosedVolume;
    int32_t DisclosedVolumeRemaining;
    int32_t TotalVolumeRemaining;
    int32_t Volume;
    int32_t VolumeFilledToday;
    int32_t Price;
    int32_t TriggerPrice;
    int32_t GoodTillDate;
    int32_t EntryDateTime;
    int32_t MinimumFillAONVolume;
    int32_t LastModified;
    ST_ORDER_FLAGS_SMALL_ENDIAN OrderFlags;
    int16_t BranchId;
    int32_t TraderId;
    char BrokerId[5];
    char cOrdFiller[24];
    char OpenClose;
    char Settlor[12];
    int16_t ProClientIndicator;
    int16_t SettlementPeriod;
    ADDITIONAL_ORDER_FLAGS_SMALL_ENDIAN AdditionalOrderFlags;
    char Reserved7;
    char Filler17;
    char Filler18;
    double NnField;
    int64_t MktReplay;
    char PAN[10];
    int32_t AlgoID;
    int16_t Reserved8;
    int64_t LastActivityReference;
    char Reserved9[52];
};


struct PRICE_MOD {
    MESSAGE_HEADER Header;
    int32_t TokenNo;
    int32_t TraderID;
    double OrderNumber;
    int16_t BuySell;
    int32_t Price;
    int32_t Volume;
    int32_t LastModified;
    char Reference[4];
    int64_t LastActivityReference;
    char Reserved[24];
};


struct MS_TRADE_INQ_DATA {
    MESSAGE_HEADER Header;
    int32_t TokenNo;
    CONTRACT_DESC ContractDesc;
    int32_t FillNumber;
    int32_t FillQuantity;
    int32_t FillPrice;
    char MktType;
    char BuyOpenClose;
    int32_t Reserved;
    char BuyBrokerId[5];
    char SellBrokerId[5];
    int32_t TraderId;
    char RequestedBy;
    char SellOpenClose;
    char BuyAccountNumber[10];
    char SellAccountNumber[10];
    char Reserved[24];
    char ReservedFiller[2];
    char Reserved[2];
    char BuyPAN[10];
    char SellPAN[10];
    char Reserved[60];
};


struct MS_SPD_LEG_INFO {
    int32_t Token2;
    CONTRACT_DESC ContractDesc;
    char OpBrokerId2[5];
    char Fillerx2[1];
    int16_t OrderType2;
    int16_t BuySell2;
    int32_t DisclosedVol2;
    int32_t DisclosedVolRemaining2;
    int32_t TotalVolRemaining2;
    int32_t Volume2;
    int32_t VolumeFilledToday2;
    int32_t Price2;
    int32_t TriggerPrice2;
    int32_t MinFillAon2;
    ST_ORDER_FLAGS_SMALL_ENDIAN OrderFlags;
    char OpenClose2[1];
    ADDITIONAL_ORDER_FLAGS_SMALL_ENDIAN AdditionalOrderFlags;
    char Reserved[1];
    char FillerY[1];
};


struct MS_SPD_OE_REQUEST {
    MESSAGE_HEADER Header;
    char ParticipantType1;
    char Filler1;
    int16_t CompetitorPeriod1;
    int16_t SolicitorPeriod1;
    char ModCxBy1;
    char Filler9;
    int16_t ReasonCode1;
    char StartAlpha1[2];
    char EndAlpha1[2];
    int32_t Token1;
    CONTRACT_DESC ContractDesc;
    char OpBrokerId1[5];
    char Fillerx1;
    char FillerOptions1[3];
    char Filler1_2;
    int16_t OrderType1;
    double OrderNumber1;
    char AccountNumber1[10];
    int16_t BookType1;
    int16_t BuySell1;
    int32_t DisclosedVol1;
    int32_t DisclosedVolRemaining1;
    int32_t TotalVolRemaining1;
    int32_t Volume1;
    int32_t VolumeFilledToday1;
    int32_t Price1;
    int32_t TriggerPrice1;
    int32_t GoodTillDate1;
    int32_t EntryDateTime1;
    int32_t MinFillAon1;
    int32_t LastModified1;
    ST_ORDER_FLAGS_SMALL_ENDIAN OrderFlags;
    int16_t BranchId1;
    int32_t TraderId1;
    char BrokerId1[5];
    char cOrdFiller[24];
    char OpenClose1;
    char Settlor1[12];
    int16_t ProClient1;
    int16_t SettlementPeriod1;
    ADDITIONAL_ORDER_FLAGS_SMALL_ENDIAN AdditionalOrderFlags;
    char Reserved;
    char Filler17;
    char Filler18;
    double NnField;
    int64_t MktReplay;
    char PAN[10];
    int32_t AlgoID;
    int16_t Reserved_2;
    int64_t LastActivityReference;
    char Reserved_3[52];
    int32_t PriceDiff;
    MS_SPD_LEG_INFO MS_SPD_LEG_INFO_leg2;
    MS_SPD_LEG_INFO MS_SPD_LEG_INFO_leg3;
};


struct SEC_INFO {
    char InstrumentName[6];
    char Symbol[10];
    char Series[2];
    int32_t ExpiryDate;
    int32_t StrikePrice;
    char OptionType[2];
    int16_t CALevel;
};


struct ST_SPD_ELIGIBILITY {
    uint8_t Reserved : 7;
    uint8_t Eligibility : 1;
};


struct MS_SPD_UPDATE_INFO {
    MESSAGE_HEADER Header;
    int32_t Token1;
    int32_t Token2;
    SEC_INFO SecInfo1;
    SEC_INFO SecInfo2;
    int32_t ReferencePrice;
    int32_t DayLowPriceDiffRange;
    int32_t DayHighPriceDiffRange;
    int32_t OpLowPriceDiffRange;
    int32_t OpHighPriceDiffRange;
    ST_SPD_ELIGIBILITY SPDEligibility;
    char Reserved;
    char DeleteFlag;
    char Reserved;
};


struct MS_TRADE_CONFIRM {
    MESSAGE_HEADER Header;
    double ResponseOrderNumber;
    char BrokerId[5];
    char Reserved;
    int32_t TraderNumber;
    char AccountNumber[10];
    int16_t BuySellIndicator;
    int32_t OriginalVolume;
    int32_t DisclosedVolume;
    int32_t RemainingVolume;
    int32_t DisclosedVolumeRemaining;
    int32_t Price;
    ST_ORDER_FLAGS_SMALL_ENDIAN OrderFlags;
    int32_t GoodTillDate;
    int32_t FillNumber;
    int32_t FillQuantity;
    int32_t FillPrice;
    int32_t VolumeFilledToday;
    char ActivityType[2];
    int32_t ActivityTime;
    double CounterTraderOrderNumber;
    char CounterBrokerId[5];
    int32_t Token;
    CONTRACT_DESC ContractDesc;
    char OpenClose;
    char OldOpenClose;
    char BookType;
    int32_t Reserved1;
    char OldAccountNumber[10];
    char Participant[12];
    char OldParticipant[12];
    ADDITIONAL_ORDER_FLAGS_SMALL_ENDIAN AdditionalOrderFlags;
    char Reserved2;
    char Reserved3;
    char ReservedFiller2;
    char PAN[10];
    char OldPAN[10];
    int32_t AlgoID;
    int16_t Reserved4;
    int64_t LastActivityReference;
    char Reserved5[52];
};


struct INSTRUMENT_USER {
    double BranchBuyValueLimit;
    double BranchSellValueLimit;
    double BranchUsedBuyValueLimit;
    double BranchUsedSellValueLimit;
    double UserOrderBuyValueLimit;
    double UserOrderSellValueLimit;
    double UserOrderUsedBuyValueLimit;
    double UserOrderUsedSellValueLimit;
};


struct MS_ORDER_VAL_LIMIT_DATA {
    MESSAGE_HEADER Header;
    char BrokerId[5];
    int16_t BranchId;
    char UserName[25];
    int32_t UserId;
    int16_t UserType;
    INSTRUMENT_USER InstrumentUser;
};


struct DEALER_ORD_LMT {
    MESSAGE_HEADER Header;
    char BrokerId[5];
    int32_t UserId;
    double OrdQtyBuff;
    double OrdValBuff;
};


struct SPD_ORD_LMT {
    MESSAGE_HEADER Header;
    char BrokerId[5];
    int32_t UserId;
    double SpdOrdQtyBuff;
    double SpdOrdValBuff;
};


struct MS_TRADER_INT_MSG {
    MESSAGE_HEADER Header;
    int32_t TraderId;
    char Reserved[3];
    char Reserved[1];
    int16_t BroadCastMessageLength;
    char BroadCastMessage[239];
};


struct ST_BCAST_DESTINATION_SMALL_ENDIAN {
    uint8_t Reserved : 4;
    uint8_t JournalingRequired : 1;
    uint8_t Tandem : 1;
    uint8_t ControlWorkstation : 1;
    uint8_t TraderWorkstation : 1;
    char Reserved2;
};


struct ST_BCAST_DESTINATION_BIG_ENDIAN {
    uint8_t TraderWorkstation : 1;
    uint8_t ControlWorkstation : 1;
    uint8_t Tandem : 1;
    uint8_t JournalingRequired : 1;
    uint8_t Reserved : 4;
    char Reserved2;
};


struct MS_BCAST_MESSAGE {
    BCAST_HEADER Header;
    int16_t BranchNumber;
    char BrokerNumber[5];
    char ActionCode[3];
    ST_BCAST_DESTINATION_SMALL_ENDIAN BCASTDestination;
    char Reserved[26];
    int16_t BroadcastMessageLength;
    char BroadcastMessage[239];
};


struct CTRL_MSG_TO_TRADER {
    MESSAGE_HEADER Header;
    int32_t TraderId;
    char ActionCode[3];
    char Reserved[1];
    int16_t BroadCastMessageLength;
    char BroadCastMessage[239];
};


struct MS_RP_HDR_RPRT_MARKET_STATS_OUT_RPT {
    MESSAGE_HEADER Header;
    char MessageType;
    int32_t ReportDate;
    int16_t UserType;
    char BrokerId[5];
    char FirmName[25];
    int32_t TraderNumber;
    char TraderName[26];
};


struct MKT_STATS_DATA {
    CONTRACT_DESC ContractDesc;
    int16_t MarketType;
    int32_t OpenPrice;
    int32_t HighPrice;
    int32_t LowPrice;
    int32_t ClosingPrice;
    uint32_t TotalQuantityTraded;
    double TotalValueTraded;
    int32_t PreviousClosePrice;
    uint32_t OpenInterest;
    int32_t ChgOpenInterest;
    char Indicator[4];
};


struct MS_RP_MARKET_STATS {
    MESSAGE_HEADER Header;
    char MessageType;
    char Reserved;
    int16_t NumberOfRecords;
    MKT_STATS_DATA MarketStatsData;
};


struct MKT_INDEX {
    int16_t Opening;
    int16_t High;
    int16_t Low;
    int16_t Closing;
    int16_t Start;
};


struct MKT_IDX_RPT_DATA {
    MESSAGE_HEADER Header;
    char MessageType;
    char IndexName[15];
    MKT_INDEX Index;
};


struct INDUSTRY_INDEX {
    char IndustryName[15];
    int32_t Opening;
    int32_t High;
    int32_t Low;
    int32_t Closing;
    int32_t Start;
};


struct IND_IDX_RPT_DATA {
    MESSAGE_HEADER Header;
    char MessageType;
    char Reserved;
    int16_t NumberOfIndustryRecords;
    INDUSTRY_INDEX IndustryIndex;
};


struct INDEX_DATA {
    char SectorName[15];
    int32_t IndexValue;
};


struct SECT_IDX_RPT_DATA {
    MESSAGE_HEADER Header;
    char MessageType;
    char IndustryName[15];
    int16_t NumberOfIndustryRecords;
    INDEX_DATA IndexData;
};


struct MS_RP_TRAILER_RPRT_MARKET_STATS_OUT_RPT {
    MESSAGE_HEADER Header;
    char MessageType;
    int32_t NumberOfPackets;
    char Reserved;
};


struct MS_RP_HDR_SPD_BC_JRNL_VCT_MSG {
    MESSAGE_HEADER Header;
    char MessageType;
    char OrgScope;
    int32_t ReportDate;
    int16_t UserType;
    char BrokerNumber[5];
    char BrokerName[25];
    int32_t TraderNumber;
    char TraderName[26];
};


struct SPD_STATS_DATA {
    int16_t MARKETTYPE;
    char INSTRUMENTNAME1[6];
    char SYMBOL1[10];
    int32_t EXPIRYDATE1;
    int32_t STRIKEPRICE1;
    char OPTIONTYPE1[2];
    int16_t CALEVEL1;
    char INSTRUMENTNAME2[6];
    char SYMBOL2[10];
    int32_t EXPIRYDATE2;
    int32_t STRIKEPRICE2;
    char OPTIONTYPE2[2];
    int16_t CALEVEL2;
    int32_t OPENPD;
    int32_t HIPD;
    int32_t LOWPD;
    int32_t LASTTRADEDPD;
    int32_t NOOFCONTRACTSTRADED;
};


struct RP_SPD_MKT_STATS {
    MESSAGE_HEADER Header;
    char MessageType;
    char Reserved;
    int16_t NoOfRecords;
    SPD_STATS_DATA SPDStatsData;
};


struct MS_RP_TRAILER_SPD_BC_JRNL_VCT_MSG {
    MESSAGE_HEADER Header;
    char MessageType;
    int32_t NumberOfPackets;
    char Reserved;
};


struct ST_SEC_ELIGIBILITY_PER_MARKET_3_BYTES_SMALL_ENDIAN {
    uint8_t Reserved : 7;
    uint8_t Eligibility : 1;
    int16_t Status;
};


struct ST_SEC_ELIGIBILITY_PER_MARKET_3_BYTES_BIG_ENDIAN {
    uint8_t Eligibility : 1;
    uint8_t Reserved : 7;
    int16_t Status;
};


struct ST_ELIGIBILITY_INDICATORS_SMALL_ENDIAN {
    uint8_t Reserved : 5;
    uint8_t MinimumFill : 1;
    uint8_t AON : 1;
    uint8_t ParticipateInMarketIndex : 1;
    char Reserved2;
};


struct ST_ELIGIBILITY_INDICATORS_BIG_ENDIAN {
    uint8_t ParticipateInMarketIndex : 1;
    uint8_t AON : 1;
    uint8_t MinimumFill : 1;
    uint8_t Reserved : 5;
    char Reserved2;
};


struct ST_PURPOSE_SMALL_ENDIAN {
    uint8_t ExerciseStyle : 1;
    uint8_t Reserved1 : 1;
    uint8_t EGM : 1;
    uint8_t AGM : 1;
    uint8_t Interest : 1;
    uint8_t Bonus : 1;
    uint8_t Rights : 1;
    uint8_t Dividend : 1;
    uint8_t Reserved2 : 3;
    uint8_t IsCorporateAdjusted : 1;
    uint8_t IsThisAsset : 1;
    uint8_t PIAllowed : 1;
    uint8_t ExRejectionAllowed : 1;
    uint8_t ExAllowed : 1;
};


struct ST_PURPOSE_BIG_ENDIAN {
    uint8_t Dividend : 1;
    uint8_t Rights : 1;
    uint8_t Bonus : 1;
    uint8_t Interest : 1;
    uint8_t AGM : 1;
    uint8_t EGM : 1;
    uint8_t Reserved1 : 1;
    uint8_t ExerciseStyle : 1;
    uint8_t ExAllowed : 1;
    uint8_t ExRejectionAllowed : 1;
    uint8_t PIAllowed : 1;
    uint8_t IsThisAsset : 1;
    uint8_t IsCorporateAdjusted : 1;
    uint8_t Reserved2 : 3;
};


struct MS_SECURITY_UPDATE_INFO {
    BCAST_HEADER Header;
    int32_t Token;
    SEC_INFO SecInfo;
    int16_t PermittedToTrade;
    double IssuedCapital;
    int32_t WarningQuantity;
    int32_t FreezeQuantity;
    char CreditRating[12];
    ST_SEC_ELIGIBILITY_PER_MARKET_3_BYTES_SMALL_ENDIAN EligibilityPerMarket;
    int16_t IssueRate;
    int32_t IssueStartDate;
    int32_t InterestPaymentDate;
    int32_t IssueMaturityDate;
    int32_t MarginPercentage;
    int32_t MinimumLotQuantity;
    int32_t BoardLotQuantity;
    int32_t TickSize;
    char Name[25];
    char Reserved[1];
    int32_t ListingDate;
    int32_t ExpulsionDate;
    int32_t ReAdmissionDate;
    int32_t RecordDate;
    int32_t LowPriceRange;
    int32_t HighPriceRange;
    int32_t ExpiryDate;
    int32_t NoDeliveryStartDate;
    int32_t NoDeliveryEndDate;
    ST_ELIGIBILITY_INDICATORS_SMALL_ENDIAN EligibilityIndicators;
    int32_t BookClosureStartDate;
    int32_t BookClosureEndDate;
    int32_t ExerciseStartDate;
    int32_t ExerciseEndDate;
    int32_t OldToken;
    char AssetInstrument[6];
    char AssetName[10];
    int32_t AssetToken;
    int32_t IntrinsicValue;
    int32_t ExtrinsicValue;
    ST_PURPOSE_SMALL_ENDIAN Purpose;
    int32_t LocalUpdateDateTime;
    char DeleteFlag[1];
    char Remark[25];
    int32_t BasePrice;
};


struct MS_INSTRUMENT_UPDATE_INFO {
    BCAST_HEADER Header;
    int16_t InstrumentId;
    char InstrumentName[6];
    char InstrumentDescription[25];
    int32_t InstrumentUpdateTime;
    char DeleteFlag;
};


struct PARTICIPANT_UPDATE_INFO {
    BCAST_HEADER Header;
    char ParticipantId[12];
    char ParticipantName[25];
    char ParticipantStatus;
    int32_t ParticipantUpdateDateTime;
    char DeleteFlag;
};


struct ST_SEC_STATUS_PER_MARKET {
    int16_t Status;
};


struct TOKEN_AND_ELIGIBILITY {
    int32_t Token;
    ST_SEC_STATUS_PER_MARKET StatusPerMarket[4];
};


struct MS_SECURITY_STATUS_UPDATE_INFO {
    BCAST_HEADER Header;
    int16_t NumberOfRecords;
    TOKEN_AND_ELIGIBILITY TokenAndEligibility[35];
};


struct MS_BROADCAST_LIMIT_EXCEEDED {
    BCAST_HEADER Header;
    char BrokerCode[5];
    char CounterBrokerCode[5];
    int16_t WarningType;
    int32_t Token;
    char InstrumentName[6];
    char Symbol[10];
    int32_t ExpiryDate;
    int32_t StrikePrice;
    char OptionType[2];
    int16_t CALevel;
    int32_t TradeNumber;
    int32_t TradePrice;
    int32_t TradeVolume;
    char Final[1];
    char Filler[1];
};


struct MS_BCAST_VCT_MSGS {
    BCAST_HEADER Header;
    int32_t Token;
    SEC_INFO SecInfo;
    int16_t MarketType;
    ST_BCAST_DESTINATION_SMALL_ENDIAN BCASTDestination;
    int16_t BroadcastMessageLength;
    char BroadcastMessage[239];
};


struct ST_TICKER_INDEX_INFO {
    int32_t Token;
    int16_t MarketType;
    int32_t FillPrice;
    int32_t FillVolume;
    uint32_t OpenInterest;
    uint32_t DayHiOI;
    uint32_t DayLoOI;
};


struct MS_TICKER_TRADE_DATA {
    BCAST_HEADER Header;
    int16_t Number_of_Records;
    ST_TICKER_INDEX_INFO TickerIndexInfo;
};


struct ST_ENHNCD_TICKER_INDEX_INFO {
    int32_t Token;
    int16_t MarketType;
    int32_t FillPrice;
    int32_t FillVolume;
    int64_t OpenInterest;
    int64_t DayHiOi;
    int64_t DayLoOi;
};


struct MS_ENHNCD_TICKER_TRADE_DATA {
    BCAST_HEADER Header;
    int16_t Number_of_Records;
    ST_ENHNCD_TICKER_INDEX_INFO EnhancdTickerIndexInfo;
};


struct ST_INTERACTIVE_MBO_DATA {
    int32_t Token;
    int16_t BookType;
    int16_t TradingStatus;
    uint32_t VolumeTradedToday;
    int32_t LastTradedPrice;
    char NetChangeIndicator;
    int32_t NetPriceChangeFromClosingPrice;
    int32_t LastTradeQuantity;
    int32_t LastTradeTime;
    int32_t AverageTradePrice;
    int16_t AuctionNumber;
    int16_t AuctionStatus;
    int16_t InitiatorType;
    int32_t InitiatorPrice;
    int32_t InitiatorQuantity;
    int32_t AuctionPrice;
    int32_t AuctionQuantity;
    char RecordBuffer[180];
};


struct ST_INDICATOR_SMALL_ENDIAN {
    uint8_t Reserved : 4;
    uint8_t Sell : 1;
    uint8_t Buy : 1;
    uint8_t LastTradeLess : 1;
    uint8_t LastTradeMore : 1;
    char Reserved2;
};


struct ST_INDICATOR_BIG_ENDIAN {
    uint8_t LastTradeMore : 1;
    uint8_t LastTradeLess : 1;
    uint8_t Buy : 1;
    uint8_t Sell : 1;
    uint8_t Reserved : 4;
    char Reserved2;
};


struct MS_BCAST_MBO_MBP {
    BCAST_HEADER Header;
    ST_INTERACTIVE_MBO_DATA Interactive_MBO_Data;
    char RecordBuffer[100];
    double TotalBuyQuantity;
    double TotalSellQuantity;
    ST_INDICATOR_SMALL_ENDIAN Indicator;
    int32_t ClosingPrice;
    int32_t OpenPrice;
    int32_t HighPrice;
    int32_t LowPrice;
};


struct ST_MBO_MBP_TERMS_SMALL_ENDIAN {
    uint8_t Reserved : 6;
    uint8_t AON : 1;
    uint8_t MF : 1;
    char Reserved2;
};


struct ST_MBO_MBP_TERMS_BIG_ENDIAN {
    uint8_t MF : 1;
    uint8_t AON : 1;
    uint8_t Reserved : 6;
    char Reserved2;
};


struct ST_MBO_INFO {
    int32_t TraderId;
    int32_t Qty;
    int32_t Price;
    ST_MBO_MBP_TERMS_SMALL_ENDIAN Terms;
    int32_t MinFillQty;
};


struct ST_MBP_INFO {
    int32_t Qty;
    int32_t Price;
    int16_t NoOfOrders;
};


struct INTERACTIVE_ONLY_MBP_DATA {
    int32_t Token;
    int16_t BookType;
    int16_t TradingStatus;
    uint32_t VolumeTradedToday;
    int32_t LastTradedPrice;
    char NetChangeIndicator;
    int32_t NetPriceChangeFromClosingPrice;
    int32_t LastTradeQuantity;
    int32_t LastTradeTime;
    int32_t AverageTradePrice;
    int16_t AuctionNumber;
    int16_t AuctionStatus;
    int16_t InitiatorType;
    int32_t InitiatorPrice;
    int32_t InitiatorQuantity;
    int32_t AuctionPrice;
    int32_t AuctionQuantity;
    char RecordBuffer[55];
    int16_t BbTotalBuyFlag;
    int16_t BbTotalSellFlag;
    double TotalBuyQuantity;
    double TotalSellQuantity;
    ST_INDICATOR_SMALL_ENDIAN Indicator;
    int32_t ClosingPrice;
    int32_t OpenPrice;
    int32_t HighPrice;
    int32_t LowPrice;
};


struct MS_BCAST_ONLY_MBP {
    BCAST_HEADER Header;
    int16_t NoOfRecords;
    INTERACTIVE_ONLY_MBP_DATA InteractiveOnlyMBPData;
};


struct MBP_INFORMATION {
    int32_t Quantity;
    int32_t Price;
    int16_t NumberOfOrders;
    int16_t BbBuySellFlag;
};


struct ST_MKT_WISE_INFO {
    ST_INDICATOR_SMALL_ENDIAN Indicator;
    int32_t BuyVolume;
    int32_t BuyPrice;
    int32_t SellVolume;
    int32_t SellPrice;
    int32_t LastTradePrice;
    int32_t LastTradeTime;
};


struct ST_MARKET_WATCH_BCAST {
    int32_t Token;
    ST_MKT_WISE_INFO MarketWiseInfo;
    uint32_t OpenInterest;
};


struct MS_BCAST_INQ_RESP_2 {
    BCAST_HEADER Header;
    int16_t NoOfRecords;
    ST_MARKET_WATCH_BCAST MarketWatchBCAST;
};


struct ST_ENHNCD_MARKET_WATCH_BCAST {
    int32_t Token;
    ST_MKT_WISE_INFO MarketWiseInfo;
    int64_t OpenInterest;
};


struct MS_ENHNCD_BCAST_INQ_RESP_2 {
    BCAST_HEADER Header;
    int16_t NoOfRecords;
    ST_ENHNCD_MARKET_WATCH_BCAST EnhancdMarketWatchBCAST;
};


struct MS_SEC_OPEN_MSGS {
    MESSAGE_HEADER Header;
    char Symbol[10];
    char Series[2];
    int32_t Token;
    int32_t OpeningPrice;
    uint32_t Reserved;
};


struct MS_INDICES {
    char IndexName[21];
    int32_t IndexValue;
    int32_t HighIndexValue;
    int32_t LowIndexValue;
    int32_t OpeningIndex;
    int32_t ClosingIndex;
    int32_t PercentChange;
    int32_t YearlyHigh;
    int32_t YearlyLow;
    int32_t NoOfUpmoves;
    int32_t NoOfDownmoves;
    double MarketCapitalisation;
    char NetChangeIndicator;
    char Reserved;
};


struct MS_BCAST_INDICES {
    BCAST_HEADER Header;
    int16_t NumberOfRecords;
    MS_INDICES Indices;
};


struct INDUSTRY_INDICES {
    char IndustryName[15];
    int32_t IndexValue;
};


struct MS_BCAST_INDUSTRY_INDICES {
    BCAST_HEADER Header;
    int16_t NoOfRecs;
    INDUSTRY_INDICES Indices;
};


struct GI_INDEX_DETAILS {
    int32_t Token;
    char Name[50];
    int32_t Open;
    int32_t High;
    int32_t Low;
    int32_t Last;
    int32_t Close;
    int32_t PrevClose;
    int32_t LifeHigh;
    int32_t LifeLow;
    int32_t filler1;
    int32_t filler2;
    int32_t filler3;
};


struct MS_GLOBAL_INDICES {
    MESSAGE_HEADER Header;
    GI_INDEX_DETAILS IndexDetails;
};


struct CONTRACT_DETAILS {
    int32_t Token;
    char NseSymbol[16];
    char InstrumentName[6];
    int16_t ExpDay;
    int16_t ExpMonth;
    int16_t ExpYear;
    char OptionType[2];
    int32_t StrikePrice;
    int32_t BidPrice;
    int32_t AskPrice;
    double BidSize;
    double AskSize;
    int32_t Open;
    int32_t High;
    int32_t Low;
    int32_t Last;
    int32_t Close;
    int32_t PrevClose;
    int32_t LimitHigh;
    int32_t LimitLow;
    double TotalTrades;
    double OpenInterest;
    int32_t filler1;
    int32_t filler2;
    int32_t filler3;
};


struct MS_GLOBAL_CONTRACTS {
    MESSAGE_HEADER Header;
    CONTRACT_DETAILS ContractDetails;
};


struct MbpBuys {
    int16_t NoOrders;
    int32_t Volume;
    int32_t Price;
};


struct MbpSells {
    int16_t NoOrders;
    int32_t Volume;
    int32_t Price;
};


struct TotalOrderVolume {
    double Buy;
    double Sell;
};


struct MS_SPD_MKT_INFO {
    BCAST_HEADER Header;
    int32_t Token1;
    int32_t Token2;
    int16_t MbpBuy;
    int16_t MbpSell;
    int32_t LastActiveTime;
    uint32_t TradedVolume;
    double TotalTradedValue;
    MbpBuys Mbpbuys;
    MbpSells Mbpsells;
    TotalOrderVolume Totalordervolume;
    int32_t OpenPriceDifference;
    int32_t DayHighPriceDifference;
    int32_t DayLowPriceDifference;
    int32_t LastTradedPriceDifference;
    int32_t LastUpdateTime;
};


struct OPEN_INTEREST {
    int32_t TokenNo;
    uint32_t CurrentOi;
};


struct CM_ASSET_OI {
    char Reserved1[2];
    char Reserved2[2];
    int32_t LogTime;
    char MarketType[2];
    int16_t TransactionCode;
    int16_t NoOfRecords;
    char Reserved3[8];
    int64_t TimeStamp;
    char Reserved4[8];
    int16_t MessageLength;
    OPEN_INTEREST OpenInterest;
};


struct ENHNCD_OPEN_INTEREST {
    int32_t TokenNo;
    int64_t CurrentOi;
};


struct ENHNCD_CM_ASSET_OI {
    char Reserved[2];
    char Reserved[2];
    int32_t LogTime;
    char MarketType[2];
    int16_t TransactionCode;
    int16_t NoOfRecords;
    char Reserved[8];
    int64_t TimeStamp;
    char Reserved[8];
    int16_t MessageLength;
    ENHNCD_OPEN_INTEREST EnhncdOpenInterest;
};


struct LIMIT_PRICE_PROTECTION_RANGE_DETAILS {
    int32_t TokenNumber;
    int32_t HighExecBand;
    int32_t LowExecBand;
};


struct LIMIT_PRICE_PROTECTION_RANGE_DATA {
    int32_t MsgCount;
    LIMIT_PRICE_PROTECTION_RANGE_DETAILS LimitPriceProtectionRangeDetails[25];
};


struct MS_BCAST_LIMIT_PRICE_PROTECTION_RANGE {
    BCAST_HEADER Header;
    LIMIT_PRICE_PROTECTION_RANGE_DATA LimitPriceProtectionRangeData;
};


struct MS_GR_REQUEST {
    MESSAGE_HEADER Header;
    int16_t BoxID;
    char BrokerID[5];
    char Filler[1];
};


struct MS_GR_RESPONSE_OLD {
    MESSAGE_HEADER Header;
    int16_t BoxID;
    char BrokerID[5];
    char Filler[1];
    char IPAddress[16];
    int32_t Port;
    char SessionKey[8];
    char CryptographicKey[32];
    char CryptographicIV[16];
};


struct MS_GR_RESPONSE_NEW {
    MESSAGE_HEADER Header;
    int16_t BoxID;
    char BrokerID[5];
    char Filler[1];
    char IPAddress[16];
    int32_t Port;
    char SessionKey[8];
    char CryptographicKey[32];
    char CryptographicIV[16];
    char CryptographicAdditionalKey[12];
};


struct MS_SECURE_BOX_REGISTRATION_REQUEST_IN {
    MESSAGE_HEADER Header;
    int16_t BoxId;
};


struct MS_SECURE_BOX_REGISTRATION_RESPONSE_OUT {
    MESSAGE_HEADER Header;
};


struct MS_BOX_SIGN_ON_REQUEST_IN {
    MESSAGE_HEADER Header;
    int16_t BoxId;
    char BrokerId[5];
    char Reserved[5];
    char SessionKey[8];
};


struct MS_BOX_SIGN_ON_REQUEST_OUT {
    MESSAGE_HEADER Header;
    int16_t BoxId;
    char Reserved[10];
};


struct HEARTBEAT {
    MESSAGE_HEADER Header;
};


struct MS_BOX_SIGN_OFF {
    MESSAGE_HEADER Header;
    int16_t BoxId;
};


struct MS_BCAST_CONT_MESSAGE {
    MESSAGE_HEADER Header;
    int16_t StreamNumber;
    int16_t Status;
    char Reserved[200];
};


struct BRANCH_LIMITS {
    double BranchBuyValueLimit;
    double BranchSellValueLimit;
    char Reserved[16];
};


struct BRANCH_ORD_VAL_LIMIT_UPDATE_REQ {
    MESSAGE_HEADER Header;
    char BrokerId[5];
    char Reserved[25];
    int16_t BranchId;
    BRANCH_LIMITS BranchLimits;
};


struct USER_LIMITS {
    char Reserved[32];
    double UserOrderBuyValueLimit;
    double UserOrderSellValueLimit;
    char Reserved[16];
};


struct USER_ORD_VAL_LIMIT_UPDATE_REQ {
    MESSAGE_HEADER Header;
    char BrokerId[5];
    char Reserved;
    int16_t BranchId;
    char Reserved[26];
    int32_t UserId;
    char Reserved[2];
    USER_LIMITS UserLimits;
};


struct NORMAL_ORD_LIMIT_UPDATE_REQ {
    MESSAGE_HEADER Header;
    char BrokerId[5];
    char Reserved[1];
    int32_t UserId;
    double OrderQtyLimit;
    double OrderValLimit;
};


struct RESET_USER_PASSWORD_IN_FO {
    MESSAGE_HEADER Header;
    int32_t Userid;
    char Reserved[14];
};


struct COL_USER_STATUS_CHANGE_REQ {
    MESSAGE_HEADER Header;
    int32_t Userid;
    char ColUserBit;
    char Reserved[7];
};


struct COL_USER_STATUS_CHANGE_RESP {
    MESSAGE_HEADER Header;
    int32_t Userid;
    char ColUserBit;
    char Reserved;
};


struct USER_TRD_MOD_CXL_STATUS_CHG_REQ {
    MESSAGE_HEADER Header;
    int32_t Userid;
    char TrdModCxlBit;
    char Reserved[7];
};


struct USER_TRD_MOD_CXL_STATUS_CHG_RESP {
    MESSAGE_HEADER Header;
    int32_t Userid;
    char TrdModCxlBit;
    char Reserved;
};


struct USER_ADDR_UNLOCK_REQ_FO {
    MESSAGE_HEADER Header;
    int32_t Userid;
    char Reserved[70];
};


struct USER_ADDR_UNLOCK_CONFIRM_FO {
    MESSAGE_HEADER Header;
    int32_t Userid;
    char Reserved[36];
};


struct USER_ADDR_UNLOCK_APPROVE_FO {
    MESSAGE_HEADER Header;
    int32_t UserId;
    char Reserved[36];
};


struct GIVEUP {
    double OrderNumber;
    int32_t FillNumber;
    char InstrumentName[6];
    char Symbol[10];
    int32_t ExpiryDate;
    int32_t StrikePrice;
    char OptionType[2];
    int16_t CALevel;
    int32_t FillVolume;
    int32_t FillPrice;
    char BrokerId[5];
    char Filler[1];
    int16_t BuySell;
    int16_t BookType;
    int32_t LastModifiedDateTime;
    char InitiatedByControl[1];
    char OpenClose[1];
    char ReservedFiller[1];
    char Participant[12];
    char GiveupFlag[1];
    char Deleted[1];
};


struct GIVEUP_RESPONSE {
    MESSAGE_HEADER Header;
    int16_t ReasonCode;
    GIVEUP Giveup;
};


struct MS_ACK_RESPONSE {
    int16_t TransactionCode;
    int32_t TraderId;
    int64_t TimeStamp;
    int32_t Reference;
    int16_t ErrorCode;
    int16_t MessageLength;
};


struct CONTRACT_FILE_HEADER {
    char NEATFO[6];
    char Reserved;
    char VersionNumber[5];
    char Reserved;
};


struct ST_SEC_ELIGIBILITY_PER_MARKET_6_BYTES {
    int16_t SecurityStatus;
    char Reserved;
    char Eligibility;
    char Reserved[2];
};


struct STOCK_STRUCTURE {
    int32_t Token;
    char Reserved1;
    int32_t AssetToken;
    char Reserved2;
    char InstrumentName[6];
    char Reserved3;
    char Symbol[10];
    char Reserved4;
    char Series[2];
    char Reserved5[2];
    int32_t ExpiryDate;
    char Reserved6;
    int32_t StrikePrice;
    char Reserved7;
    char OptionType[2];
    char Reserved8;
    char Category;
    char Reserved9;
    int16_t CALevel;
    char Reserved10;
    char ReservedIdentifier;
    char Reserved11;
    int16_t PermittedToTrade;
    char Reserved12;
    int16_t IssueRate;
    char Reserved13;
    ST_SEC_ELIGIBILITY_PER_MARKET_6_BYTES EligibilityPerMarket;
    int32_t IssueStartDate;
    char Reserved14;
    int32_t InterestPaymentDate;
    char Reserved15;
    int32_t IssueMaturityDate;
    char Reserved16;
    int32_t MarginPercentage;
    char Reserved17;
    int32_t MinimumLotQuantity;
    char Reserved18;
    int32_t BoardLotQuantity;
    char Reserved19;
    int32_t TickSize;
    char Reserved20;
    double IssuedCapital;
    char Reserved21;
    int32_t FreezeQuantity;
    char Reserved22;
    int32_t WarningQuantity;
    char Reserved23;
    int32_t ListingDate;
    char Reserved24;
    int32_t ExpulsionDate;
    char Reserved25;
    int32_t ReadmissionDate;
    char Reserved26;
    int32_t RecordDate;
    char Reserved27;
    int32_t NoDeliveryStartDate;
    char Reserved28;
    int32_t NoDeliveryEndDate;
    char Reserved29;
    int32_t LowPriceRange;
    char Reserved30;
    int32_t HighPriceRange;
    char Reserved31;
    int32_t ExDate;
    char Reserved32;
    int32_t BookClosureStartDate;
    char Reserved33;
    int32_t BookClosureEndDate;
    char Reserved34;
    int32_t LocalLDBUpdateDateTime;
    char Reserved35;
    int32_t ExerciseStartDate;
    char Reserved36;
    int32_t ExerciseEndDate;
    char Reserved37;
    int16_t TickerSelection;
    char Reserved38;
    int32_t OldTokenNumber;
    char Reserved39;
    char CreditRating[12];
    char Reserved40;
    char Name[25];
    char Reserved41;
    char EGMAGM;
    char Reserved42;
    char InterestDividend;
    char Reserved43;
    char RightsBonus;
    char Reserved44;
    char MFAON;
    char Reserved45;
    char Remarks[24];
    char Reserved46;
    char ExStyle;
    char Reserved47;
    char ExAllowed;
    char Reserved48;
    char ExRejectionAllowed;
    char Reserved49;
    char PIAllowed;
    char Reserved50;
    char SettlementIndicator;
    char Reserved51;
    char IsCorporateAdjusted;
    char Reserved52;
    char SymbolForAsset[10];
    char Reserved53;
    char InstrumentOfAsset[6];
    char Reserved54;
    int32_t BasePrice;
    char Reserved55;
    char DeleteFlag;
};


struct PARTICIPANT_FILE_HEADER {
    char NSEFO[6];
    char Reserved;
    char VersionNumber[5];
    char Reserved[2];
};


struct PARTICIPANT_STRUCTURE {
    char ParticipantId[12];
    char Reserved;
    char ParticipantName[25];
    char Reserved;
    char ParticipantStatus;
    char Reserved;
    char DeleteFlag;
    char Reserved;
    int32_t LastUpdateTime;
};


struct SECURITY_FILE_HEADER {
    char NEATCM[6];
    char Reserved;
    char VersionNumber[7];
    char Reserved;
    int32_t CreationTime;
};


struct ST_SEC_ELIGIBILITY_PER_MARKET_5_BYTES {
    int16_t SecurityStatus;
    char Reserved;
    char Eligibility;
    char Reserved;
};


struct SECURITY_STRUCTURE {
    int32_t Token;
    char Reserved;
    char Symbol[10];
    char Reserved;
    char Series[2];
    char Reserved;
    int16_t InstrumentType;
    char Reserved;
    double IssuedCapital;
    char Reserved;
    int16_t PermittedToTrade;
    char Reserved;
    char CreditRating[17];
    char Reserved;
    ST_SEC_ELIGIBILITY_PER_MARKET_5_BYTES EligibilityPerMarket;
    int32_t BoardLotQty;
    char Reserved;
    int32_t TickSize;
    char Reserved;
    char Name[25];
    char Reserved;
    int16_t IssueRate;
    char Reserved;
    int32_t IssueStartDate;
    char Reserved;
    int32_t IssuePDate;
    char Reserved;
    int32_t IssueMaturityDate;
    char Reserved;
    int16_t FreezePercent;
    char Reserved;
    int32_t ListingDate;
    char Reserved;
    int32_t ExpulsionDate;
    char Reserved;
    int32_t ReAdmissionDate;
    char Reserved;
    int32_t ExDate;
    char Reserved;
    int32_t RecordDate;
    char Reserved;
    int32_t NoDeliveryStartDate;
    char Reserved;
    int32_t NoDeliveryEndDate;
    char Reserved;
    char ParticipateIndex;
    char Reserved;
    char AON;
    char Reserved;
    char MinFill;
    char Reserved;
    int16_t WarningPercent;
    char Reserved;
    int32_t BookClosureStartDate;
    char Reserved;
    int32_t BookClosureEndDate;
    char Reserved;
    char Dividend;
    char Reserved;
    char Rights;
    char Reserved;
    char Bonus;
    char Reserved;
    char Interest;
    char Reserved;
    char AGM;
    char Reserved;
    char EGM;
    char Reserved;
    char Remark[25];
    char Reserved;
    int32_t LocalDBUpdateDateTime;
    char Reserved;
    char DeleteFlag;
    char Reserved;
    int32_t FaceValue;
    char Reserved;
    char ISIN[12];
};


struct CONTRACT_DESC_TR {
    char InstrumentName[6];
    char Symbol[10];
    int32_t ExpiryDate;
    int32_t StrikePrice;
    char OptionType[2];
};


struct MS_OE_REQUEST_TR {
    int16_t TransactionCode;
    int32_t UserID;
    int16_t ReasonCode;
    int32_t TokenNo;
    CONTRACT_DESC_TR ContractDesc;
    char AccountNumber[10];
    int16_t BookType;
    int16_t BuySellIndicator;
    int32_t DisclosedVolume;
    int32_t Volume;
    int32_t Price;
    int32_t GoodTillDate;
    ST_ORDER_FLAGS_SMALL_ENDIAN OrderFlags;
    int16_t BranchId;
    int32_t TraderId;
    char BrokerId[5];
    char OpenClose;
    char Settlor[12];
    int16_t ProClientIndicator;
    ADDITIONAL_ORDER_FLAGS_SMALL_ENDIAN AdditionalOrderFlags;
    int32_t Filler;
    double NnField;
    char PAN[10];
    int32_t AlgoID;
    int16_t Reserved1;
    char Reserved2[32];
};


struct MS_OM_REQUEST_TR {
    int16_t TransactionCode;
    int32_t UserID;
    char ModifiedCancelledBy;
    int32_t TokenNo;
    CONTRACT_DESC_TR ContractDesc;
    double OrderNumber;
    char AccountNumber[10];
    int16_t BookType;
    int16_t BuySellIndicator;
    int32_t DisclosedVolume;
    int32_t DisclosedVolumeRemaining;
    int32_t TotalVolumeRemaining;
    int32_t Volume;
    int32_t VolumeFilledToday;
    int32_t Price;
    int32_t GoodTillDate;
    int32_t EntryDateTime;
    int32_t LastModified;
    ST_ORDER_FLAGS_SMALL_ENDIAN OrderFlags;
    int16_t BranchId;
    int32_t TraderId;
    char BrokerId[5];
    char OpenClose;
    char Settlor[12];
    int16_t ProClientIndicator;
    ADDITIONAL_ORDER_FLAGS_SMALL_ENDIAN AdditionalOrderFlags;
    int32_t Filler;
    double NnField;
    char PAN[10];
    int32_t AlgoID;
    int16_t Reserved;
    int64_t LastActivityReference;
    char Reserved2[24];
};


struct MS_OE_RESPONSE_TR {
    int16_t TransactionCode;
    int32_t LogTime;
    int32_t UserId;
    int16_t ErrorCode;
    int64_t TimeStamp1;
    char TimeStamp2;
    char ModifiedCancelledBy;
    int16_t ReasonCode;
    int32_t TokenNo;
    CONTRACT_DESC_TR ContractDesc;
    char CloseoutFlag;
    double OrderNumber;
    char AccountNumber[10];
    int16_t BookType;
    int16_t BuySellIndicator;
    int32_t DisclosedVolume;
    int32_t DisclosedVolumeRemaining;
    int32_t TotalVolumeRemaining;
    int32_t Volume;
    int32_t VolumeFilledToday;
    int32_t Price;
    int32_t GoodTillDate;
    int32_t EntryDateTime;
    int32_t LastModified;
    ST_ORDER_FLAGS_SMALL_ENDIAN OrderFlags;
    int16_t BranchId;
    int32_t TraderId;
    char BrokerId[5];
    char OpenClose;
    char Settlor[12];
    int16_t ProClientIndicator;
    ADDITIONAL_ORDER_FLAGS_SMALL_ENDIAN AdditionalOrderFlags;
    int32_t Filler;
    double NnField;
    int64_t TimeStamp;
    char PAN[10];
    int32_t AlgoID;
    int16_t Reserved;
    int64_t LastActivityReference;
    char Reserved[52];
};


struct MS_TRADE_CONFIRM_TR {
    int16_t TransactionCode;
    int32_t LogTime;
    int32_t TraderId;
    int64_t Timestamp;
    double Timestamp1;
    double Timestamp2;
    double ResponseOrderNumber;
    char BrokerId[5];
    char Reserved[1];
    char AccountNumber[10];
    int16_t BuySellIndicator;
    int32_t OriginalVolume;
    int32_t DisclosedVolume;
    int32_t RemainingVolume;
    int32_t DisclosedVolumeRemaining;
    int32_t Price;
    ST_ORDER_FLAGS_SMALL_ENDIAN OrderFlags;
    int32_t GoodTillDate;
    int32_t FillNumber;
    int32_t FillQuantity;
    int32_t FillPrice;
    int32_t VolumeFilledToday;
    char ActivityType[2];
    int32_t ActivityTime;
    int32_t Token;
    CONTRACT_DESC_TR ContractDesc;
    char OpenClose[1];
    char BookType[1];
    char Participant[12];
    ADDITIONAL_ORDER_FLAGS_SMALL_ENDIAN AdditionalOrderFlags;
    char PAN[10];
    int32_t AlgoID;
    int16_t Reserved2;
    int64_t LastActivityReference;
    char Reserved3[52];
};

#pragma pack(pop)

// Transaction Codes
namespace TransactionCodes {
    const int16_t SIGNON_REQUEST_IN = 2300;
    const int16_t SIGNON_REQUEST_OUT = 2301;
    const int16_t SIGN_OFF_REQUEST_IN = 2320;
    const int16_t SIGN_OFF_REQUEST_OUT = 2321;
    const int16_t SYSTEM_INFO_REQUEST = 1600;
    const int16_t SYSTEM_INFO_DATA = 1601;
    const int16_t UPDATE_LOCAL_DATABASE = 7300;
    const int16_t UPDATE_LOCAL_DATABASE_HEADER = 7307;
    const int16_t UPDATE_LOCAL_DATABASE_DATA = 7304;
    const int16_t BCAST_PART_MSTR_CHG = 7306;
    const int16_t PARTIAL_SYSTEM_INFORMATION = 7321;
    const int16_t EXCHANGE_PORTFOLIO_REQUEST = 1775;
    const int16_t EXCHANGE_PORTFOLIO_RESPONSE = 1776;
    const int16_t MESSAGE_DOWNLOAD = 7000;
    const int16_t MESSAGE_DOWNLOAD_HEADER = 7011;
    const int16_t MESSAGE_DOWNLOAD_DATA = 7021;
    const int16_t MESSAGE_DOWNLOAD_TRAILER = 7031;
    const int16_t ORDER_ENTRY_REQUEST = 2000;
    const int16_t PRICE_CONFIRMATION = 2012;
    const int16_t ORDER_CONFIRMATION_OUT = 2073;
    const int16_t ORDER_CANCEL_CONFIRMATION = 2075;
    const int16_t FREEZE_TO_CONTROL = 2170;
    const int16_t ORDER_ERROR_OUT = 2231;
    const int16_t PRICE_MODIFICATION_REQUEST = 2013;
    const int16_t ORDER_MOD_CONFIRM_OUT = 2074;
    const int16_t ORDER_MOD_REJ_OUT = 2042;
    const int16_t ORDER_CANCEL_IN = 2070;
    const int16_t ORDER_CANCEL_RESPONSE = 2071;
    const int16_t ORDER_CXL_REJ_OUT = 2072;
    const int16_t ORDER_CANCEL_CONFIRM_OUT = 2075;
    const int16_t BATCH_ORDER_CANCEL = 9002;
    const int16_t KILL_SWITCH_IN = 2062;
    const int16_t ORDER_ENTRY_REQUEST_TR = 20000;
    const int16_t ORDER_MODIFY_REQUEST_TR = 20040;
    const int16_t TRADE_MOD_IN = 5445;
    const int16_t TRADE_ERROR = 2223; 
    const int16_t TRADE_CANCEL_IN = 5440;
    const int16_t TRADE_CANCEL_OUT = 5441;
    const int16_t SP_BOARD_LOT_IN = 2100;
    const int16_t SP_ORDER_MOD_IN = 2118;
    const int16_t SP_ORDER_CANCEL_IN = 2106;
    const int16_t SP_ORDER_CONFIRMATION = 2124;
    const int16_t SP_ORDER_MOD_CON_OUT = 2136;
    const int16_t SP_ORDER_CXL_CONFIRMATION = 2130;
    const int16_t SP_ORDER_MOD_REJ_OUT = 2133;
    const int16_t SP_ORDER_CXL_REJ_OUT = 2127;
    const int16_t SP_ORDER_ERROR = 2154;
    const int16_t BATCH_SPREAD_CXL_OUT = 9004;
    const int16_t BCAST_SPD_MSTR_CHG = 7309;
    const int16_t BCAST_SPD_MSTR_CHG_PERIODIC = 7341;
}

// Error Codes
namespace ErrorCodes {
    const int16_t SUCCESS = 0;
    const int16_t ERR_INVALID_ORDER_NUMBER = 16013;
    const int16_t e$not_your_order = 16014;
    const int16_t USER_NOT_FOUND = 16042;
    const int16_t ERR_INVALID_TRADER_ID = 16154;
    const int16_t OE_IS_NOT_ACTIVE = 16285;
    const int16_t OE_QTY_FREEZE_CAN = 16307;
    const int16_t OE_PRICE_FREEZE_CAN = 16308;
    const int16_t OE_ORD_CANNOT_CANCEL = 16344;
    const int16_t OE_ORD_CANNOT_MODIFY = 16346;
    const int16_t INVALID_ORDER = 16419;
    const int16_t CLOSEOUT_ORDER_REJECT = 16686;
    const int16_t CLOSEOUT_NOT_ALLOWED = 16688;
    const int16_t CLOSEOUT_TRDMOD_REJECT = 16690;
    const int16_t MARKET_CLOSED = 16000;
    const int16_t e$gtc_gtd_ord_not_allowed_pclose = 16229;
    const int16_t e$invalid_contract_comb = 16627; 
    const int16_t e$invalid_pro_client = 16414;
    const int16_t e$invalid_cli_ac = 16632;
    const int16_t OE_QUANTITY_NOT_MULT_RL = 16328;
    const int16_t e$price_diff_out_of_range = 16713;
}

// Reason Codes
namespace ReasonCodes {
    const int16_t NORMAL_CONFIRMATION = 0;
    const int16_t PRICE_FREEZE = 17;
    const int16_t QUANTITY_FREEZE = 18;
}

// Broker Types
namespace BrokerTypes {
    const char CORPORATE_MANAGER = 'C';
    const char BRANCH_MANAGER = 'B';
    const char DEALER = 'D';
}