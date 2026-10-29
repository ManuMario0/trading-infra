/// I will put here a structure that reflects the IEX TOPS Binary Feed.
///
/// I will not nevessarily explicitly built the entire message deserialized in the code
/// those are purely notes to have a global view of the protocol. Most of the infos
/// will be decoded and not stored strcutured in memory for speed reasons.
use crate::pump::cursor::FileCursor;

pub enum Message {
    SystemEvent(SystemEvent),                           // 'S' (Ox53)
    SecurityDirectory(SecurityDirectory),               // 'D' (Ox44)
    TradingStatus(TradingStatus),                       // 'H' (Ox48)
    RetailLiquidityIndicator(RetailLiquidityIndicator), // 'I' (Ox49)
    QuoteUpdateMessage(QuoteUpdateMessage),             // 'Q' (Ox51)
    TradeReportMessage(TradeReportMessage),             // 'T' (Ox54)
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct Timestamp(u64);

#[allow(dead_code)]
pub struct SystemEvent {
    system_event: SystemEventIdentifier, // 1 byte
    timestamp: Timestamp,                // 8 bytes
}

pub enum SystemEventIdentifier {
    StartOfMessages,           // 'O' (Ox4f)
    StartOfSystemHours,        // 'S' (Ox53)
    StartOfRegularMarketHours, // 'R' (Ox52)
    EndOfRegularMarketHours,   // 'M' (Ox4d)
    EndOfSystemHours,          // 'E' (Ox45)
    EnfOdMessages,             // 'C' (Ox43)
    Other,                     // One always should assume that a wild message appeared in the feed
}

#[allow(dead_code)]
pub struct SecurityDirectory {
    flags: u8,            // 1 byte
    timestamp: Timestamp, // 8 bytes
    symbol: [u8; 8],      // 8 bytes
    /// This represents the smallest unit of trading for a security.
    round_lot_size: u32, // 4 bytes
    /// This is the adjusted closing price of the security.
    adjusted_poc_price: u32, // 4 bytes
    /// This is the limit up / limit down tier of the security.
    ///
    /// It is one of:
    /// - 0 (0x0): Not applicable
    /// - 1 (0x1): Tier 1 NMS Stock
    /// - 2 (0x2): Tier 2 NMS Stock
    luld_tier: u8, // 1 byte
}

#[allow(dead_code)]
pub struct TradingStatus {
    trading_status: TradingStatusIdentifier, // 1 byte
    timestamp: Timestamp,                    // 8 bytes
    symbol: [u8; 8],                         // 8 bytes
    reason: TradingStatusReason,             // 4 bytes
}

pub enum TradingStatusIdentifier {
    TradingHalt,      // 'H' (Ox48)
    TradingHaltOAP,   // 'O' (Ox4f)
    TradingPausedOAP, // 'P' (Ox50)
    Trading,          // 'T' (Ox54)
    Other,            // One always should assume that a wild message appeared in the feed
}

pub enum TradingStatusReason {
    HaltNewsPending,                // 'T1'
    IPONotYetTrading,               // 'IPO1'
    IPODeferred,                    // 'IPOD'
    MarketWideCircuitBreakerLevel3, // 'MCB3'
    ReasonNotAvailable,             // 'NA'
    HaltNewsDissemination,          // 'T2'
    IPOOrderAcceptancePeriod,       // 'IPO2'
    IPOPreLaunchPeriod,             // 'IPO3'
    MarketWideCircuitBreakerLevel1, // 'MCB1'
    MarketWideCircuitBreakerLevel2, // 'MCB2'
}

#[allow(dead_code)]
pub struct RetailLiquidityIndicator {
    retail_liquidity_indicator_identifier: RetailLiquidityIndicatorIdentifier,
    timestamp: Timestamp,
    symbol: [u8; 8],
}

pub enum RetailLiquidityIndicatorIdentifier {
    NotApplicable,               // [space] (0x20)
    BuyInterestForRetail,        // 'A' (0x41)
    SellInterestForRetail,       // 'B' (0x42)
    BuyAndSellInterestForRetail, // 'C' (0x43)
    Other, // One always should assume that a wild message appeared in the feed
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct QuoteUpdateMessage {
    flags: u8,
    timestamp: Timestamp,
    pub symbol: [u8; 8],
    pub bid_size: u32,
    pub bid_price: u64,
    pub ask_price: u64,
    pub ask_size: u32,
}

impl QuoteUpdateMessage {
    /// Reads a QuoteUpdateMessage directly from the cursor at the given offset.
    ///
    /// The message is 42 bytes starting at `offset`:
    ///   [0]    = message type ('Q')
    ///   [1]    = flags
    ///   [2..9] = timestamp (u64 LE)
    ///   [10..17] = symbol (8 bytes)
    ///   [18..21] = bid_size (u32 LE)
    ///   [22..29] = bid_price (u64 LE)
    ///   [30..37] = ask_price (u64 LE)
    ///   [38..41] = ask_size (u32 LE)
    #[inline(always)]
    pub fn from_cursor(cursor: &mut FileCursor, offset: usize) -> Self {
        Self {
            flags: cursor.get(offset + 1),
            timestamp: Timestamp(cursor.read_u64_le(offset + 2)),
            symbol: [
                cursor.get(offset + 10),
                cursor.get(offset + 11),
                cursor.get(offset + 12),
                cursor.get(offset + 13),
                cursor.get(offset + 14),
                cursor.get(offset + 15),
                cursor.get(offset + 16),
                cursor.get(offset + 17),
            ],
            bid_size: cursor.read_u32_le(offset + 18),
            bid_price: cursor.read_u64_le(offset + 22),
            ask_price: cursor.read_u64_le(offset + 30),
            ask_size: cursor.read_u32_le(offset + 38),
        }
    }

    #[inline(always)]
    pub fn from_bytes(bytes: &[u8]) -> Self {
        debug_assert!(bytes.len() == 42);
        Self {
            flags: bytes[1],
            timestamp: Timestamp(u64::from_le_bytes(bytes[2..10].try_into().unwrap())),
            symbol: [
                bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15], bytes[16],
                bytes[17],
            ],
            bid_size: u32::from_le_bytes(bytes[18..22].try_into().unwrap()),
            bid_price: u64::from_le_bytes(bytes[22..30].try_into().unwrap()),
            ask_price: u64::from_le_bytes(bytes[30..38].try_into().unwrap()),
            ask_size: u32::from_le_bytes(bytes[38..42].try_into().unwrap()),
        }
    }
}

#[allow(dead_code)]
pub struct TradeReportMessage {
    sale_condition_flags: u8,
    timestamp: Timestamp,
    symbol: [u8; 8],
    size: u32,
    price: u64,
    trade_id: u64,
}

// ----------------- IEX Protocol Messsage Header ----

#[repr(C, packed)]
pub struct IextpHeader {
    pub version: u8,         // 0x01
    pub reserved: u8,        // Ignore
    pub protocol_id: u16,    // 0x8003 for TOPS
    pub channel_id: u32,     // Uniquely identifies the stream
    pub session_id: u32,     // Identifies the day's session
    pub payload_len: u16,    // Total length of the messages after this header
    pub message_count: u16,  // Number of messages in this packet
    pub stream_offset: u64,  // Byte offset in the stream
    pub first_sequence: u64, // Sequence number of the first message
    pub send_time: u64,      // Nanosecond epoch timestamp
}
