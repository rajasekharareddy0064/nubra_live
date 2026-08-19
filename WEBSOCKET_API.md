# WebSocket API - Frontend Integration Guide

## Connection Details

### Endpoint
```
wss://nubra-live-791197716058.asia-south1.run.app/ws/live
```

**Protocol:** WebSocket (wss:// for secure)  
**Path:** `/ws/live`  
**Authentication:** None required (public broadcast)  
**Reconnection:** Client should implement auto-reconnect on disconnect

---

## Connection Example

### JavaScript (Browser)
```javascript
const WS_URL = 'wss://nubra-live-791197716058.asia-south1.run.app/ws/live';
const ws = new WebSocket(WS_URL);

ws.onopen = () => {
    console.log('✅ Connected to Nubra Live');
};

ws.onmessage = (event) => {
    const data = JSON.parse(event.data);
    handleMessage(data);
};

ws.onerror = (error) => {
    console.error('WebSocket error:', error);
};

ws.onclose = (event) => {
    console.log(`Disconnected: code=${event.code}`);
    // Implement reconnect logic here
};

function handleMessage(data) {
    switch (data.type) {
        case 'ws_hello':
            console.log('Welcome message:', data);
            break;
        case 'tick':
            updateTickData(data);
            break;
        case 'candle_3m':
            updateClosedCandle(data);
            break;
        case 'candle_3m_open':
            updateOpenCandle(data);
            break;
        case 'option_chain':
            updateOptionChain(data);
            break;
        case 'ws_ping':
            // Server heartbeat - respond to keep connection alive
            ws.send(JSON.stringify({ 
                type: 'ping', 
                client_time: Date.now() / 1000 
            }));
            break;
    }
}
```

### React Hook
```typescript
import { useEffect, useRef, useState } from 'react';

interface WebSocketHook {
    isConnected: boolean;
    lastMessage: any;
    send: (data: any) => void;
}

export function useNubraWebSocket(url: string): WebSocketHook {
    const ws = useRef<WebSocket | null>(null);
    const [isConnected, setIsConnected] = useState(false);
    const [lastMessage, setLastMessage] = useState<any>(null);

    useEffect(() => {
        const connect = () => {
            ws.current = new WebSocket(url);

            ws.current.onopen = () => {
                setIsConnected(true);
                console.log('WebSocket connected');
            };

            ws.current.onmessage = (event) => {
                const data = JSON.parse(event.data);
                setLastMessage(data);
                
                // Auto-respond to heartbeat
                if (data.type === 'ws_ping') {
                    ws.current?.send(JSON.stringify({ 
                        type: 'ping', 
                        client_time: Date.now() / 1000 
                    }));
                }
            };

            ws.current.onerror = (error) => {
                console.error('WebSocket error:', error);
            };

            ws.current.onclose = () => {
                setIsConnected(false);
                console.log('WebSocket disconnected, reconnecting in 3s...');
                setTimeout(connect, 3000);
            };
        };

        connect();

        return () => {
            ws.current?.close();
        };
    }, [url]);

    const send = (data: any) => {
        if (ws.current?.readyState === WebSocket.OPEN) {
            ws.current.send(JSON.stringify(data));
        }
    };

    return { isConnected, lastMessage, send };
}

// Usage in component
function TradingDashboard() {
    const { isConnected, lastMessage } = useNubraWebSocket(
        'wss://nubra-live-791197716058.asia-south1.run.app/ws/live'
    );

    useEffect(() => {
        if (lastMessage?.type === 'candle_3m') {
            console.log('New closed candle:', lastMessage);
        }
    }, [lastMessage]);

    return (
        <div>
            Status: {isConnected ? '🟢 Connected' : '🔴 Disconnected'}
        </div>
    );
}
```

---

## Message Types

### 1. `ws_hello` (On Connect)
**Direction:** Server → Client  
**Frequency:** Once on connection

Handshake message with server configuration and current candle timing.

```json
{
  "type": "ws_hello",
  "interval_minutes": 3,
  "timezone": "Asia/Kolkata",
  "current_bucket_start": "2026-06-30T13:03:00+05:30",
  "seconds_until_next_closed_candle": 142.5,
  "howto": "Closed 3m bars are sent as JSON with type='candle_3m'..."
}
```

**On connect, you also receive:**
- Last 50 closed candles (history)
- Current open candle (`candle_3m_open`)
- Latest option chain snapshot

---

### 2. `tick` (Real-time Tick Data)
**Direction:** Server → Client  
**Frequency:** High (multiple per second)

Individual price/volume updates for index, futures, and options.

```json
{
  "type": "tick",
  "channel": "index",
  "key": "NIFTY",
  "data": {
    "symbol": "NIFTY",
    "ltp": 24532.45,
    "volume": 125000,
    "oi": null,
    "change_pct": 0.23,
    "timestamp": "2026-06-30T13:05:42+05:30"
  }
}
```

**Channels:**
- `index` — NIFTY spot index, NIFTY futures
- `orderbook` — Order book updates (bid/ask levels)
- `greeks` — Option Greeks (delta, gamma, theta, vega, IV)
- `option` — Option chain snapshots

**Common Keys:**
- `NIFTY` — NIFTY 50 index
- `NIFTY_FUT:NIFTY26JUNFUT` — NIFTY futures
- `STOCK_FUT:RELIANCE26JUNFUT` — Stock futures

---

### 3. `candle_3m` (Closed 3-Minute Candle)
**Direction:** Server → Client  
**Frequency:** Every 3 minutes (on boundary)

Complete OHLCV candle for the just-closed 3-minute interval.

```json
{
  "type": "candle_3m",
  "bucket_id": "2026-06-30T13:03:00+05:30:3m",
  "bucket_start": "2026-06-30T13:00:00+05:30",
  "bucket_end": "2026-06-30T13:03:00+05:30",
  "interval_minutes": 3,
  "is_empty": false,
  
  "nifty": {
    "open": 24500.00,
    "high": 24550.25,
    "low": 24495.10,
    "close": 24532.45,
    "volume": 125000,
    "change_pct": 0.13,
    "tick_count": 423
  },
  
  "futures": {
    "NIFTY26JUNFUT": {
      "open": 24505.00,
      "high": 24555.50,
      "low": 24500.00,
      "close": 24538.75,
      "volume": 85000,
      "oi": 1250000,
      "tick_count": 180,
      "is_empty": false,
      "contract": "current",
      "underlying_symbol": "NIFTY"
    }
  },
  
  "stocks": {
    "RELIANCE26JUNFUT": {
      "open": 1402.55,
      "high": 1410.00,
      "low": 1400.10,
      "close": 1408.25,
      "volume": 125000,
      "oi": 98000,
      "tick_count": 89,
      "is_empty": false,
      "underlying_symbol": "RELIANCE"
    }
  },
  
  "options": {
    "chain": [
      {
        "strike": 24500,
        "CE": {
          "ltp": 125.50,
          "oi": 450000,
          "volume": 12000,
          "delta": 0.52,
          "gamma": 0.0012,
          "theta": -8.5,
          "vega": 15.2,
          "iv": 0.185
        },
        "PE": {
          "ltp": 98.25,
          "oi": 520000,
          "volume": 15000,
          "delta": -0.48,
          "gamma": 0.0012,
          "theta": -7.8,
          "vega": 15.1,
          "iv": 0.192
        }
      }
    ],
    "metrics": {
      "atm_strike": 24500,
      "total_ce_oi": 12500000,
      "total_pe_oi": 15200000,
      "pcr": 1.216
    },
    "summary": {
      "total_ce_oi": 12500000,
      "total_pe_oi": 15200000,
      "ce_oi_change": 125000,
      "pe_oi_change": 180000
    }
  },
  
  "order_book": {
    "atm": 24532.45,
    "exec_delta": 1250,
    "book_delta": -850,
    "imbalance": 0.1245,
    "breakout_score": 68.5,
    "regime": "bullish",
    "strikes": [ /* order book per strike */ ]
  },
  
  "stock_futures_summary": {
    "bullish_count": 28,
    "bearish_count": 15,
    "neutral_count": 6
  },
  
  "analytics": { /* ML metrics, volatility, etc. */ },
  
  "meta": {
    "index": { /* NIFTY candle details */ },
    "high_liquid_symbols": ["RELIANCE", "HDFCBANK", "ICICIBANK", "INFY", "SBIN"],
    "nifty_fut_contracts": {
      "NIFTY26JUNFUT": "current",
      "NIFTY26JULFUT": "next"
    }
  }
}
```

**Key Fields:**
- `bucket_start` / `bucket_end` — Time range (IST)
- `nifty` — NIFTY 50 index OHLCV
- `futures` — NIFTY futures (current, next, far month)
- `stocks` — NIFTY 50 stock futures (up to 49 stocks)
- `options.chain` — ATM-centered option chain (±10 strikes)
- `order_book` — Order flow analytics

**All prices are in RUPEES (₹), not paise.**

---

### 4. `candle_3m_open` (Open/Current Candle Preview)
**Direction:** Server → Client  
**Frequency:** Periodic (every ~30s)

Snapshot of the currently-building candle (before it closes).

```json
{
  "type": "candle_3m_open",
  "bucket_start": "2026-06-30T13:03:00+05:30",
  "seconds_remaining": 95,
  "nifty": {
    "open": 24532.45,
    "high": 24545.00,
    "low": 24530.00,
    "close": 24542.10,
    "volume": 45000,
    "tick_count": 142
  }
}
```

Use this for live chart updates before the 3-minute boundary.

---

### 5. `option_chain` (Option Chain Update)
**Direction:** Server → Client  
**Frequency:** On NIFTY tick (throttled to 500ms)

Updated option chain centered around the current ATM strike.

```json
{
  "type": "option_chain",
  "spot": 24532.45,
  "atm": 24550,
  "strike_radius": 10,
  "updated_at": 1719734742.5,
  "chain": [
    {
      "strike": 24500,
      "CE": { "ltp": 125.50, "delta": 0.52, "iv": 0.185, "oi": 450000, "volume": 12000 },
      "PE": { "ltp": 98.25, "delta": -0.48, "iv": 0.192, "oi": 520000, "volume": 15000 }
    }
  ],
  "metrics": {
    "atm_strike": 24550,
    "total_ce_oi": 12500000,
    "total_pe_oi": 15200000,
    "pcr": 1.216,
    "atm_iv_mid": 0.188
  }
}
```

---

### 6. `ws_ping` (Server Heartbeat)
**Direction:** Server → Client  
**Frequency:** Every 15 seconds (when no other messages)

Server heartbeat to keep connection alive.

```json
{
  "type": "ws_ping",
  "server_time": 1719734742.5,
  "uptime_seconds": 142.3
}
```

**Client should respond:**
```javascript
ws.send(JSON.stringify({ 
    type: 'ping', 
    client_time: Date.now() / 1000 
}));
```

Server will reply with `pong`:
```json
{
  "type": "pong",
  "server_time": 1719734742.6,
  "client_time": 1719734742.5
}
```

---

## Data Flow

### High-Frequency Updates (Ticks)
```
Nubra API → WebSocket Ingestion → Pipeline → LiveHub → Frontend
                                      ↓
                              (filters/aggregates)
```

**Message Rate:**
- `tick` messages: 10-50 per second during market hours
- `option_chain`: ~2 per second (throttled)
- `candle_3m`: Once every 3 minutes
- `ws_ping`: Every 15 seconds (idle)

### 3-Minute Candle Flow
```
09:15:00 IST ─┐
09:16:00      │  Accumulating ticks...
09:17:00      │
09:18:00 ─────┴─→ Closed candle emitted (type='candle_3m')
                  bucket_start: 09:15:00
                  bucket_end:   09:18:00
```

---

## Best Practices

### 1. Message Filtering
Don't process every tick — filter by message type and channel:

```javascript
ws.onmessage = (event) => {
    const data = JSON.parse(event.data);
    
    // Only process closed candles for charting
    if (data.type === 'candle_3m') {
        updateChart(data.nifty);
    }
    
    // Only show NIFTY spot price
    if (data.type === 'tick' && data.channel === 'index' && data.key === 'NIFTY') {
        updateLivePrice(data.data.ltp);
    }
    
    // Ignore orderbook noise unless building order flow viz
    if (data.type === 'tick' && data.channel === 'orderbook') {
        return; // skip
    }
};
```

### 2. Reconnection Logic
Implement exponential backoff:

```javascript
let reconnectDelay = 1000;
const MAX_DELAY = 30000;

function connect() {
    const ws = new WebSocket(WS_URL);
    
    ws.onclose = () => {
        console.log(`Reconnecting in ${reconnectDelay}ms...`);
        setTimeout(connect, reconnectDelay);
        reconnectDelay = Math.min(reconnectDelay * 2, MAX_DELAY);
    };
    
    ws.onopen = () => {
        reconnectDelay = 1000; // Reset on successful connect
    };
}
```

### 3. Buffering for Chart Updates
Avoid re-rendering on every tick:

```javascript
let tickBuffer = [];
let updateTimer = null;

ws.onmessage = (event) => {
    const data = JSON.parse(event.data);
    if (data.type === 'tick') {
        tickBuffer.push(data);
        
        if (!updateTimer) {
            updateTimer = setTimeout(() => {
                processTickBatch(tickBuffer);
                tickBuffer = [];
                updateTimer = null;
            }, 250); // Batch updates every 250ms
        }
    }
};
```

### 4. State Management (Redux/Zustand)
```typescript
// Zustand store example
interface MarketStore {
    niftyLtp: number;
    lastCandle: any;
    optionChain: any[];
    isConnected: boolean;
}

const useMarketStore = create<MarketStore>((set) => ({
    niftyLtp: 0,
    lastCandle: null,
    optionChain: [],
    isConnected: false,
    
    updateFromWebSocket: (message: any) => {
        if (message.type === 'tick' && message.key === 'NIFTY') {
            set({ niftyLtp: message.data.ltp });
        } else if (message.type === 'candle_3m') {
            set({ lastCandle: message });
        } else if (message.type === 'option_chain') {
            set({ optionChain: message.chain });
        }
    },
}));
```

---

## Example: Complete Trading Dashboard

```typescript
import { useEffect, useState } from 'react';

interface CandleData {
    timestamp: string;
    open: number;
    high: number;
    low: number;
    close: number;
    volume: number;
}

function TradingDashboard() {
    const [niftyLtp, setNiftyLtp] = useState<number>(0);
    const [candles, setCandles] = useState<CandleData[]>([]);
    const [optionChain, setOptionChain] = useState<any[]>([]);
    const [isConnected, setIsConnected] = useState(false);
    
    useEffect(() => {
        const ws = new WebSocket(
            'wss://nubra-live-791197716058.asia-south1.run.app/ws/live'
        );
        
        ws.onopen = () => setIsConnected(true);
        ws.onclose = () => setIsConnected(false);
        
        ws.onmessage = (event) => {
            const data = JSON.parse(event.data);
            
            switch (data.type) {
                case 'tick':
                    if (data.channel === 'index' && data.key === 'NIFTY') {
                        setNiftyLtp(data.data.ltp);
                    }
                    break;
                    
                case 'candle_3m':
                    setCandles(prev => [...prev, {
                        timestamp: data.bucket_end,
                        ...data.nifty
                    }].slice(-100)); // Keep last 100 candles
                    break;
                    
                case 'option_chain':
                    setOptionChain(data.chain);
                    break;
                    
                case 'ws_ping':
                    ws.send(JSON.stringify({ 
                        type: 'ping', 
                        client_time: Date.now() / 1000 
                    }));
                    break;
            }
        };
        
        return () => ws.close();
    }, []);
    
    return (
        <div>
            <header>
                <h1>NIFTY: ₹{niftyLtp.toFixed(2)}</h1>
                <span>{isConnected ? '🟢 Live' : '🔴 Disconnected'}</span>
            </header>
            
            <section>
                <h2>3-Min Candles ({candles.length})</h2>
                <CandleChart data={candles} />
            </section>
            
            <section>
                <h2>Option Chain</h2>
                <OptionChainTable data={optionChain} />
            </section>
        </div>
    );
}
```

---

## Performance Tips

### 1. Use Web Workers for Heavy Processing
```javascript
// market-worker.js
self.onmessage = (e) => {
    const message = JSON.parse(e.data);
    
    // Process heavy calculations (Greeks, P&L, etc.)
    const processed = calculatePortfolioPnL(message);
    
    self.postMessage(processed);
};

// main.js
const worker = new Worker('market-worker.js');
ws.onmessage = (event) => {
    worker.postMessage(event.data);
};
```

### 2. Throttle UI Updates
```javascript
import { throttle } from 'lodash';

const updatePrice = throttle((ltp) => {
    setPriceDisplay(ltp);
}, 100); // Max 10 updates/second

ws.onmessage = (event) => {
    const data = JSON.parse(event.data);
    if (data.type === 'tick') {
        updatePrice(data.data.ltp);
    }
};
```

### 3. Virtual Scrolling for Large Lists
Use `react-window` or `react-virtualized` for option chains (100+ strikes).

---

## Troubleshooting

### Connection Refused
```
Error: WebSocket connection to 'wss://...' failed
```
- Check URL is correct
- Verify service is running: `curl https://SERVICE_URL/health`
- Check CORS if connecting from browser (server allows all origins)

### Messages Not Arriving
- Market might be closed (check `/health/ready` → `ingestion.state`)
- WebSocket might be idle (expect `ws_ping` every 15s)
- Check browser dev tools → Network → WS tab for errors

### High Memory Usage
- Limit stored candle history (use `.slice(-100)`)
- Clear old option chain snapshots
- Use message filtering to ignore noise

---

## Test Page

A complete test page is available at:
```
c:\trading_code\Nubra_live\ws_test.html
```

Open it in a browser to verify connectivity and see live messages.

---

## Support

**Endpoint:** `wss://nubra-live-791197716058.asia-south1.run.app/ws/live`  
**Health Check:** `https://nubra-live-791197716058.asia-south1.run.app/health`  
**API Docs:** `https://nubra-live-791197716058.asia-south1.run.app/docs`  

**Market Hours:** Mon-Fri, 09:15 - 15:30 IST  
**Timezone:** Asia/Kolkata (IST)  
**Candle Interval:** 3 minutes
