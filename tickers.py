"""
get_top_tickers.py
==================
Fetches the top ~1000 liquid US stock tickers from Interactive Brokers.

Correctly parses the scanner XML to extract ONLY scan codes that are
valid for STK (stocks) — ignoring futures, bonds, forex, etc.

Prerequisites:
    pip install ib_insync
    TWS or IB Gateway running on port 7496 (live)

Usage:
    python get_top_tickers.py
    python get_top_tickers.py 500     # collect 500 instead of 1000
"""

import sys
import time
import xml.etree.ElementTree as ET
from ib_insync import IB, ScannerSubscription

HOST      = "127.0.0.1"
PORT      = 7496
CLIENT_ID = 1

# Preferred codes tried first (in order) if they're valid for STK
PREFERRED_SCANS = [
    "MOST_ACTIVE",
    "MOST_ACTIVE_USD",
    "TOP_PERC_GAIN",
    "TOP_PERC_LOSE",
    "HIGH_VS_52W_HL",
    "HIGH_VS_26W_HL",
    "HIGH_VS_13W_HL",
    "TOP_VOLUME_RATE",
    "TOP_TRADE_COUNT",
    "HOT_BY_VOLUME",
    "HOT_BY_OPT_VOLUME",
    "OPT_VOLUME_MOST_ACTIVE",
    "HIGH_OPT_IMP_VOLAT",
    "HIGH_OPT_IMP_VOLAT_OVER_HIST",
    "TOP_OPEN_PERC_GAIN",
    "HOT_BY_PRICE",
    "TOP_BETA",
    "LOW_PE_RATIO",
    "HIGH_DIVIDEND_YIELD_IB",
    "LOW_OPT_IMP_VOLAT",
]

EXCHANGES = ["NYSE", "NASDAQ"]

FILTERS = dict(
    abovePrice   = 1.0,
    aboveVolume  = 100_000,
    numberOfRows = 50,      # IBKR hard limit per scan call
)


def get_stk_scan_codes(ib: IB) -> list[str]:
    """
    Parse the scanner XML and return only codes that support STK on
    US exchanges. The XML structure is:

        <ScanParameterResponse>
          <InstrumentList>
            <Instrument>
              <name>STK</name>          ← instrument type
              <filters>
                <ScanCode>
                  <code>MOST_ACTIVE</code>
                </ScanCode>
                ...
              </filters>
            </Instrument>
          </InstrumentList>
        </ScanParameterResponse>
    """
    print("Fetching scanner parameters from TWS …")
    xml_str = ib.reqScannerParameters()
    root    = ET.fromstring(xml_str)

    stk_codes: set[str] = set()

    for instrument in root.iter("Instrument"):
        name_el = instrument.find("name")
        if name_el is None or name_el.text is None:
            continue
        if name_el.text.strip() != "STK":
            continue
        # This instrument block is for stocks — collect all its scan codes
        for code_el in instrument.iter("code"):
            if code_el.text:
                stk_codes.add(code_el.text.strip())

    # If the XML structure differs, fall back to ANY scanCode tag
    if not stk_codes:
        print("  ⚠  Could not find STK-specific codes; trying fallback XML parse …")
        # Look for ScanCode elements that have a child <instruments> containing STK
        for scan_el in root.iter("ScanCode"):
            code_el = scan_el.find("code")
            instr_el = scan_el.find("instruments")
            if code_el is None or code_el.text is None:
                continue
            if instr_el is not None and "STK" in (instr_el.text or ""):
                stk_codes.add(code_el.text.strip())

    print(f"  → {len(stk_codes)} STK-compatible scan codes found")
    return list(stk_codes)


def build_scan_queue(stk_codes: list[str]) -> list[str]:
    """Order: preferred first (if valid), then the rest alphabetically."""
    stk_set   = set(stk_codes)
    preferred = [c for c in PREFERRED_SCANS if c in stk_set]
    rest      = sorted(c for c in stk_set if c not in PREFERRED_SCANS)
    return preferred + rest


def fetch_tickers(ib: IB, limit: int = 1000) -> list[str]:
    stk_codes  = get_stk_scan_codes(ib)
    scan_queue = build_scan_queue(stk_codes)

    print(f"\nRunning up to {len(scan_queue)} STK scans across {EXCHANGES} …\n")

    seen:    set[str]  = set()
    tickers: list[str] = []
    done = 0

    for exchange in EXCHANGES:
        for scan_code in scan_queue:
            sub = ScannerSubscription(
                instrument   = "STK",
                locationCode = f"STK.{exchange}",
                scanCode     = scan_code,
                **FILTERS,
            )

            try:
                results = ib.reqScannerData(sub)  # blocking
                new = 0
                for item in results:
                    symbol = item.contractDetails.contract.symbol
                    if symbol not in seen:
                        seen.add(symbol)
                        tickers.append(symbol)
                        new += 1
                time.sleep(0.3)

            except Exception as e:
                print(f"\n  ⚠  Skipping {exchange}/{scan_code}: {e}")
                continue

            done += 1
            print(
                f"  [{done}] {exchange}/{scan_code:<40} "
                f"+{new:<3} → {len(tickers)} total" + " " * 5,
                end="\r",
            )

            if len(tickers) >= limit:
                break
        if len(tickers) >= limit:
            break

    print()
    return tickers[:limit]


def get_top_tickers(limit: int = 1000) -> list[str]:
    """
    Import and call from your backtester:

        from get_top_tickers import get_top_tickers
        TICKERS = get_top_tickers()
    """
    ib = IB()
    print(f"Connecting to {HOST}:{PORT} (clientId={CLIENT_ID}) …")
    ib.connect(HOST, PORT, clientId=CLIENT_ID)
    print("Connected ✓\n")
    try:
        return fetch_tickers(ib, limit=limit)
    finally:
        ib.disconnect()
        print("Disconnected.")


if __name__ == "__main__":
    limit   = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
    tickers = get_top_tickers(limit=limit)
    print(f"\n✓ Collected {len(tickers)} unique tickers")
    print("Sample (first 20):", tickers[:20])