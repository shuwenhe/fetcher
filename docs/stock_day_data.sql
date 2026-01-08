CREATE TABLE quant.stock_day_data (
    ts_code String,
    trade_date String,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    pre_close Float64,
    change Float64,
    pct_chg Float64,
    vol Float64,
    amount Float64,
    date_stamp Float64
) ENGINE = MergeTree()
ORDER BY (ts_code, trade_date);
