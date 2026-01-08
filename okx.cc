#include <iostream>
#include <fstream>
#include <csignal>
#include <vector>
#include <string>
#include <mutex>
#include <queue>
#include <chrono>
#include <iomanip>
#include <cstring>
#include <thread>
#include <map>
#include <nlohmann/json.hpp>
#include "clickhouse/client.h"
#include <libwebsockets.h>

using json = nlohmann::json;
using namespace clickhouse;

// --- 配置结构（全局）---
static struct AppConfig {
    std::string db_host;
    std::string db_name;
    std::string db_user;
    std::string db_pass;
    std::string db_table;
    size_t max_batch_size = 1000;
    int flush_interval = 5;
    std::vector<std::string> instruments;
} g_cfg;

// --- Tick 数据记录 ---
struct TickRecord {
    std::string instId;
    std::string tradeId;
    double price;
    double size;
    std::string side;
    uint64_t timestamp;
};

// --- 全局变量 ---
volatile sig_atomic_t stopFlag = 0;
std::mutex queueMutex;
std::queue<TickRecord> dataQueue;
std::mutex statsMutex;
std::map<std::string, size_t> tickCountByInstrument;  // 统计每个币种的tick数量

void handleSignal(int signal) { stopFlag = 1; }

struct Logger {
    void log(const std::string& msg) {
        auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        char buf[64];
        std::strftime(buf, sizeof(buf), "%F %T", std::localtime(&now));
        std::cout << "[" << buf << "] " << msg << std::endl;
    }
    
    void logTrade(const std::string& instId, double price, double size, const std::string& side) {
        auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        char buf[64];
        std::strftime(buf, sizeof(buf), "%F %T", std::localtime(&now));
        std::cout << "\033[1;36m[" << buf << "] [TRADE] " << instId 
                  << " | Price: " << std::fixed << std::setprecision(4) << price
                  << " | Size: " << std::setprecision(2) << size
                  << " | Side: " << (side == "buy" ? "\033[1;32m" : "\033[1;31m") << side << "\033[0m"
                  << std::endl;
    }
};

// --- 加载配置 ---
void loadConfig(const std::string& filename) {
    std::ifstream f(filename);
    if (!f.is_open()) throw std::runtime_error("Cannot open config file: " + filename);
    json j;
    f >> j;
    g_cfg.db_host = j["clickhouse"]["host"];
    g_cfg.db_name = j["clickhouse"]["database"];
    g_cfg.db_user = j["clickhouse"]["user"];
    g_cfg.db_pass = j["clickhouse"]["password"];
    g_cfg.db_table = j["clickhouse"]["table"];
    if (j["settings"].contains("max_batch_size")) g_cfg.max_batch_size = j["settings"]["max_batch_size"];
    if (j["settings"].contains("flush_interval_sec")) g_cfg.flush_interval = j["settings"]["flush_interval_sec"];
    g_cfg.instruments = j["instruments"].get<std::vector<std::string>>();
}

// --- ClickHouse 批量写入（带统计信息）---
bool insertBatch(Client& client, std::vector<TickRecord>& batch, Logger& logger) {
    if (batch.empty()) return true;
    
    // 统计每个币种的数量
    std::map<std::string, size_t> batchStats;
    for (const auto& t : batch) {
        batchStats[t.instId]++;
    }
    
    try {
        auto col_inst = std::make_shared<ColumnString>();
        auto col_ts = std::make_shared<ColumnUInt64>();
        auto col_trade_id = std::make_shared<ColumnString>();
        auto col_price = std::make_shared<ColumnFloat64>();
        auto col_size = std::make_shared<ColumnFloat64>();
        auto col_side = std::make_shared<ColumnString>();

        for (const auto& t : batch) {
            col_inst->Append(t.instId);
            col_ts->Append(t.timestamp);
            col_trade_id->Append(t.tradeId);
            col_price->Append(t.price);
            col_size->Append(t.size);
            col_side->Append(t.side);
        }

        Block block;
        block.AppendColumn("inst_id", col_inst);
        block.AppendColumn("timestamp", col_ts);
        block.AppendColumn("trade_id", col_trade_id);
        block.AppendColumn("price", col_price);
        block.AppendColumn("size", col_size);
        block.AppendColumn("side", col_side);

        client.Insert(g_cfg.db_table, block);
        
        // 打印详细统计
        std::cout << "\033[1;32m[BATCH] Inserted " << batch.size() << " records:\033[0m" << std::endl;
        for (const auto& [inst, count] : batchStats) {
            std::cout << "  - " << inst << ": " << count << " ticks" << std::endl;
        }
        
        return true;
    } catch (const std::exception& e) {
        logger.log("ClickHouse Error: " + std::string(e.what()));
        return false;
    }
}

// --- WebSocket 回调（带详细日志）---
static Logger g_logger;  // 全局logger供回调使用

static int ws_callback(struct lws* wsi, enum lws_callback_reasons reason, void* user, void* in, size_t len) {
    switch (reason) {
        case LWS_CALLBACK_CLIENT_ESTABLISHED: {
            std::cout << "\033[1;32m[WS] Connected. Subscribing to trades...\033[0m" << std::endl;
            json sub = {{"op", "subscribe"}, {"args", json::array()}};
            for (const auto& inst : g_cfg.instruments) {
                sub["args"].push_back({{"channel", "trades"}, {"instId", inst}});
                std::cout << "[WS] Subscribing to: " << inst << std::endl;
            }
            std::string msg = sub.dump();
            std::vector<unsigned char> buf(LWS_PRE + msg.size() + 1);
            memcpy(&buf[LWS_PRE], msg.c_str(), msg.size());
            lws_write(wsi, &buf[LWS_PRE], msg.size(), LWS_WRITE_TEXT);
            lws_set_timer_usecs(wsi, 20 * 1000000);
            break;
        }

        case LWS_CALLBACK_CLIENT_RECEIVE: {
            std::string payload((char*)in, len);
            if (payload == "pong") return 0;
            try {
                json j = json::parse(payload);
                
                // 打印订阅确认消息
                if (j.contains("event") && j["event"] == "subscribe") {
                    std::cout << "\033[1;33m[WS] Subscription confirmed: " 
                              << j["arg"]["instId"] << "\033[0m" << std::endl;
                }
                
                if (j.contains("arg") && j["arg"]["channel"] == "trades" && j.contains("data")) {
                    std::string instId = j["arg"]["instId"];
                    size_t tradeCount = j["data"].size();
                    
                    for (const auto& trade : j["data"]) {
                        TickRecord rec{
                            instId,
                            trade["tradeId"].get<std::string>(),
                            std::stod(trade["px"].get<std::string>()),
                            std::stod(trade["sz"].get<std::string>()),
                            trade["side"].get<std::string>(),
                            std::stoull(trade["ts"].get<std::string>())
                        };
                        
                        // 打印每笔交易
                        g_logger.logTrade(instId, rec.price, rec.size, rec.side);
                        
                        {
                            std::lock_guard<std::mutex> lock(queueMutex);
                            dataQueue.push(std::move(rec));
                        }
                        
                        // 更新统计
                        {
                            std::lock_guard<std::mutex> lock(statsMutex);
                            tickCountByInstrument[instId]++;
                        }
                    }
                }
            } catch (const std::exception& e) {
                std::cerr << "[WS] Parse error: " << e.what() << std::endl;
            }
            break;
        }

        case LWS_CALLBACK_TIMER: {
            std::string ping = "ping";
            std::vector<unsigned char> buf(LWS_PRE + ping.size());
            memcpy(&buf[LWS_PRE], ping.c_str(), ping.size());
            lws_write(wsi, &buf[LWS_PRE], ping.size(), LWS_WRITE_TEXT);
            lws_set_timer_usecs(wsi, 20 * 1000000);
            break;
        }

        case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
        case LWS_CALLBACK_CLOSED: {
            std::cerr << "\033[1;31m[WS] Connection lost. Reconnecting in 5s...\033[0m" << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(5));
            break;
        }

        default:
            break;
    }
    return 0;
}

static struct lws_protocols protocols[] = {
    {"okx-trades", ws_callback, 0, 131072},
    {nullptr, nullptr, 0, 0}
};

// --- 统计线程 ---
void printStats() {
    while (!stopFlag) {
        std::this_thread::sleep_for(std::chrono::seconds(30));
        
        std::lock_guard<std::mutex> lock(statsMutex);
        if (!tickCountByInstrument.empty()) {
            std::cout << "\n\033[1;35m=== 统计报告（过去30秒）===\033[0m" << std::endl;
            size_t total = 0;
            for (const auto& [inst, count] : tickCountByInstrument) {
                std::cout << "  " << inst << ": " << count << " ticks" << std::endl;
                total += count;
            }
            std::cout << "  \033[1;33mTotal: " << total << " ticks\033[0m" << std::endl;
            std::cout << "=========================\n" << std::endl;
            
            // 重置计数器
            tickCountByInstrument.clear();
        }
    }
}

// --- 主函数 ---
int main() {
    signal(SIGINT, handleSignal);
    Logger logger;

    try {
        loadConfig("config.json");
        logger.log("Config loaded. Monitoring " + std::to_string(g_cfg.instruments.size()) + " instruments:");
        for (const auto& inst : g_cfg.instruments) {
            std::cout << "  - " << inst << std::endl;
        }
    } catch (const std::exception& e) {
        std::cerr << "Config Error: " << e.what() << std::endl;
        return 1;
    }

    ClientOptions opts;
    opts.SetHost(g_cfg.db_host).SetPort(9000)
        .SetUser(g_cfg.db_user).SetPassword(g_cfg.db_pass)
        .SetDefaultDatabase(g_cfg.db_name);
    Client client(opts);

    client.Execute(
        "CREATE TABLE IF NOT EXISTS " + g_cfg.db_table + " ("
        "inst_id String, timestamp UInt64, trade_id String, "
        "price Float64, size Float64, side String"
        ") ENGINE = MergeTree() ORDER BY (inst_id, timestamp)"
    );

    struct lws_context_creation_info info{};
    info.port = CONTEXT_PORT_NO_LISTEN;
    info.protocols = protocols;
    info.options = LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT;
    info.user = &g_cfg;

    struct lws_context* context = lws_create_context(&info);
    if (!context) {
        std::cerr << "Failed to create lws context" << std::endl;
        return 1;
    }

    struct lws_client_connect_info ccinfo{};
    ccinfo.context = context;
    ccinfo.address = "ws.okx.com";
    ccinfo.port = 8443;
    ccinfo.path = "/ws/v5/public";
    ccinfo.host = "ws.okx.com";
    ccinfo.origin = "ws.okx.com";
    ccinfo.ssl_connection = LCCSCF_USE_SSL;
    ccinfo.protocol = protocols[0].name;

    lws_client_connect_via_info(&ccinfo);

    logger.log("OKX Tick collector started. Waiting for data...");
    
    // 启动统计线程
    std::thread statsThread(printStats);

    std::vector<TickRecord> batch;
    auto last_flush = std::chrono::steady_clock::now();

    while (!stopFlag) {
        lws_service(context, 50);

        {
            std::lock_guard<std::mutex> lock(queueMutex);
            while (!dataQueue.empty()) {
                batch.push_back(std::move(dataQueue.front()));
                dataQueue.pop();
            }
        }

        auto now = std::chrono::steady_clock::now();
        if (!batch.empty() &&
            (batch.size() >= g_cfg.max_batch_size ||
             std::chrono::duration_cast<std::chrono::seconds>(now - last_flush).count() >= g_cfg.flush_interval)) {
            insertBatch(client, batch, logger);
            batch.clear();
            last_flush = now;
        }
    }

    if (!batch.empty()) insertBatch(client, batch, logger);
    
    statsThread.join();
    lws_context_destroy(context);
    logger.log("Collector stopped.");
    return 0;
}

