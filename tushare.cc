#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <cstdlib>
#include <thread>
#include <chrono>
#include <curl/curl.h>
#include <nlohmann/json.hpp>
#include <clickhouse/client.h>

using json = nlohmann::json;
using namespace clickhouse;

class TushareSyncer {
public:
    TushareSyncer() {
        token = load_token_from_home();
        load_ch_config();

        ClientOptions options;
        options.SetHost(ch_config["host"])
               .SetUser(ch_config["user"])
               .SetPassword(ch_config["password"])
               .SetDefaultDatabase(ch_config["database"]);
        
        ch_client = std::make_unique<Client>(options);
        target_table = ch_config["table"];
    }

    std::vector<std::string> fetch_stock_list() {
        std::vector<std::string> codes;
        json body = {
            {"api_name", "stock_basic"},
            {"token", token},
            {"params", {{"list_status", "L"}, {"fields", "ts_code"}}}
        };

        std::string resp = post_request(body.dump());
        auto j = json::parse(resp);

        if (j.contains("code") && j["code"].get<int>() != 0) {
            std::cerr << "[Tushare Error] " << j["msg"] << " (Code: " << j["code"] << ")" << std::endl;
            return codes;
        }

        if (j.contains("data") && j["data"].contains("items")) {
            for (auto& row : j["data"]["items"]) {
                codes.push_back(row[0].get<std::string>());
            }
        }
        return codes;
    }

    void sync_daily(const std::string& ts_code, const std::string& start, const std::string& end) {
        json body = {
            {"api_name", "daily"},
            {"token", token},
            {"params", {{"ts_code", ts_code}, {"start_date", start}, {"end_date", end}}}
        };

        std::string resp = post_request(body.dump());
        std::cout << "DEBUG [" << ts_code << "] Resp: " << resp << std::endl; 
        
        auto j = json::parse(resp);

        if (!j.contains("data") || j["data"]["items"].is_null() || j["data"]["items"].empty()) {
            std::cout << " -> No valid items in response for " << ts_code << std::endl;
            return;
        }

        auto& items = j["data"]["items"];
        Block block;

        auto c_code  = std::make_shared<ColumnString>();
        auto c_date  = std::make_shared<ColumnString>();
        auto c_open  = std::make_shared<ColumnFloat64>();
        auto c_high  = std::make_shared<ColumnFloat64>();
        auto c_low   = std::make_shared<ColumnFloat64>();
        auto c_close = std::make_shared<ColumnFloat64>();
        auto c_pre   = std::make_shared<ColumnFloat64>();
        auto c_chg   = std::make_shared<ColumnFloat64>();
        auto c_pct   = std::make_shared<ColumnFloat64>();
        auto c_vol   = std::make_shared<ColumnFloat64>();
        auto c_amt   = std::make_shared<ColumnFloat64>();
        auto c_stamp = std::make_shared<ColumnFloat64>();

        for (auto& row : items) {
            std::string date_str = row[1].get<std::string>();
            c_code->Append(row[0].get<std::string>());
            c_date->Append(date_str);
            c_open->Append(row[2].get<double>());
            c_high->Append(row[3].get<double>());
            c_low->Append(row[4].get<double>());
            c_close->Append(row[5].get<double>());
            c_pre->Append(row[6].get<double>());
            c_chg->Append(row[7].get<double>());
            c_pct->Append(row[8].get<double>());
            c_vol->Append(row[9].get<double>());
            c_amt->Append(row[10].get<double>());
            c_stamp->Append(convert_to_stamp(date_str));
        }

        block.AppendColumn("ts_code",    c_code);
        block.AppendColumn("trade_date", c_date);
        block.AppendColumn("open",       c_open);
        block.AppendColumn("high",       c_high);
        block.AppendColumn("low",        c_low);
        block.AppendColumn("close",      c_close);
        block.AppendColumn("pre_close",  c_pre);
        block.AppendColumn("change",     c_chg);
        block.AppendColumn("pct_chg",    c_pct);
        block.AppendColumn("vol",        c_vol);
        block.AppendColumn("amount",     c_amt);
        block.AppendColumn("date_stamp", c_stamp);

        try {
            ch_client->Insert(target_table, block);
            std::cout << " -> Successfully inserted " << items.size() << " rows." << std::endl;
        } catch (const std::exception& e) {
            std::cerr << " -> ClickHouse Insert Error: " << e.what() << std::endl;
        }
    }

private:
    std::string token;
    json ch_config;
    std::string target_table;
    std::unique_ptr<Client> ch_client;

    void load_ch_config() {
        std::ifstream f("config.json");
        if (!f.is_open()) throw std::runtime_error("Missing config.json");
        json j; f >> j;
        ch_config = j.at("clickhouse");
    }

    std::string load_token_from_home() {
        const char* home = std::getenv("HOME");
        std::string path = std::string(home) + "/.tushare_config.json";
        std::ifstream f(path);
        if (!f.is_open()) throw std::runtime_error("Missing ~/.tushare_config.json");
        json j; f >> j;
        return j.at("token").get<std::string>();
    }

    static size_t WriteCallback(void* contents, size_t size, size_t nmemb, void* userp) {
        ((std::string*)userp)->append((char*)contents, size * nmemb);
        return size * nmemb;
    }

    std::string post_request(const std::string& payload) {
        CURL* curl = curl_easy_init();
        std::string buffer;
        if (curl) {
            struct curl_slist* headers = curl_slist_append(nullptr, "Content-Type: application/json");
            curl_easy_setopt(curl, CURLOPT_URL, "https://api.tushare.pro");
            curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
            curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload.c_str());
            curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
            curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buffer);
            curl_easy_perform(curl);
            curl_easy_cleanup(curl);
            curl_slist_free_all(headers);
        }
        return buffer;
    }

    double convert_to_stamp(const std::string& d) {
        struct tm tm = {0};
        if (strptime(d.c_str(), "%Y%m%d", &tm) == nullptr) return 0;
        return (double)mktime(&tm);
    }
};

int main() {
    try {
        TushareSyncer syncer;
        auto stocks = syncer.fetch_stock_list();
        
        if (stocks.empty()) {
            std::cout << "Fallback to test codes..." << std::endl;
            stocks = {"000001.SZ", "600519.SH"};
        }

        for (size_t i = 0; i < stocks.size(); ++i) {
            std::cout << "[" << i + 1 << "/" << stocks.size() << "] Syncing " << stocks[i] << "..." << std::endl;
            syncer.sync_daily(stocks[i], "20240101", "20241231");
            std::this_thread::sleep_for(std::chrono::milliseconds(800));
        }
        std::cout << "Process Done." << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Main Error: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
