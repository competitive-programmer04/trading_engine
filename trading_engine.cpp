#include <iostream>
#include <map>
#include <vector>
#include <string>
#include <algorithm>
#include <chrono>
#include <nlohmann/json.hpp>

// WebSocket++ includes
#include <websocketpp/config/asio_no_tls.hpp>
#include <websocketpp/server.hpp>

using json = nlohmann::json;
typedef websocketpp::server<websocketpp::config::asio> server;
using websocketpp::connection_hdl;

// The Order Struct
struct Order {
    std::string order_id;
    std::string bot_id;
    std::string symbol;
    std::string action;
    double price;
    int quantity;
    long long timestamp;
};

// The Orderbook (Separated by Symbol)
struct OrderBook {
    std::vector<Order> buys;
    std::vector<Order> sells;
};

std::map<std::string, OrderBook> market;
server ws_server;

// Helper to get current time in milliseconds
long long get_current_time_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

// Callback for incoming messages
void on_message(connection_hdl hdl, server::message_ptr msg) {
    long long now = get_current_time_ms();
    json req;

    try {
        req = json::parse(msg->get_payload());
    } catch (...) {
        return; // Ignore garbage non-JSON data
    }

    // Safely extract fields
    std::string action = req.value("action", "");
    std::transform(action.begin(), action.end(), action.begin(), ::tolower);
    
    std::string order_id = req.value("order_id", "");
    std::string bot_id = req.value("bot_id", "");
    std::string symbol = req.value("symbol", "");
    double price = req.value("price", 0.0);
    int quantity = req.value("quantity", 0);

    // ==========================================
    // 1. TRAP CHECK: Negative Testing
    // ==========================================
    if (price <= 0 || quantity <= 0) {
        if (action != "cancel") {
            json res = {
                {"event", "Invalid Request"},
                {"order_id", order_id},
                {"reason", "Negative price or quantity"},
                {"processed_at", now}
            };
            ws_server.send(hdl, res.dump(), websocketpp::frame::opcode::text);
            return;
        }
    }

    if (action == "cancel") {
        json res = {{"event", "Cancelled"}, {"order_id", order_id}, {"processed_at", now}};
        ws_server.send(hdl, res.dump(), websocketpp::frame::opcode::text);
        return;
    }

    Order incoming = {order_id, bot_id, symbol, action, price, quantity, now};
    OrderBook& book = market[symbol];
    bool matched = false;

    // ==========================================
    // 2. THE MATCHING ENGINE (Price-Time Priority)
    // ==========================================
    std::vector<Order>& oppositeSide = (action == "buy") ? book.sells : book.buys;

    // Sort the opposite side
    if (action == "buy") {
        std::sort(oppositeSide.begin(), oppositeSide.end(), [](const Order& a, const Order& b) {
            if (a.price == b.price) return a.timestamp < b.timestamp;
            return a.price < b.price; // Lowest sell price first
        });
    } else {
        std::sort(oppositeSide.begin(), oppositeSide.end(), [](const Order& a, const Order& b) {
            if (a.price == b.price) return a.timestamp < b.timestamp;
            return a.price > b.price; // Highest buy price first
        });
    }

    for (auto it = oppositeSide.begin(); it != oppositeSide.end(); ) {
        bool priceCrosses = (action == "buy" && incoming.price >= it->price) || 
                            (action == "sell" && incoming.price <= it->price);

        if (priceCrosses) {
            // ==========================================
            // 3. TRAP CHECK: Wash Trade
            // ==========================================
            if (incoming.bot_id == it->bot_id) {
                json res = {
                    {"event", "Rejected"},
                    {"reason", "Self-Trade"},
                    {"incoming_order_id", incoming.order_id},
                    {"resting_order_id", it->order_id},
                    {"bot_id", incoming.bot_id},
                    {"processed_at", now}
                };
                ws_server.send(hdl, res.dump(), websocketpp::frame::opcode::text);
                matched = true;
                break;
            }

            // ==========================================
            // 4. VALID MATCH
            // ==========================================
            double matchPrice = it->price;
            int fillQty = std::min(incoming.quantity, it->quantity);

            incoming.quantity -= fillQty;
            it->quantity -= fillQty;

            std::string eventType = (incoming.quantity > 0 || it->quantity > 0) ? "Partially Filled" : "Filled";

            json res = {
                {"event", eventType},
                {"buy_order_id", action == "buy" ? incoming.order_id : it->order_id},
                {"sell_order_id", action == "sell" ? incoming.order_id : it->order_id},
                {"match_price", matchPrice},
                {"filled_quantity", fillQty},
                {"buy_remaining", action == "buy" ? incoming.quantity : it->quantity},
                {"sell_remaining", action == "sell" ? incoming.quantity : it->quantity},
                {"processed_at", now}
            };

            ws_server.send(hdl, res.dump(), websocketpp::frame::opcode::text);
            matched = true;

            if (it->quantity == 0) {
                it = oppositeSide.erase(it); // Remove fully filled resting order
            } else {
                ++it;
            }

            if (incoming.quantity == 0) break; // Incoming fully filled
        } else {
            ++it;
        }
    }

    // ==========================================
    // 5. RESTING ORDER
    // ==========================================
    if (incoming.quantity > 0 && !matched) {
        json res = {{"event", "Acknowledged"}, {"order_id", incoming.order_id}, {"processed_at", now}};
        ws_server.send(hdl, res.dump(), websocketpp::frame::opcode::text);

        if (action == "buy") book.buys.push_back(incoming);
        else book.sells.push_back(incoming);
    }
}

int main() {
    try {
        ws_server.set_access_channels(websocketpp::log::alevel::none);
        ws_server.clear_access_channels(websocketpp::log::alevel::frame_payload);
        
        ws_server.init_asio();
        ws_server.set_message_handler(&on_message);

        ws_server.listen(8080);
        ws_server.start_accept();

        std::cout << "🚀 C++ Trading Engine compiled and running on ws://0.0.0.0:8080" << std::endl;
        ws_server.run();
    } catch (websocketpp::exception const & e) {
        std::cout << "Error: " << e.what() << std::endl;
    }
    return 0;
}
