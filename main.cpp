#include "handle_client.h"


int main() {
    Schema schema;
    DBMS dbms;
    setConfig(schema);

    try {
        boost::asio::io_context io_context;
        tcp::acceptor acceptor(io_context, tcp::endpoint(tcp::v4(), 7432));

        std::cout << "Сервер запущен. Ожидание подключений на порту 7432..." << std::endl;

        for (;;) {
            tcp::socket socket(io_context);
            acceptor.accept(socket);
            std::thread(handle_client, std::move(socket), std::ref(schema), std::ref(dbms)).detach();
        }
    } catch (std::exception& e) {
        std::cerr << "Ошибка: " << e.what() << "\n";
    }

    return 0;
}