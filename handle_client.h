#include <iostream>
#include <string>
#include <thread>
#include <mutex>
#include <boost/asio.hpp>
#include "main.h"
#include "jsonparse.h"
#include "actions.h"

using boost::asio::ip::tcp;

std::mutex db_mutex; // Мьютекс для защиты доступа к БД

void handle_client(tcp::socket socket, Schema& schema, DBMS& dbms) {
    try {
        for (;;) {
            char data[1024];
            boost::system::error_code error;

            // Чтение запроса от клиента
            size_t length = socket.read_some(boost::asio::buffer(data), error);
            if (error == boost::asio::error::eof) {
                break; // Соединение закрыто
            } else if (error) {
                throw boost::system::system_error(error);
            }

            // Обработка запроса
            std::string query(data, length);
            std::string response;

            std::lock_guard<std::mutex> lock(db_mutex); // Защита доступа к БД
            if (query.find("INSERT") != std::string::npos) {
                SQLQuery parsed_query = parse_insert_query(query);
                if (parsed_query.table_name == "-1") {
                    response = "Синтаксическая ошибка\n";
                } else {
                    dbms.insert_into_csv(schema, parsed_query.table_name, parsed_query);
                    response = "Строка добавлена\n";
                }
            } else if (query.find("DELETE") != std::string::npos) {
                SQLQuery parsed_query = parse_delete_query(query);
                if (parsed_query.table_name == "-1") {
                    response = "Синтаксическая ошибка\n";
                } else {
                    dbms.delete_from_csv(schema, parsed_query);
                    response = "Строки удалены\n";
                }
            } else if (query.find("SELECT") != std::string::npos) {
                SQLQuery parsed_query = parse_select_query(query);
                if (parsed_query.table_name == "-1") {
                    response = "Синтаксическая ошибка\n";
                } else {
                    Tables* tables = dbms.select_data(parsed_query, schema.name + "/", schema);
                    // Форматирование результата для отправки клиенту
                    std::ostringstream oss;
                    tables->print(tables);
                    response = oss.str();
                }
            } else {
                response = "Некорректный запрос\n";
            }

            // Отправка ответа клиенту
            boost::asio::write(socket, boost::asio::buffer(response), error);
        }
    } catch (std::exception& e) {
        std::cerr << "Ошибка: " << e.what() << "\n";
    }
}
