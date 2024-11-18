#include "main.h"
#include "jsonparse.h"
#include "actions.h"
#include <boost/asio.hpp>
#include <thread>
#include <mutex>

using boost::asio::ip::tcp;

std::mutex db_mutex;

void setConfig(Schema& schema) ;
void handle_client(tcp::socket socket, Schema schema)
{
    DBMS dmbs;
    SQLQuery parsed_query;
    string response;
    try {

        for (;;){
            char data[1024];
            boost::system::error_code error;
            size_t length = socket.read_some(boost::asio::buffer(data), error);
            if (error == boost::asio::error::eof) {
                break; // Соединение закрыто
            } else if (error) {
                throw boost::system::system_error(error);
            }
            std::string query(data,length);
        
            query.erase(std::remove(query.begin(), query.end(), '\n'), query.end());

            std::lock_guard<std::mutex> lock(db_mutex);
            
            if (query.find("INSERT") != string::npos) {
        parsed_query = parse_insert_query(query);
        if (parsed_query.table_name=="-1") {
            throw runtime_error("Синтаксическая ошибка");

        }
       dmbs.insert_into_csv(schema,parsed_query.table_name,parsed_query);
       response = "Successful insert\n" ;
    boost::asio::write(socket, boost::asio::buffer(response), error);

    }else if (query.find("DELETE") != string::npos) {
        parsed_query = parse_delete_query(query);
                if (parsed_query.table_name=="-1") {
           throw runtime_error("Синтаксическая ошибка");

        }
        dmbs.delete_from_csv(schema,parsed_query);
        response = "Successful delete\n";
    boost::asio::write(socket, boost::asio::buffer(response), error);

    } else if (query.find("SELECT") != string::npos) {
        parsed_query = parse_select_query(query);
                if (parsed_query.table_name=="-1") {
            throw runtime_error("Синтаксическая ошибка");
        }
        Tables* tables=dmbs.select_data(parsed_query,schema.name+"/",schema);
                
        response=tables->print(tables);
        // delete tables
        boost::asio::write(socket, boost::asio::buffer(response), error);

    } else {
        throw runtime_error("Invalid query");
            }
        }

    }catch(std::exception& ex){
        std::cerr << "Ex: " << ex.what()<<endl;
        response = ex.what();
        boost::asio::write(socket, boost::asio::buffer(response), error);
    }





}

//INSERT INTO таблица2 VALUES ('seervertte','tttt5')
//DELETE FROM таблица1 WHERE колонка1 = 'somedata'
int main(){
    Schema schema;
    setConfig(schema);


    try{

        boost::asio::io_context io_context;
        tcp::acceptor acceptor(io_context, tcp::endpoint(tcp::v4(), 7436));

        std::cout << "Server is running on port 7436..." << std::endl;

        while (true) {
            tcp::socket socket(io_context);
            acceptor.accept(socket);
            std::thread(handle_client, std::move(socket),schema).detach(); // Обработка клиента в отдельном потоке
        
    } 
    }catch(std::exception& ex){
    std::cerr << "Ex: " << ex.what()<<endl;
    }

    return 0;

}