/* Test client for Zero MQ
   Copyright (C) 2018-2021 Adam Leszczynski (aleszczynski@bersler.com)

This file is part of OpenLogReplicator.

OpenLogReplicator is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License as published
by the Free Software Foundation; either version 3, or (at your option)
any later version.

OpenLogReplicator is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License
along with OpenLogReplicator; see the file LICENSE;  If not see
<http://www.gnu.org/licenses/>.  */

#include <atomic>
#include "rapidjson/document.h"
#include <condition_variable>


#include "rapidjson/document.h"
#include "rapidjson/prettywriter.h" 

#include "OraProtoBuf.pb.h"
#include "NetworkException.h"
#include "RuntimeException.h"
#include "StreamNetwork.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <mutex>
#include <sstream>
#include <thread>

#include <vector>
#include <map>

#include "sys/sysinfo.h"
#include <sys/file.h>
#include <sys/stat.h>
#include "unistd.h"

#include <occi.h>



#ifdef LINK_LIBRARY_ZEROMQ
#include "StreamZeroMQ.h"
#endif /* LINK_LIBRARY_ZEROMQ */

#define POLL_INTERVAL           10000



std::mutex mutex_1;
std::ofstream outfile("./test.sql", std::ios::app);


std::mutex mutex1;
std::ofstream outfile1("./unexcute.sql", std::ios::app);

std::mutex mutex2;
std::ofstream outfile2("./error.sql", std::ios::app);



using namespace oracle::occi;

using namespace std;
using namespace OpenLogReplicator;

Environment *env;
Connection *conn;
Statement *gstmt;


static std::string g_BigJsonFile;
static std::string g_ZeroMqUrl;
static std::string g_DataBase;

static std::string g_WriteOracleSys;
static std::string g_WriteOralceName;
static std::string g_WriteOralcePassword;
static int g_StartScn = -1;
static int g_CommitSqlCount = -1;






static const int kItemRepositorySize = 30000; // 读写缓冲区大小
//static  int kCommitCount=10;//数据库提交事务大小



void send(pb::RedoRequest &request, OpenLogReplicator::Stream *stream_);
void receive(pb::RedoResponse &response, OpenLogReplicator::Stream *stream_);


struct ItemRepository
{
	std::string item_buffer[kItemRepositorySize];//读写缓冲区
	size_t read_position;//读index
	size_t write_position;//写index
	std::mutex mtx;//读写缓冲区锁

	size_t item_sql_counter;//写数据库条数
	std::mutex item_sql_counter_mtx;//写数据库锁
	std::condition_variable repo_not_full;
	std::condition_variable repo_not_empty;
} gItemRepository;

typedef struct ItemRepository ItemRepository;


void InitItemRepository(ItemRepository *ir)
{
	ir->write_position = 0;
	ir->read_position = 0;
	ir->item_sql_counter = 0;
}


void ParSeJsonAndWriteOracle(std::string json,Statement *stmt)
{
	rapidjson::Document doc;
	doc.Parse(json.c_str());
	if (doc.HasParseError())
	{
		printf("parse失败:%d\n", doc.GetParseError());
	}
	else
	{
		if (doc.HasMember("scn") &&!doc["scn"].IsNull())
		{
			if(doc["scn"].GetUint64() <g_StartScn)
				return;
		}
		
		rapidjson::Value& infoArray = doc["payload"];
		if (infoArray.IsArray()) {
			
			for (int i = 0; i < infoArray.Size(); i++) {
				const rapidjson::Value& object = infoArray[i];
				std::string op = object["op"].GetString();

				if (op != "begin" && op != "commit")
				{
					if (op == "c") //插入操作
					{

						std::string strSql = "INSERT INTO ";
						std::string strCloum = "(";
						std::string strValues = " VALUES( ";
						const rapidjson::Value& schema = object["schema"];
						if (schema["table"].IsString())
							strSql += schema["table"].GetString();
						//writer.String("sql");
						const rapidjson::Value& after = object["after"];
						for (rapidjson::Value::ConstMemberIterator itr = after.MemberBegin(); itr != after.MemberEnd(); itr++)
						{
							strCloum.append(itr->name.GetString());

							if (itr->value.IsInt())
							{
								strValues.append(std::to_string(itr->value.GetInt64()));
							}
							else if (itr->value.IsString())
							{
								strValues.append("'");
								strValues.append(itr->value.GetString());
								strValues.append("'");
							}

							if (itr < after.MemberEnd() - 1)
							{
								strCloum.append(",");
								strValues.append(",");
							}

						}

						strCloum.append(") ");
						strValues.append(") ");
						strSql.append(strCloum).append(strValues);
						{
							try 
							{
								
							     int iRet  = stmt->executeUpdate(strSql);

								 std::unique_lock<std::mutex> lock(gItemRepository.item_sql_counter_mtx);
								 ++(gItemRepository.item_sql_counter);
								 lock.unlock();
								 if(gItemRepository.item_sql_counter >= g_CommitSqlCount/*kCommitCount*/)
								 {
									std::unique_lock<std::mutex> lock(gItemRepository.item_sql_counter_mtx);
									gItemRepository.item_sql_counter =0;
									lock.unlock();
									conn->commit();	
								 }
								 
								 //std::lock_guard<std::mutex> lock(mutex_1);
								 //outfile << strSql << std::endl;
								 
							} 
							catch (SQLException ex) 
							{
							    //std::cout << "Error Number : "<< ex.getErrorCode() << std::endl; //取出异常代码
							    //std::cout << ex.getMessage() << std::endl; //取出异常信息
							    std::lock_guard<std::mutex> lock(mutex2);
								outfile2 << strSql << std::endl;
							}
						}		

					}
					else if (op == "u") //更新操作
					{
						std::string strSql = "UPDATE ";
						const rapidjson::Value& schema = object["schema"];
						if (schema["table"].IsString())
							strSql += schema["table"].GetString();

						strSql += " SET ";
						std::map<std::string, std::string> mapBefore;
						std::map<std::string, std::string> mapAfter;
						std::map<std::string, std::string> mapSet;
						std::map<std::string, std::string> mapWhere;

						const rapidjson::Value& before = object["before"];
						for (rapidjson::Value::ConstMemberIterator itr = before.MemberBegin(); itr != before.MemberEnd(); itr++)
						{
							std::string name = itr->name.GetString();
							std::string value;
							if (itr->value.IsInt64())
							{
								value = std::to_string(itr->value.GetInt64());
							}
							else if (itr->value.IsString())
							{
								value = "'";
								value.append(itr->value.GetString());
								value.append("'");
							}
							mapBefore.insert(std::pair<std::string, std::string>(name, value));
						}

						const rapidjson::Value& after = object["after"];
						for (rapidjson::Value::ConstMemberIterator itr = after.MemberBegin(); itr != after.MemberEnd(); itr++)
						{
							std::string name = itr->name.GetString();
							std::string value;
							if (itr->value.IsInt64())
							{
								value = std::to_string(itr->value.GetInt64());
							}
							else if (itr->value.IsString())
							{
								value = "'";
								value.append(itr->value.GetString());
								value.append("'");
							}
							mapAfter.insert(std::pair<std::string, std::string>(name, value));
						}

						std::map<std::string, std::string>::iterator  iterBefore;
						std::map<std::string, std::string>::iterator  iterAfter;
						for (iterBefore = mapBefore.begin(); iterBefore != mapBefore.end(); iterBefore++)
						{
							for (iterAfter = mapAfter.begin(); iterAfter != mapAfter.end(); iterAfter++)
							{
								if (iterBefore->first == iterAfter->first && iterBefore->second != iterAfter->second)
								{
									mapSet.insert(std::pair<std::string, std::string>(iterAfter->first, iterAfter->second));
								}
								else if (iterBefore->first == iterAfter->first && iterBefore->second == iterAfter->second)
								{
									mapWhere.insert(std::pair<std::string, std::string>(iterAfter->first, iterAfter->second));
								}
							}
							
						}

						std::map<std::string, std::string>::iterator  iterSet;
						for (iterSet = mapSet.begin(); iterSet != mapSet.end(); iterSet++)
						{
							strSql += iterSet->first;
							strSql += "=";
							strSql += iterSet->second;
							if (iterSet != --mapSet.end())
							{
								strSql.append(",");

							}
						}

						if(mapSet.size()==0)
							return;

						std::map<std::string, std::string>::iterator  iterWhere;
						if(mapWhere.size()>0)
							strSql.append(" WHERE ");
						for (iterWhere = mapWhere.begin(); iterWhere != mapWhere.end(); iterWhere++)
						{
							strSql += iterWhere->first;
							strSql += "=";
							strSql += iterWhere->second;
							if (iterWhere != --mapWhere.end())
							{
								strSql.append(" AND ");

							}
						}

						{
						
							if(strSql.find("WHERE") != strSql.npos)
							{
								try 
								{
									
								     int iRet  = stmt->executeUpdate(strSql);

									 std::unique_lock<std::mutex> lock(gItemRepository.item_sql_counter_mtx);
									 ++(gItemRepository.item_sql_counter);
									 lock.unlock();
									 if(gItemRepository.item_sql_counter >= g_CommitSqlCount)
									 {
										std::unique_lock<std::mutex> lock(gItemRepository.item_sql_counter_mtx);
										gItemRepository.item_sql_counter =0;
										lock.unlock();
										conn->commit();	
									 }
									 
									 //std::lock_guard<std::mutex> lock(mutex_1);
								 	 //outfile << strSql << std::endl;
								 
								} 
								catch (SQLException ex) 
								{
								    //std::cout << "Error Number : "<< ex.getErrorCode() << std::endl; //取出异常代码
								    //std::cout << ex.getMessage() << std::endl; //取出异常信息
								    std::lock_guard<std::mutex> lock(mutex2);
									outfile2 << strSql << std::endl;
								}
							}
							else
							{
								std::lock_guard<std::mutex> lock(mutex1);
								outfile1 << strSql << std::endl;
							}
						}
						

					}
					else if (op == "d")//删除操作
					{
						std::string strSql = "DELETE FROM  ";
						const rapidjson::Value& schema = object["schema"];
						if (schema["table"].IsString())
							strSql += schema["table"].GetString();
						//writer.Key("sql");
						const rapidjson::Value& before = object["before"];
						strSql += " WHERE ";
						for (rapidjson::Value::ConstMemberIterator itr = before.MemberBegin(); itr != before.MemberEnd(); itr++)
						{
							
							strSql += itr->name.GetString();

							strSql += "=";

							if (itr->value.IsInt64())
							{
								strSql.append(std::to_string(itr->value.GetInt64()));
							}
							else if (itr->value.IsString())
							{
								strSql.append("'");
								strSql.append(itr->value.GetString());
								strSql.append("'");
							}

							if (itr < before.MemberEnd() - 1)
							{
								strSql.append(" AND ");

							}

						}
						{
							if(strSql.find("WHERE") != strSql.npos)
							{
								try 
								{
									
								     int iRet  = stmt->executeUpdate(strSql);
									 std::unique_lock<std::mutex> lock(gItemRepository.item_sql_counter_mtx);
									 ++(gItemRepository.item_sql_counter);
									 lock.unlock();
									 if(gItemRepository.item_sql_counter >= g_CommitSqlCount)
									 {
										std::unique_lock<std::mutex> lock(gItemRepository.item_sql_counter_mtx);
										gItemRepository.item_sql_counter =0;
										lock.unlock();
										conn->commit();	
									 }
									 

									 //std::lock_guard<std::mutex> lock(mutex_1);
								 	 //outfile << strSql << std::endl;
								} 
								catch (SQLException ex) 
								{
								    //std::cout << "Error Number : "<< ex.getErrorCode() << std::endl; //取出异常代码
								    //std::cout << ex.getMessage() << std::endl; //取出异常信息
								    std::lock_guard<std::mutex> lock(mutex2);
									outfile2 << strSql << std::endl;
								}
							}
							else
							{
								std::lock_guard<std::mutex> lock(mutex1);
								outfile1 << strSql << std::endl;
							}
						}
					}

				}
			}
			
		}

	}
}



void ProduceItem(ItemRepository *ir, std::string item)
{
	std::unique_lock<std::mutex> lock(ir->mtx);
	while (((ir->write_position + 1) % kItemRepositorySize) == ir->read_position)
	{
		(ir->repo_not_full).wait(lock);
	}
	(ir->item_buffer)[ir->write_position] = item;
	(ir->write_position)++;

	if (ir->write_position == kItemRepositorySize)
		ir->write_position = 0;

	(ir->repo_not_empty).notify_all();
	lock.unlock();
}

std::string ConsumeItem(ItemRepository *ir)
{
	//int data;
	std::string data;
	std::unique_lock<std::mutex> lock(ir->mtx);
	// item buffer is empty, just wait here.
	while (ir->write_position == ir->read_position)
	{
		(ir->repo_not_empty).wait(lock);
	}
	data = (ir->item_buffer)[ir->read_position];
	(ir->read_position)++;

	if (ir->read_position >= kItemRepositorySize)
		ir->read_position = 0;

	(ir->repo_not_full).notify_all();
	lock.unlock();

	

	return data;
}


void ProducerTask()
{
	cout << "OpenLogReplicator v." PACKAGE_VERSION " StreamClient (C) 2018-2021 by Adam Leszczynski (aleszczynski@bersler.com), see LICENSE file for licensing information" << endl;

    pb::RedoRequest request;
    pb::RedoResponse response;
    atomic<bool> shutdown(false);


	

    try {
		
        OpenLogReplicator::Stream *stream = nullptr;
		stream = new StreamZeroMQ(g_ZeroMqUrl.c_str(), POLL_INTERVAL);
        stream->initializeClient(&shutdown);

        request.set_code(pb::RequestCode::INFO);
        request.set_database_name(g_DataBase.c_str());
        cout << "INFO database: " << request.database_name() << endl;
        send(request, stream);
        receive(response, stream);
        cout << "- code: " << (uint64_t)response.code() << ", scn: " << response.scn() << endl;

        uint64_t scn = 0;
        if (response.code() == pb::ResponseCode::STARTED) {
            scn = response.scn();
        } else if (response.code() == pb::ResponseCode::READY) {
            request.Clear();
            request.set_code(pb::RequestCode::START);
            request.set_database_name(g_DataBase.c_str());
            if (g_StartScn > -1) {
                request.set_scn(g_StartScn);
                cout << "START scn: " << dec << request.scn() << ", database: " << request.database_name() << endl;
            } else {
                //start from now, when SCN is not given
                request.set_scn(ZERO_SCN);
                cout << "START NOW, database: " << request.database_name() << endl;
            }
            send(request, stream);
            receive(response, stream);
            cout << "- code: " << (uint64_t)response.code() << ", scn: " << response.scn() << endl;

            if (response.code() == pb::ResponseCode::STARTED || response.code() == pb::ResponseCode::ALREADY_STARTED) {
                scn = response.scn();
            } else {
                cout << "returned code: " << response.code() << endl;
                return ;
            }
        } else {
            return ;
        }

        uint64_t lastScn, prevScn = 0;
        uint64_t num = 0;

        request.Clear();
        request.set_code(pb::RequestCode::REDO);
        request.set_database_name(g_DataBase.c_str());
        cout << "REDO database: " << request.database_name() << endl;
        send(request, stream);
        receive(response, stream);
        cout << "- code: " << (uint64_t)response.code() << endl;

        //if (response.code() != pb::ResponseCode::STREAMING)
        //    return 1;

        for (;;) {
            receive(response, stream);
            //cout << "- scn: " << dec << response.scn() << ", code: " << (uint64_t) response.code() << " payload size: " << response.payload_size() << endl;
            lastScn = response.scn();
            ++num;

            //confirm every 1000 messages
            if (num > 1000 && prevScn < lastScn) {
                request.Clear();
                request.set_code(pb::RequestCode::CONFIRM);
                request.set_scn(prevScn);
                request.set_database_name(g_DataBase.c_str());
                cout << "CONFIRM scn: " << dec << request.scn() << ", database: " << request.database_name() << endl;
                send(request, stream);
                num = 0;
            }
            prevScn = lastScn;
        }

    } catch (RuntimeException &ex) {
    } catch (NetworkException &ex) {
    }
	
	{
		std::lock_guard<std::mutex> lock(mutex);
		std::cout << "生产线程" << std::this_thread::get_id()
			<< "退出" << std::endl;
	}


}



void ConsumerTask()
{
	rapidjson::StringBuffer json;
	Statement *stmt_= conn->createStatement(); //创建一个Statement对象

	
	bool ready_to_exit = false;
	while (1)
	{
		std::string item = ConsumeItem(&gItemRepository);
		//std::cout << "消费线程:" << std::this_thread::get_id() << std::endl;
		ParSeJsonAndWriteOracle(item,stmt_);
		
	}
	{
		std::lock_guard<std::mutex> lock(mutex);
		std::cout << "消费线程" << std::this_thread::get_id()
			<< "退出" << std::endl;
		conn->terminateStatement(stmt_); //终止Statement对象
	}
}







void send(pb::RedoRequest &request, OpenLogReplicator::Stream *stream_) {
    string buffer;
    bool ret = request.SerializeToString(&buffer);
    if (!ret) {
        cout << "Message serialization error" << endl;
        exit(0);
    }

    stream_->sendMessage(buffer.c_str(), buffer.length());
}



void receive(pb::RedoResponse &response, OpenLogReplicator::Stream *stream_) {
    uint8_t buffer[READ_NETWORK_BUFFER]={0};
    uint64_t length = stream_->receiveMessage(buffer, READ_NETWORK_BUFFER);

    
    //std::lock_guard<std::mutex> lock(mutex_1);
    //outfile << buffer << std::endl;

    response.Clear();
    if (!response.ParseFromArray(buffer, length)) {
		/*
		std::lock_guard<std::mutex> lock(mutex_1);
		outfile << buffer << std::endl;
		*/
		stringstream strStream;
		strStream << buffer;
		ProduceItem(&gItemRepository, strStream.str());
		//ParSeJsonAndWriteOracle(strStream.str());
		
        //exit(0);
   }
}

int main(int argc, char** argv) {

	InitItemRepository(&gItemRepository);

	

	int fid = -1;
    char *configFileBuffer = nullptr;

    try {
        struct stat fileStat;
        std::string configFileName = "StreamClient.json";
        fid = open(configFileName.c_str(), O_RDONLY);
        if (fid == -1) {
            printf("can't open file:%s\n " , configFileName.c_str());
        }

        if (flock(fid, LOCK_EX | LOCK_NB)) {
            printf("can't lock file %s%s\n" , configFileName.c_str() , ", another process may be running");
        }

        int ret = stat(configFileName.c_str(), &fileStat);
        if (ret != 0) {
            printf("can't check file size of %s\n" , configFileName.c_str());
        }
        if (fileStat.st_size == 0) {
            printf("file %s%s\n" , configFileName.c_str() , " is empty");
        }

        configFileBuffer = new char[fileStat.st_size + 1];
        if (configFileBuffer == nullptr) {
            printf("couldn't allocate \n");
        }
        if (read(fid, configFileBuffer, fileStat.st_size) != fileStat.st_size) {
            printf("can't read file%s\n" , configFileName.c_str());
        }
        configFileBuffer[fileStat.st_size] = 0;
    }catch (...){
		printf("ERROR\n");
    }

	printf("%s\n",configFileBuffer);	

	rapidjson::Document doc;
	doc.Parse(configFileBuffer);
	if (doc.HasParseError())
	{
		printf("parse失败:%d\n", doc.GetParseError());
	}
	else
	{
		if(doc.HasMember("BigJsonName") && !doc["BigJsonName"].IsNull())
		{
			g_BigJsonFile = doc["BigJsonName"].GetString();
			printf("%s\n",g_BigJsonFile.c_str());
		}

		if(doc.HasMember("ZeroMqUrl") && !doc["ZeroMqUrl"].IsNull())
		{
			g_ZeroMqUrl = doc["ZeroMqUrl"].GetString();
			printf("%s\n",g_ZeroMqUrl.c_str());
		}

		if(doc.HasMember("WriteOracleServer") && !doc["WriteOracleServer"].IsNull())
		{
			g_WriteOracleSys = doc["WriteOracleServer"].GetString();
			printf("%s\n",g_WriteOracleSys.c_str());
		}
		if(doc.HasMember("DataBase") && !doc["DataBase"].IsNull())
		{
			g_DataBase = doc["DataBase"].GetString();
			printf("%s\n",g_DataBase.c_str());
		}

		if(doc.HasMember("WriteOracleName") && !doc["WriteOracleName"].IsNull())
		{
			g_WriteOralceName = doc["WriteOracleName"].GetString();
			printf("%s\n",g_WriteOralceName.c_str());
		}

		if(doc.HasMember("WriteOraclePasswod") && !doc["WriteOraclePasswod"].IsNull())
		{
			g_WriteOralcePassword = doc["WriteOraclePasswod"].GetString();
			printf("%s\n",g_WriteOralcePassword.c_str());
		}

		if(doc.HasMember("BeginScn") && !doc["BeginScn"].IsNull())
		{
			g_StartScn = doc["BeginScn"].GetUint64();
			printf("%d\n",g_StartScn);
		}
		if(doc.HasMember("CommitSqlCount") && !doc["CommitSqlCount"].IsNull())
		{
			g_CommitSqlCount = doc["CommitSqlCount"].GetUint64();
			printf("%d\n",g_CommitSqlCount);
		}
		
	}

	env = Environment::createEnvironment(Environment::THREADED_MUTEXED); //创建一个环境变量
	conn = env->createConnection(g_WriteOralceName,g_WriteOralcePassword,g_WriteOracleSys); //创建一个数据库连接对象
	gstmt = conn->createStatement(); //创建一个Statement对象

	//---------------------测试数据库连接-----begin--------
 	Statement *stmt1 = conn->createStatement(); //创建一个Statement对象
	ResultSet *rs;

	string sql,strname;
    int isno;

	sql = " SELECT MEMBERID,MEMBERMAIL FROM MEMBER where MEMBERID = 62437040 ";  //拼接一个SQL语句
    stmt1->setSQL(sql); //设置SQL语句到Statement对象中
    try 
    {
        rs =  stmt1->executeQuery();//执行SQL语句
        while (rs->next()) 
        { //用循环，一条一条地取得查询的结果记录
            isno = rs->getInt(1);//取出第一列的值（对应为int型）
            strname=rs->getString(2); //取出第二列的值（对应为string型）
            cout << isno << "  AND  " << strname << endl;
        }
        cout << "SELECT ―― SUCCESS" << endl;
    } 
    catch (SQLException ex) 
    {
        cout << "Error Number : "<< ex.getErrorCode() << endl; //取出异常代码
        cout << ex.getMessage() << endl; //取出异常信息
    }

    conn->terminateStatement(stmt1); //终止Statement对象
    //---------------------测试数据库连接-----end------
     
	

	int nCpuNumber = 0;
#ifndef __linux__
	SYSTEM_INFO sysInfo;
	GetSystemInfo(&sysInfo);
	nCpuNumber = sysInfo.dwNumberOfProcessors;
	//printf("system cpu num is %d\n", sysInfo.dwNumberOfProcessors);
#else

	nCpuNumber = sysconf(_SC_NPROCESSORS_ONLN);
#endif
	printf("cpu size =%d\n", nCpuNumber);

	std::thread producer(ProducerTask);
	std::thread consumer1(ConsumerTask);
	std::thread consumer2(ConsumerTask);
	std::thread consumer3(ConsumerTask);
	std::thread consumer4(ConsumerTask);
	std::thread consumer5(ConsumerTask);
	std::thread consumer6(ConsumerTask);
	std::thread consumer7(ConsumerTask);
	std::thread consumer8(ConsumerTask);

	producer.join();
	consumer1.join();
	consumer2.join();
	consumer3.join();
	consumer4.join();
	consumer5.join();
	consumer6.join();
	consumer7.join();
	consumer8.join();
	
	/*
	std::vector<std::thread> threads;
	bool bRunProducer = true;
	for (int i = 0; i < nCpuNumber-1; i++)
	{
		
		std::thread consumer(ConsumerTask);
		if (bRunProducer)
		{
			bRunProducer = false;
			producer.join();
		}
		consumer.join();
	}
    */

	conn->terminateStatement(gstmt); //终止Statement对象
	env->terminateConnection(conn); //断开数据库连接
	Environment::terminateEnvironment(env); //终止环境变量

    return 0;
}
