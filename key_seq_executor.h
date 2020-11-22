#ifndef KEY_SEQ_EXEC_H
#define KEY_SEQ_EXEC_H

#include <iostream>
#include <thread>
#include <functional>
#include <memory>
#include <unordered_map>

#include <boost/thread/latch.hpp>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/functional/hash.hpp>

using Job_t = std::function<void(void)>;

class Worker;
using WorkerPtr_t = std::shared_ptr<Worker>;

class KeySequentialExecutor
{
public:
	KeySequentialExecutor(size_t numThreads);

    ~KeySequentialExecutor();

    virtual void execute(const std::string& key, Job_t job);

private:
	void run();

    void execute_(const std::string& key, Job_t job);

	void work(const boost::system::error_code& /*e*/);

    void start();

    void stop();

	void startWorkers();

	void stopWorkers();

	boost::asio::io_service     m_ios;
	const std::string           m_name;
	boost::asio::deadline_timer m_wrkTimer;
	std::vector<WorkerPtr_t>  m_workers;

    const size_t                m_numThreads;
    bool                        m_stopExecutor;

	std::unique_ptr<std::thread>    m_thread;
    boost::latch                    m_latch;

    std::unordered_map<int, unsigned long>  m_stats;
};

#endif // KEY_SEQ_EXEC_H
