#include "key_seq_executor.h"
#include <atomic>

class Worker
{
public:
	Worker(const std::string& name)
		: m_name {name},
		  m_wrkTimer {m_ios, boost::posix_time::milliseconds(1)},
		  m_thread{std::bind(&Worker::run, this)},
          m_stopFlag{false},
          m_pendingJobCount{0},
          m_jobCount{0},
          m_processedJobs{0}
	{
		//std::cout << "Worker " << name << " is created" << std::endl;
	}

	~Worker()
	{
		//std::cout << "Worker " << m_name << " is destroyed" << std::endl;
	}

	void waitForCompletion()
	{
        m_stopFlag = true;
		m_thread.join();
        m_ios.stop();
	}

	void submit(Job_t job)
	{
        ++m_pendingJobCount;
        ++m_jobCount;
		m_ios.post([job, this](){
            job();
            ++m_processedJobs;
            --m_pendingJobCount;
        });
	}

private:
	void heartbeat(const boost::system::error_code& /*e*/)
	{
        bool stop = m_stopFlag && m_pendingJobCount.load() == 0;
        if(!stop) {
		    m_wrkTimer.expires_from_now(boost::posix_time::milliseconds(1));
		    m_wrkTimer.async_wait(boost::bind(&Worker::heartbeat, this, boost::asio::placeholders::error));
        }
        else
        {
#if DEBUG
            std::stringstream ss;
            ss << "Worker " << m_name << " completed " << std::this_thread::get_id() << ": " << m_jobCount << " : processed : " << m_processedJobs << std::endl;
            std::cout << ss.str();
            std::cout.flush();
#endif
        }
	}

	void run()
	{
		//std::cout << "Worker " << m_name << " started in thread " << std::this_thread::get_id() << std::endl;
		m_wrkTimer.async_wait(boost::bind(&Worker::heartbeat, this, boost::asio::placeholders::error));
		m_ios.run();
        m_ios.stop();
		//std::cout << "Worker " << m_name << " stopped in thread " << std::this_thread::get_id() << std::endl;
	}

	boost::asio::io_service     m_ios;
	std::string                 m_name;
	boost::asio::deadline_timer m_wrkTimer;
	std::thread                 m_thread;
    bool                        m_stopFlag;
    std::atomic<unsigned long>  m_pendingJobCount;
    std::atomic<unsigned long>  m_jobCount;
    unsigned long               m_processedJobs;
};

KeySequentialExecutor::KeySequentialExecutor(size_t numThreads)
		: m_name {std::string("KeySequentialExecutor") + "-" +
                  std::to_string(reinterpret_cast<unsigned long>(this))},
		  m_wrkTimer {m_ios, boost::posix_time::milliseconds(1)},
          m_numThreads{numThreads},
          m_stopExecutor{false},
          m_latch{1}
{
    //std::cout << "KeySequentialExecutor " << m_name << " is created" << std::endl;
    start();
}

KeySequentialExecutor::~KeySequentialExecutor() 
{
    //std::cout << "~KeySequentialExecutor " << m_name << " is destroyed" << std::endl;
    stop();
}

void KeySequentialExecutor::start()
{
    //std::cout << "KeySequentialExecutor " << m_name << " starting ..." << std::endl;
    m_thread.reset(new std::thread(std::bind(&KeySequentialExecutor::run, this)));
}

void KeySequentialExecutor::stop()
{
    //std::cout << "KeySequentialExecutor " << m_name << " stopping ..." << std::endl;
    m_stopExecutor = true;
    m_latch.wait();
    m_thread->join();
}

void KeySequentialExecutor::execute(const std::string& key, Job_t job) 
{
    m_ios.post(boost::bind(&KeySequentialExecutor::execute_, this, key, job));
}

void KeySequentialExecutor::run()
{
    //std::cout << "\"" << std::this_thread::get_id() << "\": Starting executor ..." << std::endl;
    startWorkers();
    m_wrkTimer.async_wait(boost::bind(&KeySequentialExecutor::work, this, boost::asio::placeholders::error));
    //std::cout << "\"" << std::this_thread::get_id() << "\": Executor is up and running ..." << std::endl;
    m_ios.run();
    stopWorkers();
    m_ios.stop();
    m_latch.count_down();
    //std::cout << "\"" << std::this_thread::get_id() << "\": Executor is stopped" << std::endl;
}

void KeySequentialExecutor::execute_(const std::string& key, Job_t job) 
{
    //std::stringstream ss;
    //ss << "\"" << std::this_thread::get_id() << "\": scheduling job ..." << std::endl;
    //std::cout << ss.str();
    //std::cout.flush(); 

    boost::hash<std::string> hash;
    int32_t hashCode = static_cast<int32_t>(hash(key) & std::numeric_limits<int32_t>::max());
    int index = hashCode % m_numThreads;
    m_stats[index]++;
    m_workers[index]->submit(job);
}

void KeySequentialExecutor::work(const boost::system::error_code& /*e*/)
{
    //std::cout << "Executor is breathing ... " << std::endl;
    if(!m_stopExecutor)
    {
        m_wrkTimer.expires_from_now(boost::posix_time::milliseconds(1));
        m_wrkTimer.async_wait(boost::bind(&KeySequentialExecutor::work, this, boost::asio::placeholders::error));
    }
}

void KeySequentialExecutor::startWorkers()
{
    //std::cout << "Going to start workers : " << m_numThreads << std::endl;
    for(int i = 0; i < m_numThreads; ++i)
    {
        m_workers.push_back(std::make_shared<Worker>(std::to_string(i)));
        m_stats.insert({i, 0});
    }
}

void KeySequentialExecutor::stopWorkers()
{
    //std::cout << "Going to stop workers : " << m_numThreads << std::endl;
    for(int i = 0; i < m_numThreads; ++i)
    {
        m_workers[i]->waitForCompletion();
    }

#if DEBUG
    std::stringstream ss;
    for(auto en: m_stats) {
        ss << "Worker " << en.first << " assigned : " << en.second << std::endl;
    }
    std::cout << ss.str();
    std::cout.flush();
#endif

    m_workers.clear();
}
