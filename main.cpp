#include <iostream>
#include <thread>
#include <functional>
#include <memory>
#include <random>
#include <string>
#include <chrono>
#include <cstdlib>

#include "key_seq_executor.h"
#include <boost/asio/thread_pool.hpp>
#include <atomic>

using namespace std;
using namespace chrono;

const unsigned int COUNT = 10'000'000;
const unsigned int REPS = 1000;
 
int main()
{
	srand(0);
    std::atomic<unsigned long> count = 0;
	auto start = high_resolution_clock::now();
	{
		KeySequentialExecutor tp(10);
 		for(int i = 0; i < COUNT; ++i)
			tp.execute(std::to_string(i), [i, &count]() {
                ++count;
				int x;
				int reps = REPS + (REPS * (rand() % 5));
				for(int n = 0; n < reps; ++n)
					x = i + rand();
			});
	}
	auto end = high_resolution_clock::now();
	auto duration = duration_cast<milliseconds>(end - start);
    cout << "function called : " << count.load() << endl;
	cout << "my pool duration = " << duration.count() / 1000.f << " s" << endl;
 
	srand(0);
    count = 0;
	start = high_resolution_clock::now();
	{
        boost::asio::thread_pool tp(10);
		for(int i = 0; i < COUNT; ++i)
			boost::asio::post(tp, [i, &count]() {
                ++count;
				int x;
				int reps = REPS + (REPS * (rand() % 5));
				for(int n = 0; n < reps; ++n)
					x = i + rand();
			});
        tp.join();
	}
	end = high_resolution_clock::now();
	duration = duration_cast<milliseconds>(end - start);
    cout << "function called : " << count.load() << endl;
	cout << "thread_pool duration = " << duration.count() / 1000.f << " s" << endl;

    return 0;
}
