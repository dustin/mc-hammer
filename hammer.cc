#include <stdio.h>
#include <unistd.h>
#include <signal.h>
#include <sysexits.h>
#include <pthread.h>

#include <string>
#include <cassert>
#include <cstdlib>
#include <vector>
#include <iostream>
#include <iomanip>
#include <algorithm>

#include <libmemcached/memcached.h>

#define NUM_ITEMS 10000
#define MAX_INCR 100
#define MAX_SIZE 8193

#define PRINT_SCHED 5

volatile int counter = 0;
volatile size_t total_size = 0;
volatile bool signaled = false;

int total_items = 0;

static int incr_counter(int by) {
    return __sync_add_and_fetch(&counter, by);
}

static size_t incr_total_size(int by) {
    return __sync_add_and_fetch(&total_size, by);
}

static void signal_handler(int sig) {
    __sync_bool_compare_and_swap(&signaled, false, true);
}

class Item {
public:
    Item(const char *k, int maxIncr, int maxSize) : key(k), len(0) {
        incrementSize(maxIncr, maxSize);
    }

    void incrementSize(int maxIncr, int maxSize) {
        size_t oldlen = len;
        len += (rand() % maxIncr);
        len %= maxSize;

        incr_total_size(len - oldlen);
    }

    std::string key;
    size_t      len;
};

class MCHammer {
public:

    MCHammer(const char *servers,
             size_t mincr,
             size_t msize,
             std::vector<Item*> is) : max_incr(mincr),
                                      max_size(msize),
                                      memc(NULL),
                                      bigassbuffer(NULL),
                                      items(is) {

        memc= memcached_create(NULL);
        assert(memc);

        memcached_server_st *srvrs = memcached_servers_parse(servers);
        memcached_server_push(memc, srvrs);
        memcached_server_list_free(srvrs);

        memcached_behavior_set(memc,
                               MEMCACHED_BEHAVIOR_BUFFER_REQUESTS, 1);
        memcached_behavior_set(memc,
                               MEMCACHED_BEHAVIOR_RCV_TIMEOUT,
                               1 * 1000 * 1000);
        memcached_behavior_set(memc,
                               MEMCACHED_BEHAVIOR_SND_TIMEOUT,
                               1 * 1000 * 1000);
        memcached_behavior_set(memc,
                               MEMCACHED_BEHAVIOR_POLL_TIMEOUT,
                               1 * 1000);

        bigassbuffer = static_cast<char *>(malloc(sizeof(char) * max_size));
        assert(bigassbuffer);

        for (int i = 0; i < (sizeof(char) * max_size); ++i) {
            bigassbuffer[i] = 0xff & rand();
        }

    }

    ~MCHammer() {
        std::vector<Item*>::iterator it;
        for (it = items.begin(); it != items.end(); ++it) {
            delete *it;
        }
        free(bigassbuffer);
        memcached_free(memc);
    }

    void maybeReport(void) {
        if(__sync_bool_compare_and_swap(&signaled, true, false)) {

            int oldval = incr_counter(0);
            size_t total_size = incr_total_size(0);

            incr_counter(0 - oldval);

            double persec = (double)oldval / (double)PRINT_SCHED;
            double avg_size = (double) total_size / (double)total_items;
            time_t t = time(NULL);

            std::cout << std::setw(2) << persec << "/s, avg size="
                      << avg_size << "\t" << ctime(&t)
                      << std::flush;

            alarm(PRINT_SCHED);
        }
    }

    void hurtEm(void) {
        while (true) {
            std::random_shuffle(items.begin(), items.end());
            std::vector<Item*>::iterator it;
            for (it = items.begin(); it != items.end(); ++it) {
                Item *i = *it;

                maybeReport();

                send(i);

                i->incrementSize(max_incr, max_size);
            }
        }
    }

private:

    void send(Item *i) {
        memcached_return rc = memcached_set(memc,
                                            i->key.c_str(), i->key.length(),
                                            bigassbuffer, i->len,
                                            0, 0);
        incr_counter(1);
        if (rc != MEMCACHED_SUCCESS && rc != MEMCACHED_BUFFERED) {
            std::cerr << "Error setting " << i->key << ": "
                      << memcached_strerror(memc, rc) << std::endl;
        }
    }

    size_t        max_incr;
    size_t        max_size;
    memcached_st *memc;
    char * bigassbuffer;
    std::vector<Item*> items;
};

void usage(const char *name) {
    std::cerr << "Usage:  " << name
              << " [-n num_items] [-i max_incr] [-s max_size]"
              << " [-t threads] server_list"
              << std::endl;
    exit(EX_USAGE);
}

class ItemGenerator {
public:

    ItemGenerator(int msize, int mincr) : maxSize(msize), maxIncr(mincr), n(0) {}

    Item *operator()(void) {
        char buf[32];
        snprintf(buf, sizeof(buf), "k%d", n++);

        return new Item(buf, maxSize, maxIncr);
    }

private:
    int maxSize;
    int maxIncr;
    int n;
    ItemGenerator(const ItemGenerator&);
    void operator=(const ItemGenerator&);
};

static void* launch_thread(void* arg) {
    MCHammer *hammer = static_cast<MCHammer*>(arg);
    hammer->hurtEm();
    return NULL;
}

int main(int argc, char **argv) {

    int numThreads(1), numItems(NUM_ITEMS), maxIncr(MAX_INCR),
        maxSize(MAX_SIZE), ch(0);

    const char *name = argv[0];

    while ((ch = getopt(argc, argv, "t:n:i:s:")) != -1) {
        switch(ch) {
        case 'n':
            numItems = atoi(optarg);
            break;
        case 'i':
            maxIncr = atoi(optarg);
            break;
        case 's':
            maxSize = atoi(optarg);
            break;
        case 't':
            numThreads = atoi(optarg);
            break;
        default:
            usage(name);
            break;
        }
    }
    argc -= optind;
    argv += optind;

    if (argc != 1) {
        usage(name);
    }

    char *server_list = argv[0];

    pthread_t threads[numThreads];

    ItemGenerator generator(maxSize, maxIncr);

    int itemsEach = numItems / numThreads;
    for (int nt = 0; nt < numThreads; ++nt) {
        std::vector<Item*> items;
        items.reserve(itemsEach);
        for (int i = 0; i < itemsEach; i++) {
            items.push_back(generator());
        }
        total_items += itemsEach;

        MCHammer *hammer = new MCHammer(server_list, maxIncr, maxSize, items);

        pthread_create(&threads[nt], NULL, launch_thread, hammer);
    }

    signal(SIGALRM, signal_handler);
    alarm(PRINT_SCHED);

    std::cout << "# threads=" << numThreads
              << ", total items = " << total_items
              << ", max size = " << maxSize
              << ", max incr = " << maxIncr
              << std::endl;

    for (int nt = 0; nt < numThreads; ++nt) {
        pthread_join(threads[nt], NULL);
    }
}
