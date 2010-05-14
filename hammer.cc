#include <stdio.h>
#include <unistd.h>
#include <sysexits.h>

#include <string>
#include <cassert>
#include <cstdlib>
#include <vector>
#include <iostream>
#include <algorithm>

#include <libmemcached/memcached.h>

#define NUM_ITEMS 10000
#define MAX_INCR 100
#define MAX_SIZE 8193

class Item {
public:
    Item(const char *k) : key(k), len(0) {
        incrementSize();
    }

    void incrementSize(void) {
        len += (rand() % MAX_INCR);
        len %= MAX_SIZE;
    }

    std::string key;
    size_t      len;
};

class MCHammer {
public:

    MCHammer(const char *servers,
             size_t nitems,
             size_t mincr,
             size_t msize) : num_items(nitems),
                             max_incr(mincr),
                             max_size(msize),
                             memc(NULL),
                             bigassbuffer(NULL) {

        memc= memcached_create(NULL);
        assert(memc);

        memcached_server_st *srvrs = memcached_servers_parse(servers);
        memcached_server_push(memc, srvrs);
        memcached_server_list_free(srvrs);

        bigassbuffer = static_cast<char *>(malloc(sizeof(char) * max_size));
        assert(bigassbuffer);

        for (int i = 0; i < sizeof(bigassbuffer); ++i) {
            bigassbuffer[i] = 0xff & rand();
        }

    }

    void hurtEm(std::vector<Item> &items) {
        while (true) {
            std::random_shuffle(items.begin(), items.end());
            std::vector<Item>::iterator it;
            for (it = items.begin(); it != items.end(); ++it) {
                Item &i = *it;

                send(i);

                i.incrementSize();
            }
        }
    }

private:

    void send(Item &i) {
        memcached_return rc = memcached_set(memc,
                                            i.key.c_str(), i.key.length(),
                                            bigassbuffer, i.len,
                                            0, 0);
        if (rc != MEMCACHED_SUCCESS) {
            std::cerr << "Error setting " << i.key << ": "
                      << memcached_strerror(memc, rc) << std::endl;
        }
    }

    size_t        num_items;
    size_t        max_incr;
    size_t        max_size;
    memcached_st *memc;
    char * bigassbuffer;
};

void usage(const char *name) {
    std::cerr << "Usage:  " << name
              << " [-n num_items] [-i max_incr] [-s max_size] server_list"
              << std::endl;
    exit(EX_USAGE);
}

int main(int argc, char **argv) {

    int numItems(NUM_ITEMS), maxIncr(MAX_INCR), maxSize(MAX_SIZE), ch(0);
    const char *name = argv[0];

    while ((ch = getopt(argc, argv, "n:i:s:")) != -1) {
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

    std::vector<Item> items;

    for (int i = 0; i < numItems; ++i) {
        char buf[32];
        snprintf(buf, sizeof(buf), "k%d", i);

        items.push_back(Item(buf));
    }

    MCHammer hammer(server_list, numItems, maxIncr, maxSize);
    hammer.hurtEm(items);
}
