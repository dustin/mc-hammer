#include <stdio.h>

#include <string>
#include <cassert>
#include <cstdlib>
#include <vector>
#include <iostream>

#include <libmemcached/memcached.h>

#define NUM_ITEMS 10000
#define MAX_INCR 100
#define MAX_SIZE 8193

int keyn = 0;

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

int main(int argc, char **argv) {
    std::vector<Item> items;

    for (int i = 0; i < NUM_ITEMS; ++i) {
        char buf[32];
        snprintf(buf, sizeof(buf), "k%d", i);

        items.push_back(Item(buf));
    }

    MCHammer hammer("localhost:11211",
                    NUM_ITEMS,
                    MAX_INCR,
                    MAX_SIZE);
    hammer.hurtEm(items);
}
