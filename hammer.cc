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

char bigassbuffer[MAX_SIZE];

memcached_st *memc;
memcached_server_st *servers;

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
    size_t len;
};

static void send(Item &i) {
    memcached_return rc = memcached_set(memc,
                                        i.key.c_str(), i.key.length(),
                                        bigassbuffer, i.len,
                                        0, 0);
    if (rc != MEMCACHED_SUCCESS) {
        std::cerr << "Error setting " << i.key << ": "
                  << memcached_strerror(memc, rc) << std::endl;
    }
}

static void process(std::vector<Item> &items) {
    while (true) {
        std::vector<Item>::iterator it;
        for (it = items.begin(); it != items.end(); ++it) {
            Item &i = *it;

            send(i);

            i.incrementSize();
        }
    }
}

int main(int argc, char **argv) {
    std::vector<Item> items;

    memc= memcached_create(NULL);
    assert(memc);

    servers= memcached_servers_parse(argv[--argc]);
    memcached_server_push(memc, servers);
    memcached_server_list_free(servers);

    for (int i = 0; i < sizeof(bigassbuffer); ++i) {
        bigassbuffer[i] = 0xff & rand();
    }

    for (int i = 0; i < NUM_ITEMS; ++i) {
        char buf[32];
        snprintf(buf, sizeof(buf), "k%d", i);

        items.push_back(Item(buf));
    }

    process(items);
}
