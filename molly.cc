/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <stdio.h>
#include <unistd.h>
#include <signal.h>
#include <sysexits.h>
#include <pthread.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <poll.h>

#include <netinet/in.h>
#include <netdb.h>
#include <errno.h>
#include <sys/errno.h>
#include <netinet/tcp.h>

#include <limits>
#include <string>
#include <cstring>
#include <cassert>
#include <cstdlib>
#include <vector>
#include <iostream>
#include <iomanip>
#include <algorithm>

#include <memcached/protocol_binary.h>

const char *delimiter = "\t";

using namespace std;

#ifdef __sun
#include <atomic.h>
#endif

#if !defined(__GNUC__) && !defined(__sun)
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
#endif

#define NUM_ITEMS 10000
#define MAX_SIZE 8193

#define PRINT_SCHED 5

static volatile int counter = 0;
static volatile size_t total_size = 0;
static volatile bool signaled = false;

static int total_items(0);
static int num_secs(0);
static int max_seconds(std::numeric_limits<int>::max());
static int delete_percentage(0);

static int incr_counter(int by) {
#ifdef __GNUC__
    return __sync_add_and_fetch(&counter, by);
#elif defined(__sun)
    return atomic_add_int_nv((volatile uint_t*)&counter, by);
#else
    int ret;
    pthread_mutex_lock(&mutex);
    counter += by;
    ret = counter;
    pthread_mutex_unlock(&mutex);
    return ret;
#endif
}

static bool sync_bool_compare_and_swap(volatile bool *dst, bool old, bool n) {
#ifdef __GNUC__
    return __sync_bool_compare_and_swap(dst, old, n);
#elif defined(__sun)
    bool ret = *dst;
    atomic_cas_8((volatile uint8_t*)&dst, (uint8_t)old, (uint8_t)n);
    return dst;
#else

    pthread_mutex_lock(&mutex);
    bool ret = *dst;
    if (*dst == old) {
       *dst = n;
    }
    pthread_mutex_unlock(&mutex);
    return ret;
#endif
}

extern "C" {
    static void signal_handler(int) {
        sync_bool_compare_and_swap(&signaled, false, true);
    }
}

static uint32_t OPAQUE(3);

class Item {
public:
    Item(const char *k, uint16_t vbucket, size_t datalen) : key(k), exists(false) {
        memset(&packet, 0, sizeof(packet));
        packet.pkt_base.message.header.request.magic = PROTOCOL_BINARY_REQ;
        packet.pkt_base.message.header.request.opcode = PROTOCOL_BINARY_CMD_SETQ;
        packet.pkt_base.message.header.request.keylen = htons(key.size());
        packet.pkt_base.message.header.request.extlen = 8; // MAGIC
        packet.pkt_base.message.header.request.vbucket = htons(vbucket);
        packet.pkt_base.message.header.request.opaque = htonl(++OPAQUE);

        packet.pkt_set.message.body.flags = htonl(918448);
        packet.pkt_set.message.body.expiration = 0;

        computeBodyLen(datalen);
    }

    void computeBodyLen(size_t datalen) {
        packet.pkt_base.message.header.request.bodylen = htonl(datalen + key.size() +
                                                               packet.pkt_base.message.header.request.extlen);
    }

    union {
        protocol_binary_request_no_extras pkt_base;
        protocol_binary_request_set pkt_set;
        protocol_binary_request_delete pkt_del;
    } packet;
    std::string key;
    bool exists;
};

class Molly {
public:

    Molly(const char *servername, const char *svc,
                 size_t msize,
                 std::vector<Item*> is) : max_size(msize),
                                          bigassbuffer(NULL),
                                          items(is) {

        _connect(servername, svc);

        bigassbuffer = static_cast<char *>(malloc(sizeof(char) * max_size));
        assert(bigassbuffer);

        for (unsigned long i = 0; i < (sizeof(char) * max_size); ++i) {
            bigassbuffer[i] = 0xff & rand();
        }
    }

    ~Molly() {
        std::vector<Item*>::iterator it;
        for (it = items.begin(); it != items.end(); ++it) {
            delete *it;
        }
        free(bigassbuffer);
        close(pollfd.fd);
    }

    void flog() {
        while (true) {
            std::random_shuffle(items.begin(), items.end());
            std::vector<Item*>::iterator it;
            int x = 0;
            for (it = items.begin(); it != items.end(); ++it, ++x) {
                Item *i = *it;

                send(i);
                if (x % 1000 == 0) {
                    incr_counter(1000);
                    maybeReport();
                }
            }
        }
    }

private:

    void readcomplaints() {
        protocol_binary_response_header response;
        while (true) {
            // If we get a status, we're pretty sure it's an error, so
            // let's go back into blocking mode, read it all out and
            // exit.
            int fflags = fcntl(pollfd.fd, F_GETFL);
            if(fcntl(pollfd.fd, F_SETFL, fflags & ~O_NONBLOCK) < 0) {
                perror("fcntl");
            }

            ssize_t r = read(pollfd.fd, &response, sizeof(response));
            assert(r == sizeof(response));
            char buf[1024];

            r = read(pollfd.fd, buf, response.response.extlen);
            assert(r == response.response.extlen);

            r = read(pollfd.fd, buf, ntohs(response.response.keylen));

            r = read(pollfd.fd, buf, ntohl(response.response.bodylen));

            buf[r] = '\0';

            std::cout << "Error " << (0xff && ntohs(response.response.status))
                      << ":  " << buf << std::endl;
            exit(1);
        }
    }

    void maybeReport(void) {
        if(sync_bool_compare_and_swap(&signaled, true, false)) {
            report();
        }
    }

    void report() {
        int oldval = incr_counter(0);

        incr_counter(0 - oldval);

        double persec = (double)oldval / (double)PRINT_SCHED;
        time_t t = time(NULL);

        num_secs += PRINT_SCHED;

        std::cout << std::setw(2) << num_secs << delimiter
                  << persec << delimiter
                  << ctime(&t)
                  << std::flush;

        if (num_secs >= max_seconds) {
            exit(0);
        }

        alarm(PRINT_SCHED);
    }

    void send(Item *i) {
        const int N_IOV(3);
        struct iovec iov[N_IOV];
        long r(random() % 100);

        i->packet.pkt_base.message.header.request.opcode = (r < delete_percentage
                                                            ? PROTOCOL_BINARY_CMD_DELETEQ
                                                            : PROTOCOL_BINARY_CMD_SETQ);
        // but let's make sure we think it's there.
        if (!i->exists) {
            i->packet.pkt_base.message.header.request.opcode = PROTOCOL_BINARY_CMD_SETQ;
        }

        iov[1].iov_base = const_cast<char*>(i->key.data());
        iov[1].iov_len = i->key.size();
        switch (i->packet.pkt_base.message.header.request.opcode) {
        case PROTOCOL_BINARY_CMD_SETQ:
            i->packet.pkt_base.message.header.request.extlen = 8;
            iov[0].iov_base = reinterpret_cast<char*>(i->packet.pkt_base.bytes);
            iov[0].iov_len = sizeof(i->packet.pkt_set);
            iov[2].iov_base = bigassbuffer;
            iov[2].iov_len = max_size;
            break;
        case PROTOCOL_BINARY_CMD_DELETEQ:
            i->packet.pkt_base.message.header.request.extlen = 0;
            iov[0].iov_base = reinterpret_cast<char*>(i->packet.pkt_base.bytes);
            iov[0].iov_len = sizeof(i->packet.pkt_del);
            iov[2].iov_len = 0;
            break;
        default:
            abort();
        }

        i->computeBodyLen(iov[2].iov_len);

        size_t todo(0);
        do {
            todo = 0;
            for (int ii = 0; ii < N_IOV; ++ii) {
                todo += iov[ii].iov_len;
            }
            ssize_t written = writev(pollfd.fd, iov, N_IOV);
            if (written == -1) {
                if (errno != EAGAIN) {
                    perror("writev");
                    abort();
                }
            } else {
                todo -= written;
                for (int ii = 0; ii < N_IOV && written > 0; ++ii) {
                    size_t from_this = std::min(static_cast<size_t>(written), iov[ii].iov_len);
                    iov[ii].iov_len -= from_this;
                    written -= from_this;
                }
            }

            if (todo > 0) {
                if (poll(&pollfd, 1, -1) == 1) {
                    if ((pollfd.revents & (POLLIN | POLLERR)) != 0) {
                        readcomplaints();
                    }
                }
            }
        } while(todo > 0);

        i->exists = i->packet.pkt_base.message.header.request.opcode != PROTOCOL_BINARY_CMD_DELETEQ;
    }

    void _connect(const char *host, const char *svc) {
        struct addrinfo hints, *res, *res0;
        struct linger l;
        bool connected(false);
        int err(0);
        int s(0);

        if (host == NULL || svc == NULL) {
            abort();
        }

        memset(&hints, 0, sizeof(hints));
        hints.ai_family = PF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        err=getaddrinfo(host, svc, &hints, &res0);
        if(err != 0) {
            fprintf(stderr, "Error looking up %s:%s:  %s\n",
                    host, svc, gai_strerror(err));
            return;
        }

        for (res = res0; res; res = res->ai_next) {

            if ((s = socket(res->ai_family, res->ai_socktype,
                            res->ai_protocol)) < 0) {

                continue;
            }

            l.l_onoff = 1;
            l.l_linger = 60;
            setsockopt(s, SOL_SOCKET, SO_LINGER, (char *) &l, sizeof(l));

            if (connect(s, res->ai_addr, res->ai_addrlen) >= 0) {
                connected = true;
                break;
            }

        }
        freeaddrinfo(res0);

        if (!connected) {
            std::cerr << "Error connecting" << std::endl;
            exit(1);
        }

        /* Configure non-blocking IO */
        int fflags = fcntl(s, F_GETFL);
        if(fcntl(s, F_SETFL, fflags | O_NONBLOCK) < 0) {
            perror("fcntl");
        }

        pollfd.fd = s;
        pollfd.events = (POLLIN|POLLOUT);
        pollfd.revents = 0;
    }

    size_t              max_size;
    char               *bigassbuffer;
    std::vector<Item*>  items;
    struct pollfd       pollfd;
};

class ItemGenerator {
public:

    ItemGenerator(int msize, int vb) : maxSize(msize), vbuckets(vb), n(0) {}

    Item *operator()(void) {
        char buf[32];
        snprintf(buf, sizeof(buf), "k%d", n++);

        uint16_t vbucket(rand() % vbuckets);

        return new Item(buf, vbucket, maxSize);
    }

private:
    int maxSize;
    int vbuckets;
    int n;
    ItemGenerator(const ItemGenerator&);
    void operator=(const ItemGenerator&);
};

extern "C" {
    static void* launch_thread(void* arg) {
        Molly *molly = static_cast<Molly*>(arg);
        molly->flog();
        return NULL;
    }
}

static void usage(const char *name) {
    std::cerr << "Usage:  " << name
              << " [-n num_items] [-V num_vbuckets] [-s max_size] [-D delete_percent]"
              << " [-T max_seconds] [-t threads] [-d delimiter] server_list"
              << std::endl;
    exit(EX_USAGE);
}

int main(int argc, char **argv) {
    int numThreads(1), numItems(NUM_ITEMS), maxSize(MAX_SIZE), numVbuckets(1), ch(0);
    const char *port("11211");

    while ((ch = getopt(argc, argv, "t:n:s:p:T:V:D:d:")) != -1) {
        switch(ch) {
        case 'n':
            numItems = atoi(optarg);
            break;
        case 'V':
            numVbuckets = atoi(optarg);
            break;
        case 's':
            maxSize = atoi(optarg);
            break;
        case 't':
            numThreads = atoi(optarg);
            break;
        case 'T':
            max_seconds = atoi(optarg);
            break;
        case 'p':
            port = optarg;
            break;
        case 'D':
            delete_percentage = atoi(optarg);
            break;
        case 'd':
            delimiter = optarg;
            break;
        default:
            usage(argv[0]);
            break;
        }
    }

    if (argc - optind != 1) {
        usage(argv[0]);
    }

    char *server_name = argv[optind];

    pthread_t *threads = new pthread_t[numThreads];

    ItemGenerator generator(maxSize, numVbuckets);

    int itemsEach = numItems / numThreads;
    for (int nt = 0; nt < numThreads; ++nt) {
        std::vector<Item*> items;
        items.reserve(itemsEach);
        for (int i = 0; i < itemsEach; i++) {
            items.push_back(generator());
        }
        total_items += itemsEach;

        Molly *molly = new Molly(server_name, port, maxSize, items);

        pthread_create(&threads[nt], NULL, launch_thread, molly);
    }

    sigset(SIGALRM, signal_handler);
    alarm(PRINT_SCHED);

    std::cout << "# threads=" << numThreads
              << ", total items = " << total_items
              << ", max size = " << maxSize
              << std::endl;

    for (int nt = 0; nt < numThreads; ++nt) {
        pthread_join(threads[nt], NULL);
    }

    return 0;
}
