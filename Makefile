CXXFLAGS=-I/usr/local/homebrew/include/
LDFLAGS=-L/usr/local/homebrew/lib/ -lmemcached

hammer: hammer.o
	$(CXX) -o hammer hammer.o $(LDFLAGS)

hammer.o: hammer.cc