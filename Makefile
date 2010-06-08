LIBMC=/usr/local/homebrew/
CXXFLAGS=-O3 -I$(LIBMC)/include/
LDFLAGS=-L$(LIBMC)/lib/ -lmemcached

hammer: hammer.o
	$(CXX) -o hammer hammer.o $(LDFLAGS)

clean:
	-rm hammer.o hammer

hammer.o: hammer.cc