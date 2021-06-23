
CC = mpicc
CXX = mpiCC

COPT =

CFLAGS = -Wall -std=c99 $(COPT) -O3
CXXFLAGS = -Wall -std=c++11 $(COPT)

LDFLAGS = -Wall
LDLIBS = $(LDFLAGS) -L/home/export/online1/cpc -lcheck

targets = graph-sequential graph-load-balance
commonobj = utils.o benchmark.o
objects = $(commonobj) graph-sequential.o graph-load-balance.o

.PHONY : default
default : all

.PHONY : all
all : clean $(targets)

benchmark.o : benchmark.c common.h utils.h
	$(CC) -c $(CFLAGS) $< -o $@
utils.o : utils.c common.h
	$(CC) -host -c $(CFLAGS) $< -o $@
tools.o : tools.c common.h 
	$(CC) -host -c $(CFLAGS) $< -o $@

graph-sequential.o : graph-sequential.c common.h
	$(CC) -c $(CFLAGS) $< -o $@
graph-sequential : $(commonobj) graph-sequential.o
	$(CC) -o $@ $^ $(LDLIBS)

graph-load-balance.o : graph-load-balance.c common.h tools.h 
	$(CC) -host -c $(CFLAGS) $< -o $@
slave.o:slave.c tools.h  
	sw5cc -slave -c ${CFLAGS} $< -o $@
graph-load-balance : $(commonobj) tools.o slave.o graph-load-balance.o 
	$(CC) -hybrid -o $@ $^ $(LDLIBS)

.PHONY: clean
clean:
	rm -rf $(targets) $(objects) *.o
	
