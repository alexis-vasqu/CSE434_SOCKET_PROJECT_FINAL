CC=gcc
CFLAGS=-O2 -Wall -pthread

all: manager user disk

manager: manager.c
	$(CC) $(CFLAGS) -o $@ $^

user: user.c
	$(CC) $(CFLAGS) -o $@ $^

disk: disk.c
	$(CC) $(CFLAGS) -o $@ $^

clean:
	rm -f manager user disk
	

