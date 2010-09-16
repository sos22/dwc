CFLAGS = -Os -Wall -g
LDFLAGS =

all: worker driver chunk

worker: common.o dwc.o
	gcc $(LDFLAGS) $^ -o $@

driver: common.o driver.o
	gcc $(LDFLAGS) $^ -o $@

chunk: chunk.c
	gcc $(LDFLAGS) $(CFLAGS) $^ -o $@

%.o: %.c dwc.h
	gcc $(CFLAGS) -c $< -o $@

clean:
	rm -f *.o worker driver chunk
