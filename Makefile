all: worker driver chunk

worker: common.o dwc.o
	gcc $^ -o $@

driver: common.o driver.o
	gcc $^ -o $@

chunk: chunk.c
	gcc -Wall -g $^ -o $@

%.o: %.c dwc.h
	gcc -Os -c -Wall -g $< -o $@
