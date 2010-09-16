all: worker driver

worker: common.o dwc.o
	gcc $^ -o $@

driver: common.o driver.o
	gcc $^ -o $@

%.o: %.c dwc.h
	gcc -Os -c -Wall -g $< -o $@
