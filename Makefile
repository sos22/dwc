all: worker driver

worker: common.o dwc.o
	gcc $^ -o $@

driver: common.o driver.o
	gcc $^ -o $@

%.o: %.c dwc.h
	gcc -c -Wall -g $< -o $@
