/* Driver process */
#include <sys/types.h>
#include <sys/fcntl.h>
#include <sys/sendfile.h>
#include <sys/stat.h>
#include <arpa/inet.h>
#include <assert.h>
#include <err.h>
#include <poll.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "dwc.h"

#define DBG(fmt, ...) fprintf(stderr, fmt, ## __VA_ARGS__ )
//#define DBG(fmt, ...) do {} while (0)

static void
connect_to_worker(const char *ip, const char *to_worker_port,
		  const char *from_worker_port, int *to_worker_fd,
		  int *from_worker_fd)
{
	struct sockaddr_in sin;

	memset(&sin, 0, sizeof(sin));
	sin.sin_family = AF_INET;
	inet_aton(ip, &sin.sin_addr);
	*to_worker_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (*to_worker_fd < 0)
		err(1, "sock()");
	sin.sin_port = htons(atoi(to_worker_port));
	if (connect(*to_worker_fd, (struct sockaddr *)&sin, sizeof(sin)) < 0)
		err(1, "connect to send to worker %s:%s", ip, to_worker_port);
	sin.sin_port = htons(atoi(from_worker_port));
	*from_worker_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (*from_worker_fd < 0)
		err(1, "sock2()");
	if (connect(*from_worker_fd, (struct sockaddr *)&sin, sizeof(sin)) < 0)
		err(1, "connect to receive from worker %s:%s", ip, from_worker_port);

	set_nonblock(*to_worker_fd);
	set_nonblock(*from_worker_fd);
}

struct worker {
	int to_worker_fd;
	int from_worker_fd;

	off_t send_offset;
	off_t end_of_chunk;

	/* RX machine */
#define RX_BUFFER_SIZE (1 << 20)
#define MIN_READ_SIZE (64 << 10)

	char *prefix_string;
	char *suffix_string;

	int finished;

	unsigned rx_buffer_avail;
	unsigned rx_buffer_used;
	unsigned char rx_buffer[RX_BUFFER_SIZE];
};

static char *
read_string(struct worker *w)
{
	int size;
	char *res;

	if (w->rx_buffer_used + 2 > w->rx_buffer_avail)
		return NULL;

	size = *(unsigned short *)(w->rx_buffer + w->rx_buffer_used);
	if (w->rx_buffer_used + size + 2 > w->rx_buffer_avail)
		return NULL;

	res = malloc(size + 1);
	res[size] = 0;
	memcpy(res, w->rx_buffer + w->rx_buffer_used + 2, size);

	w->rx_buffer_used += 2 + size;

	return res;
}

static void
process_split_string(char *prefix, char *suffix)
{
	int plen = strlen(prefix);
	int slen = strlen(suffix);
	int total_len = plen + slen;
	unsigned char *buf;

	buf = alloca(total_len+1);
	memcpy(buf, prefix, plen);
	memcpy(buf + plen, suffix, slen + 1);

	bump_word_counter(buf, total_len, 1);
}

static int
process_word_entry(struct worker *w)
{
	int size;
	unsigned count;

	if (w->rx_buffer_used + 6 > w->rx_buffer_avail)
		return 0;
	size = *(unsigned short *)(w->rx_buffer + w->rx_buffer_used + 4);
	if (w->rx_buffer_used + 6 + size > w->rx_buffer_avail)
		return 0;

	count = *(unsigned *)(w->rx_buffer + w->rx_buffer_used);
	bump_word_counter(w->rx_buffer + w->rx_buffer_used + 6, size, count);
	w->rx_buffer_used += 6 + size;
	return 1;
}

static void
do_rx(struct worker *w, int is_first_worker, int is_last_worker, int id)
{
	ssize_t received;

	/* Receive as much as possible. */
	if (w->from_worker_fd > 0) {
		if (RX_BUFFER_SIZE - w->rx_buffer_avail < MIN_READ_SIZE) {
			memmove(w->rx_buffer,
				w->rx_buffer + w->rx_buffer_used,
				w->rx_buffer_avail - w->rx_buffer_used);
			w->rx_buffer_avail -= w->rx_buffer_used;
			w->rx_buffer_used = 0;
		}
		received = read(w->from_worker_fd, w->rx_buffer + w->rx_buffer_avail,
				RX_BUFFER_SIZE - w->rx_buffer_avail);
		if (received == 0) {
			close(w->from_worker_fd);
			w->from_worker_fd = -1;
			DBG("Finished receiving from worker %d\n", id);
		} else if (received < 0) {
			err(1, "receiving from worker");
		} else {
			w->rx_buffer_avail += received;
		}
	}

	if (!w->prefix_string) {
		w->prefix_string = read_string(w);
		if (!w->prefix_string) {
			DBG("Worker %d hasn't provided a prefix yet\n",
			       id);
			return;
		}
		if (!is_first_worker) {
			if (w[-1].suffix_string)
				process_split_string(w[-1].suffix_string, w->prefix_string);
		} else {
			process_split_string("", w->prefix_string);
		}
	}

	if (!w->suffix_string) {
		w->suffix_string = read_string(w);
		if (!w->suffix_string) {
			DBG("Worker %d hasn't provided a suffix yet\n", id);
			return;
		}
		if (!is_last_worker) {
			if (w[1].prefix_string)
				process_split_string(w->suffix_string, w[1].prefix_string);
		} else {
			process_split_string(w->suffix_string, "");
		}
	}

	while (process_word_entry(w))
		;

	if (w->from_worker_fd == -1) {
		if (w->rx_buffer_used != w->rx_buffer_avail)
			warnx("worker %d has %d bytes left over at end",
			      id, w->rx_buffer_avail - w->rx_buffer_used);
		w->finished = 1;
	}
}

int
main(int argc, char *argv[])
{
	int fd;
	struct stat statbuf;
	off_t size;
	unsigned nr_workers;
	int workers_left_alive;
	struct pollfd *polls;
	struct worker *workers;
	unsigned x;
	int idx;
	int *poll_slots_to_workers;
	int offline;

	init_malloc();

	if (argc == 1)
		errx(1, "arguments are either --offline and a list of files, or a list of ip port1 port2 triples");

	offline = 0;
	if (!strcmp(argv[1], "--offline"))
		offline = 1;

	if (!offline) {
		if ((argc - 2) % 3)
			errx(1, "non-integer number of workers?");

		fd = open(argv[1], O_RDONLY);
		if (fd < 0)
			err(1, "open(%s)", argv[1]);
		if (fstat(fd, &statbuf) < 0)
			err(1, "stat(%s)", argv[1]);
		size = statbuf.st_size;
		nr_workers = (argc - 2) / 3;
	} else {
		fd = -1;
		size = 0;
		nr_workers = argc - 2;
	}

	workers = calloc(nr_workers, sizeof(workers[0]));
	polls = calloc(nr_workers, sizeof(polls[0]));
	poll_slots_to_workers = calloc(nr_workers, sizeof(polls[0]));
	for (x = 0; x < nr_workers; x++) {
		if (offline) {
			workers[x].to_worker_fd = -1;
			workers[x].from_worker_fd = open(argv[x + 2], O_RDONLY | O_NONBLOCK);
			if (workers[x].from_worker_fd < 0)
				err(1, "opening %s", argv[x + 2]);
			polls[x].fd = workers[x].from_worker_fd;
			polls[x].events = POLLIN;
		} else {
			connect_to_worker(argv[x * 3 + 2],
					  argv[x * 3 + 3],
					  argv[x * 3 + 4],
					  &workers[x].to_worker_fd,
					  &workers[x].from_worker_fd);

			polls[x].fd = workers[x].to_worker_fd;
			polls[x].events = POLLOUT;

			workers[x].send_offset = x * (size / nr_workers);
			if (x != 0)
				workers[x-1].end_of_chunk = workers[x].send_offset;
		}
		poll_slots_to_workers[x] = x;
	}
	workers[nr_workers - 1].end_of_chunk = size;

	workers_left_alive = nr_workers;
	while (workers_left_alive != 0) {
		int r = poll(polls, workers_left_alive, -1);
		if (r < 0)
			err(1, "poll()");
		for (x = 0; x < workers_left_alive && r; x++) {
			if (!polls[x].revents)
				continue;
			r--;
			idx = poll_slots_to_workers[x];

			assert(!(polls[x].revents & POLLNVAL));
			if (polls[x].revents & POLLERR)
				errx(1, "error on worker %d", idx);
			if (polls[x].revents & POLLHUP)
				errx(1, "worker %d hung up on us", idx);

			if (polls[x].revents & POLLOUT) {
				ssize_t s;
				assert(workers[idx].send_offset < workers[idx].end_of_chunk);
				s = sendfile(workers[idx].to_worker_fd,
					     fd,
					     &workers[idx].send_offset,
					     workers[idx].end_of_chunk - workers[idx].send_offset);
				if (s == 0)
					errx(1, "worker hung up on us");
				if (s < 0)
					err(1, "sending to worker");
				assert(workers[idx].send_offset <= workers[idx].end_of_chunk);
				if (workers[idx].send_offset == workers[idx].end_of_chunk) {
					close(workers[idx].to_worker_fd);
					workers[idx].to_worker_fd = -1;
					polls[x].events = POLLIN;
					polls[x].revents = 0;
					polls[x].fd = workers[idx].from_worker_fd;
					DBG("Finished sending input to worker %d\n",
					       idx);
				}
			} else if (polls[x].revents & POLLIN) {
				do_rx(workers + idx,
				      idx == 0,
				      idx == nr_workers - 1,
				      idx);
				if (workers[idx].finished) {
					memmove(poll_slots_to_workers + x,
						poll_slots_to_workers + x + 1,
						(workers_left_alive - x - 1) * sizeof(int));
					memmove(polls + x,
						polls + x + 1,
						(workers_left_alive - x - 1) * sizeof(polls[0]));
					workers_left_alive--;
				}
			}
		}
	}

	for (idx = 0; idx < NR_HASH_TABLE_SLOTS; idx++) {
		struct word *w;
		for (w = hash_table[idx]; w; w = w->next)
			printf("%16d %.*s\n",
			       w->counter,
			       w->len,
			       w->word);
	}

	return 0;
}
