/* Driver process */
#include <sys/types.h>
#include <sys/fcntl.h>
#include <sys/sendfile.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <arpa/inet.h>
#include <assert.h>
#include <err.h>
#include <poll.h>
#include <malloc.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "dwc.h"

/* Try to stay below 512MB */
#define TARGET_MAX_HEAP_SIZE (512 << 20)
/* Throttle fast workers if we remain above 256MB after a GC pass. */
#define THROTTLE_HEAP_SIZE (256 << 20)

/* How much of the hash table have we GC'd? */
static int last_gced_hash_slot;

static double now(void);

#define DBG(fmt, ...) fprintf(stderr, "%f: " fmt, now(), ## __VA_ARGS__ )
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

	int finished_hash_entries;

	int recent_hash_prod;
	int recent_hashes[64];

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
process_split_string(char *prefix, char *suffix, int worker1, int worker2)
{
	int plen = strlen(prefix);
	int slen = strlen(suffix);
	int total_len = plen + slen;
	unsigned char *buf;
	int idx;

	buf = alloca(total_len+1);
	memcpy(buf, prefix, plen);
	memcpy(buf + plen, suffix, slen + 1);

	idx = bump_word_counter(buf, total_len, 1);
	DBG("worker %d:%d produced split string in bucket %d\n",
	    worker1, worker2, idx);
}

static int
process_word_entry(struct worker *w, int wid)
{
	int size;
	unsigned count;
	int idx;

	if (w->rx_buffer_used + 6 > w->rx_buffer_avail)
		return 0;
	size = *(unsigned short *)(w->rx_buffer + w->rx_buffer_used + 4);
	if (w->rx_buffer_used + 6 + size > w->rx_buffer_avail)
		return 0;

	count = *(unsigned *)(w->rx_buffer + w->rx_buffer_used);
	idx = bump_word_counter(w->rx_buffer + w->rx_buffer_used + 6, size, count);
	w->rx_buffer_used += 6 + size;
	if (idx < w->finished_hash_entries + 1) {
		int s;
		DBG("worker %d went backwards through table: %d < %d\n",
		    wid, idx, w->finished_hash_entries);
		s = w->recent_hash_prod - 64;
		if (s < 0)
			s = 0;
		while (s != w->recent_hash_prod) {
			DBG("Recently produced %d\n", w->recent_hashes[s % 64]);
			s++;
		}
	}
	w->recent_hashes[w->recent_hash_prod++ % 64] = idx;
	assert(idx >= w->finished_hash_entries + 1);
	w->finished_hash_entries = idx - 1;
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
		DBG("Worker %d starts receiving data\n", id);
		if (!is_first_worker) {
			if (w[-1].suffix_string)
				process_split_string(w[-1].suffix_string, w->prefix_string,
						     id - 1, id);
		} else {
			process_split_string("", w->prefix_string, -1, id);
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
				process_split_string(w->suffix_string, w[1].prefix_string,
						     id, id + 1);
		} else {
			process_split_string(w->suffix_string, "", id, -1);
		}
	}

	while (process_word_entry(w, id))
		;

	if (w->from_worker_fd == -1) {
		if (w->rx_buffer_used != w->rx_buffer_avail)
			warnx("worker %d has %d bytes left over at end",
			      id, w->rx_buffer_avail - w->rx_buffer_used);
		w->finished = 1;
	}
}

static struct timeval start;

static double
now(void)
{
	struct timeval tv;
	gettimeofday(&tv, NULL);
	tv.tv_sec -= start.tv_sec;
	tv.tv_usec -= start.tv_usec;
	return tv.tv_sec + tv.tv_usec * 1e-6;
}

static void
compact_heap(struct worker *worker, int nr_workers, struct pollfd *polls)
{
	int x;
	int earliest_finished_slot;
	struct mallinfo mi;
	int throttle_worker_slot;
	bool some_worker_unready;

	DBG("Start hash table GC\n");

	/* Make sure that every worker has its prefix and suffix
	 * string before doing anything */
	some_worker_unready = false;
	for (x = 0; x < nr_workers; x++) {
		if (!worker[x].prefix_string || !worker[x].suffix_string) {
			DBG("Worker %d hasn't completed its boundary strings", x);
			some_worker_unready = true;
		}
	}

	if (some_worker_unready) {
		/* Okay, we're not ready for hash compaction.
		   Throttle every worker which has prefix and
		   suffix. */
		for (x = 0; x < nr_workers; x++) {
			if (worker[x].prefix_string && worker[x].suffix_string) {
				if (polls[x].events & POLLIN)
					DBG("Throttle %d for pre-compaction\n", x);
				polls[x].events &= ~POLLIN;
			}
		}
		return;
	}

	earliest_finished_slot = NR_HASH_TABLE_SLOTS;
	for (x = 0; x < nr_workers; x++) {
		DBG("worker %d has finished slot %d\n", x, worker[x].finished_hash_entries);
		if (worker[x].finished_hash_entries < earliest_finished_slot)
			earliest_finished_slot = worker[x].finished_hash_entries;
	}
	DBG("Discarding slots up to %d\n", earliest_finished_slot);
	for (x = 0; x <= earliest_finished_slot; x++) {
		struct word *w, *n;
		for (w = hash_table[x]; w; w = n) {
			n = w->next;
			printf("%16d %.*s\n",
			       w->counter,
			       w->len,
			       w->word);
			free(w);
		}
		hash_table[x] = NULL;
	}
	last_gced_hash_slot = earliest_finished_slot;
	mi = mallinfo();
	DBG("Done hash table GC; %d bytes still in use in heap\n", mi.uordblks);

	if (mi.uordblks >= THROTTLE_HEAP_SIZE) {
		throttle_worker_slot = earliest_finished_slot + 100;
		DBG("Going to throttle mode; barrier is %d\n", throttle_worker_slot);
	} else {
		throttle_worker_slot = NR_HASH_TABLE_SLOTS;
		DBG("Throttle disabled\n");
	}

	for (x = 0; x < nr_workers; x++) {
		if (worker[x].finished_hash_entries >= throttle_worker_slot) {
			if (polls[x].events & POLLIN)
				DBG("Worker %d throttles at %d\n", x,
				    worker[x].finished_hash_entries);
			polls[x].events &= ~POLLIN;
		} else if (worker[x].to_worker_fd == -1) {
			if (!(polls[x].events & POLLIN))
				DBG("worker %d unthrottled at %d\n", x,
				    worker[x].finished_hash_entries);
			polls[x].events |= POLLIN;
		} else {
			DBG("worker %d isn't ready to receive results yet\n", x);
		}
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
	int prepopulate;
	int poll_slots_in_use;
	struct mallinfo mi;

	init_malloc(false);
	gettimeofday(&start, NULL);

	if (argc == 1)
		errx(1, "arguments are either --offline and a list of files, or a list of ip port1 port2 triples");

	prepopulate = 0;
	if (!strcmp(argv[1], "--prepopulate")) {
		prepopulate = 1;
		argv++;
		argc--;
	}

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
		workers[x].finished_hash_entries = -1;
		poll_slots_to_workers[x] = x;
	}
	workers[nr_workers - 1].end_of_chunk = size;

	workers_left_alive = nr_workers;
	poll_slots_in_use = nr_workers;
	DBG("Start main loop\n");
	while (workers_left_alive != 0) {
		int r = poll(polls, poll_slots_in_use, -1);
		if (r < 0)
			err(1, "poll()");
		for (x = 0; x < poll_slots_in_use && r; x++) {
			if (!polls[x].revents)
				continue;
			r--;
			idx = poll_slots_to_workers[x];

			assert(!(polls[x].revents & POLLNVAL));
			if (polls[x].revents & POLLERR)
				errx(1, "error on worker %d", idx);
			if (polls[x].revents & POLLHUP) {
				if (polls[x].events == POLLIN) {
					polls[x].revents = POLLIN;
					warnx("worker %d hung up on us", idx);
				} else {
					errx(1, "worker %d hung up on us", idx);
				}
			}

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
					DBG("Finished sending input to worker %d\n",
					       idx);
					if (prepopulate) {
						memmove(poll_slots_to_workers + x,
							poll_slots_to_workers + x + 1,
							(poll_slots_in_use - x - 1) * sizeof(int));
						memmove(polls + x,
							polls + x + 1,
							(poll_slots_in_use - x - 1) * sizeof(polls[0]));
						poll_slots_in_use--;
						if (poll_slots_in_use == 0) {
							/* Okay, we've
							   done the
							   prepopulate
							   phase.
							   Switch
							   modes. */
							DBG("Finished prepopulate phase\n");
							for (idx = 0; idx < nr_workers; idx++) {
								polls[idx].events = POLLIN;
								polls[idx].revents = 0;
								polls[idx].fd = workers[idx].from_worker_fd;
								poll_slots_to_workers[idx] = idx;
								close(workers[idx].to_worker_fd);
								workers[idx].to_worker_fd = -1;
							}
							DBG("All workers go\n");
							poll_slots_in_use = nr_workers;
						}
					} else {
						close(workers[idx].to_worker_fd);
						workers[idx].to_worker_fd = -1;
						polls[x].events = POLLIN;
						polls[x].revents = 0;
						polls[x].fd = workers[idx].from_worker_fd;
					}
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
					poll_slots_in_use--;
					workers_left_alive--;
				}
			}
		}

		mi = mallinfo();
		if (mi.uordblks > TARGET_MAX_HEAP_SIZE)
			compact_heap(workers, nr_workers, polls);

	}

	DBG("All done\n");

	for (idx = last_gced_hash_slot + 1; idx < NR_HASH_TABLE_SLOTS; idx++) {
		struct word *w;
		for (w = hash_table[idx]; w; w = w->next) {
			printf("%16d %.*s\n",
			       w->counter,
			       w->len,
			       w->word);
		}
	}
	printf("Boundary screw ups:\n");
	for (idx = 0; idx <= last_gced_hash_slot; idx++) {
		struct word *w;
		for (w = hash_table[idx]; w; w = w->next) {
			printf("%16d %.*s\n",
			       w->counter,
			       w->len,
			       w->word);
		}
	}

	return 0;
}
