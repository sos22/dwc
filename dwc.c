/* Worker process */
#include <sys/types.h>
#include <sys/fcntl.h>
#include <sys/poll.h>
#include <arpa/inet.h>
#include <assert.h>
#include <err.h>
#include <errno.h>
#include <setjmp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "dwc.h"

static int
is_space(unsigned char c)
{
	return !((c >= '0' && c <= '9') ||
		 (c >= 'A' && c <= 'Z') ||
		 (c >= 'a' && c <= 'z'));
}

static void
down_case(unsigned char *start, unsigned len)
{
	unsigned x;
	for (x = 0; x < len; x++)
		if (start[x] >= 'A' && start[x] <= 'Z')
			start[x] = start[x] - 'A' + 'a';
}

/* Have a 1MB buffer */
#define RX_BUFFER_SIZE (1 << 20)
static int rx_fd;
static unsigned char rx_buffer[RX_BUFFER_SIZE + 1];
static unsigned rx_buffer_avail;
static unsigned rx_buffer_used;

static jmp_buf
finished_buffer;

#define MIN_READ_SIZE 32768

static void
replenish_rx_buffer()
{
	ssize_t rx;

	if (RX_BUFFER_SIZE - rx_buffer_avail < MIN_READ_SIZE) {
		memmove(rx_buffer, rx_buffer + rx_buffer_used, rx_buffer_avail - rx_buffer_used);
		rx_buffer_avail -= rx_buffer_used;
		rx_buffer_used = 0;
	}
	/* This has a nasty side effect: if a single word is bigger
	 * than 1MB, we split it.  Not necessarily entirely correct,
	 * but not completely unreasonable. */
	if (rx_buffer_avail == RX_BUFFER_SIZE) {
		warnx("replenishing RX buffer when it was already full");
		return;
	}
	rx = read(rx_fd, rx_buffer + rx_buffer_avail, RX_BUFFER_SIZE - rx_buffer_avail);
	if (rx < 0)
		err(1, "reading input");
	if (!rx)
		longjmp(finished_buffer, 1);
	rx_buffer_avail += rx;
}

#define TX_BUFFER_SIZE (1 << 20)
static int tx_fd;
static unsigned char tx_buffer[TX_BUFFER_SIZE];
static unsigned tx_buffer_producer;
static unsigned tx_buffer_consumer;

static void
flush_some_output()
{
	ssize_t sent;
	unsigned to_send;

	to_send = tx_buffer_producer - tx_buffer_consumer;
	if ( tx_buffer_producer / TX_BUFFER_SIZE != tx_buffer_consumer / TX_BUFFER_SIZE )
		to_send = TX_BUFFER_SIZE - (tx_buffer_consumer % TX_BUFFER_SIZE);
retry:
	sent = write(tx_fd, tx_buffer + (tx_buffer_consumer % TX_BUFFER_SIZE), to_send);
	if (sent < 0) {
		if (errno == EAGAIN) {
			struct pollfd p;
			p.fd = tx_fd;
			p.events = POLLOUT|POLLERR;
			p.revents = 0;
			if (poll(&p, 1, -1) < 0)
				err(1, "poll output");
			goto retry;
		}
		err(1, "sending output");
	}
	if (sent == 0)
		errx(1, "receiver hung up on us");
	tx_buffer_consumer += sent;
}

static void
transfer_bytes(const void *_bytes, unsigned nr_bytes)
{
	const unsigned char *bytes = _bytes;
	unsigned transferred;
	unsigned this_time;

	for (transferred = 0;
	     transferred < nr_bytes;
	     transferred += this_time) {
		if ( tx_buffer_producer - tx_buffer_consumer == TX_BUFFER_SIZE )
			flush_some_output();
		this_time = nr_bytes - transferred;
		if ( this_time + tx_buffer_producer - tx_buffer_consumer > TX_BUFFER_SIZE )
			this_time = TX_BUFFER_SIZE - (tx_buffer_producer - tx_buffer_consumer);
		if ( (this_time + tx_buffer_producer) / TX_BUFFER_SIZE !=
		     tx_buffer_producer / TX_BUFFER_SIZE )
			this_time = TX_BUFFER_SIZE - (tx_buffer_producer % TX_BUFFER_SIZE);
		memcpy(tx_buffer + (tx_buffer_producer % TX_BUFFER_SIZE),
		       bytes + transferred,
		       this_time);
		tx_buffer_producer += this_time;
	}
}

static void
send_word(const unsigned char *start, unsigned size)
{
	transfer_bytes(&size, 4);
	transfer_bytes(start, size);
}

static void
send_words(const struct word *w)
{
	transfer_bytes(&w->counter, 4);
	send_word(w->word, w->len);
}

static void
flush_output(void)
{
	while (tx_buffer_consumer != tx_buffer_producer)
		flush_some_output();
}

static void
accept_on_ports(int port_nr_1, int port_nr_2,
		int *fd_1, int *fd_2)
{
	int listen_sock_1;
	int listen_sock_2;
	struct sockaddr_in sin;

	listen_sock_1 = socket(AF_INET, SOCK_STREAM, 0);
	if (listen_sock_1 < 0)
		err(1, "creating listening socket");
	memset(&sin, 0, sizeof(sin));
	sin.sin_family = AF_INET;
	sin.sin_port = htons(port_nr_1);
	if (bind(listen_sock_1, (struct sockaddr *)&sin, sizeof(sin)) < 0)
		err(1, "binding to port %d", port_nr_1);
	if (listen(listen_sock_1, 1) < 0)
		err(1, "listen()");

	listen_sock_2 = socket(AF_INET, SOCK_STREAM, 0);
	if (listen_sock_2 < 0)
		err(1, "creating listening socket");
	sin.sin_port = htons(port_nr_2);
	if (bind(listen_sock_2, (struct sockaddr *)&sin, sizeof(sin)) < 0)
		err(1, "binding to port %d", port_nr_2);
	if (listen(listen_sock_2, 1) < 0)
		err(1, "listen()");

	*fd_1 = accept(listen_sock_1, NULL, NULL);
	if (*fd_1 < 0)
		err(1, "accept()");
	close(listen_sock_1);
	*fd_2 = accept(listen_sock_2, NULL, NULL);
	if (*fd_2 < 0)
		err(1, "accept()");
	close(listen_sock_2);
}

int
main(int argc, char *argv[])
{
	unsigned initial_word_size;
	unsigned word_end;
	volatile int sent_initial_word;
	int idx;

	init_malloc(true);

	if (argc == 1)
		errx(1, "need either --stdin or two port numbers");
	if (!strcmp(argv[1], "--stdin")) {
		if (argc != 2)
			errx(1, "don't want other arguments with --stdin mode");
		rx_fd = 0;
		tx_fd = 1;
	} else if (!strcmp(argv[1], "--prepopulate")) {
		int tmp;

		if (argc != 4)
			errx(1, "wrong number of arguments for prepopulate mode");
		accept_on_ports(atol(argv[2]), atol(argv[3]), &rx_fd, &tx_fd);
		tmp = open("/tmp/worker_dump.txt", O_RDWR | O_TRUNC | O_CREAT, 0666);
		if (tmp < 0)
			err(1, "open /tmp/worker_dump.txt");
		while (1) {
			unsigned char buf[16384];
			ssize_t avail;
			size_t written;
			ssize_t written_this_time;
			avail = read(rx_fd, buf, sizeof(buf));
			if (avail == 0)
				break;
			if (avail < 0)
				err(1, "receiving for pre-populate");
			for (written = 0; written < avail; written += written_this_time) {
				written_this_time = write(tmp, buf + written, avail - written);
				if (written_this_time <= 0)
					err(1, "writing for pre-populate");
			}
		}
		printf("Starting compute phase\n");
		close(rx_fd);
		rx_fd = tmp;
		lseek(rx_fd, 0, SEEK_SET);
	} else {
		if (argc != 3)
			errx(1, "wrong number of arguments for non-stdin mode");
		accept_on_ports(atol(argv[1]), atol(argv[2]), &rx_fd, &tx_fd);
	}

	set_nonblock(tx_fd);

	if (setjmp(finished_buffer)) {
		/* Hit EOF on stdin. */
		close(rx_fd);

		if (!sent_initial_word) {
			/* Can happen if the input is completely empty */
			send_word((const unsigned char *)"", 0);
		}

		/* Send the trailer word */
		send_word(rx_buffer + rx_buffer_used, rx_buffer_avail - rx_buffer_used);

		for (idx = 0; idx < NR_HASH_TABLE_SLOTS; idx++) {
			struct word *w;
			for (w = hash_table[idx]; w; w = w->next) {
				assert(w->hash % NR_HASH_TABLE_SLOTS == idx);
				send_words(w);
			}
		}

		flush_output();

		close(tx_fd);

		return 0;
	}

	replenish_rx_buffer();

	/* Find the first word. */
find_first_word:
	rx_buffer[rx_buffer_avail] = ' ';
	for (initial_word_size = 0; !is_space(rx_buffer[initial_word_size]); initial_word_size++)
		;
	if (initial_word_size == rx_buffer_avail &&
	    rx_buffer_avail != RX_BUFFER_SIZE) {
		replenish_rx_buffer();
		goto find_first_word;
	}

	send_word(rx_buffer, initial_word_size);
	sent_initial_word = 1;
	rx_buffer_used = initial_word_size;

	while (1) {
		/* Skip a run of spaces.  We know we start in this
		   state because we skip the first word. */

	skip_spaces:
		rx_buffer[rx_buffer_avail] = 'X';
		while (is_space(rx_buffer[rx_buffer_used]))
			rx_buffer_used++;
		if (rx_buffer_used == rx_buffer_avail) {
			replenish_rx_buffer();
			goto skip_spaces;
		}

		/* Find the next word. */
	find_word:
		rx_buffer[rx_buffer_avail] = ' ';
		for (word_end = rx_buffer_used;
		     !is_space(rx_buffer[word_end]);
		     word_end++)
			;
		if (word_end == rx_buffer_avail &&
		    rx_buffer_avail != RX_BUFFER_SIZE) {
			replenish_rx_buffer();
			goto find_word;
		}

		down_case(rx_buffer + rx_buffer_used, word_end - rx_buffer_used);

		bump_word_counter(rx_buffer + rx_buffer_used, word_end - rx_buffer_used, 1);
		rx_buffer_used = word_end;
	}
}
