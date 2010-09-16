/* Worker process */
#include <sys/types.h>
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
	return c == ' ' || c == '\r' || c == '\n' || c == '\t' || c == '\f';
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
send_word(const unsigned char *start, unsigned size)
{
	unsigned tx_size;
	unsigned prod;

	/* Pascal strings with two-byte length header */
	tx_size = size + 2;

	while (tx_size + tx_buffer_producer - tx_buffer_consumer > TX_BUFFER_SIZE)
		flush_some_output();

	prod = tx_buffer_producer % TX_BUFFER_SIZE;
	if ( (tx_buffer_producer + tx_size) / TX_BUFFER_SIZE ==
	     tx_buffer_producer / TX_BUFFER_SIZE ) {
		assert(prod + 2 + size <= TX_BUFFER_SIZE);
		*(unsigned short *)(tx_buffer + prod) = size;
		memcpy(tx_buffer + prod + 2,
		       start, size);
	} else {
		if ( prod == TX_BUFFER_SIZE - 1) {
			tx_buffer[TX_BUFFER_SIZE-1] = size & 0xff;
			tx_buffer[0] = size >> 8;
			memcpy(tx_buffer + 1, start, size);
		} else {
			unsigned c1;
			*(unsigned short *)(tx_buffer + prod) = size;
			c1 = TX_BUFFER_SIZE - (prod + 2);
			assert(c1 < size);
			memcpy(tx_buffer + prod + 2,
			       start,
			       c1);
			memcpy(tx_buffer,
			       start + c1,
			       size - c1);
		}
	}
	tx_buffer_producer += tx_size;
}

static void
send_words(const struct word *w)
{
	unsigned tx_size;
	unsigned prod;

	tx_size = 6 + w->len;
	while (tx_size + tx_buffer_producer - tx_buffer_consumer > TX_BUFFER_SIZE)
		flush_some_output();
	prod = tx_buffer_producer % TX_BUFFER_SIZE;

	if ( (tx_buffer_producer + 4) / TX_BUFFER_SIZE ==
	     tx_buffer_producer / TX_BUFFER_SIZE ) {
		*(unsigned *)(tx_buffer + prod) = w->counter;
	} else {
		unsigned c1;
		c1 = TX_BUFFER_SIZE - prod;
		memcpy(tx_buffer + prod,
		       &w->counter,
		       c1);
		memcpy(tx_buffer,
		       (void *)&w->counter + c1,
		       4 - c1);
	}
	tx_buffer_producer += 4;
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

	init_malloc();

	accept_on_ports(atol(argv[1]), atol(argv[2]), &rx_fd, &tx_fd);

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
			for (w = hash_table[idx]; w; w = w->next)
				send_words(w);
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
	if (initial_word_size == rx_buffer_avail) {
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
		if (word_end == rx_buffer_avail) {
			replenish_rx_buffer();
			goto find_word;
		}

		bump_word_counter(rx_buffer + rx_buffer_used, word_end - rx_buffer_used, 1);
		rx_buffer_used = word_end;
	}
}
