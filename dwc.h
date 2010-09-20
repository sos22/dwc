#include <stdbool.h>
struct word {
	unsigned long hash;
	struct word *next;
	unsigned counter;
	unsigned len;
	unsigned char word[];
};

#define NR_HASH_TABLE_SLOTS 262143
extern struct word * hash_table[NR_HASH_TABLE_SLOTS];
#define BAD_HASH_SLOT_MARKER ((struct word *)0x123)

void *bump_malloc(size_t s);
int bump_word_counter(const unsigned char *work, unsigned wordlen,
		      unsigned count);
void init_malloc(bool use_bump_allocator);
void set_nonblock(int fd);
