struct word {
	unsigned long hash;
	struct word *next;
	unsigned counter;
	unsigned len;
	unsigned char word[];
};

#define NR_HASH_TABLE_SLOTS 131072
extern struct word * hash_table[NR_HASH_TABLE_SLOTS];

void *bump_malloc(size_t s);
void bump_word_counter(const unsigned char *work, unsigned wordlen,
		       unsigned count);
void init_malloc(void);
void set_nonblock(int fd);
