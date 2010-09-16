/* Stuff which is common to both worker and driver */
#include <sys/fcntl.h>
#include <sys/mman.h>
#include <assert.h>
#include <err.h>
#include <stddef.h>
#include <string.h>

#include "dwc.h"

struct word * hash_table[NR_HASH_TABLE_SLOTS];

/* Never need to call free() -> use a bump allocator */
#define ARENA_SIZE (2 << 20)
struct arena {
	unsigned used; /* includes header */
	unsigned pad; /* Keep it 8-byte aligned */
	unsigned char content[];
};

static struct arena *current_arena;

static struct arena *
new_arena(void)
{
	struct arena *w;
	w = mmap(NULL, ARENA_SIZE, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS,
		 -1, 0);
	w->used = sizeof(struct arena);
	return w;
}


/* Very simple allocator, on the assumption that you never need to
   call free().  Returns zeroed memory. */
void *
bump_malloc(size_t s)
{
	void *res;
	s = (s + 7) & ~7;
	if (current_arena->used + s > ARENA_SIZE) {
		current_arena = new_arena();
		assert(current_arena->used + s <= ARENA_SIZE);
	}
	res = (void *)current_arena + current_arena->used;
	current_arena->used += s;
	return res;
}

void
bump_word_counter(const unsigned char *start, unsigned size,
		  unsigned count)
{
	unsigned long h;
	int idx;
	struct word **pprev, *cursor;

	h = 0;
	for (idx = 0; idx < size / 8; idx++)
		h = ((unsigned long *)start)[idx] + h * 524287;
	for (idx = size & ~7; idx < size; idx++)
		h = start[idx] + h * 127;

	idx = h % NR_HASH_TABLE_SLOTS;
	pprev = &hash_table[idx];
	while (*pprev) {
		cursor = *pprev;
		if (cursor->hash == h &&
		    cursor->len == size &&
		    !memcmp(cursor->word, start, size)) {
			*pprev = cursor->next;
			cursor->next = hash_table[idx];
			hash_table[idx] = cursor;
			cursor->counter += count;
			return;
		}
		pprev = &cursor->next;
	}

	cursor = bump_malloc(sizeof(struct word) + size);
	cursor->hash = h;
	cursor->next = hash_table[idx];
	cursor->counter = count;
	cursor->len = size;
	memcpy(cursor->word, start, size);
	hash_table[idx] = cursor;
	return;
}

void
init_malloc(void)
{
	current_arena = new_arena();
}

void
set_nonblock(int fd)
{
	int flags;
	flags = fcntl(fd, F_GETFL);
	if (flags == -1)
		err(1, "fcntl(F_GETFL)");
	if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
		err(1, "fcntl(F_SETFL)");
}

