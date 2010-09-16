/* Simple thing to chunk an input file into a bunch of output files. */
#define _GNU_SOURCE
#include <err.h>
#include <stdio.h>
#include <stdlib.h>

int
main(int argc, char *argv[])
{
	char *input = argv[1];
	int nr_outputs = atoi(argv[2]);
	char *output_prefix = argv[3];
	FILE *inp = fopen(input, "r");
	long file_size;
	unsigned x;
	unsigned long chunk_size;

	if (!inp)
		err(1, "opening %s", input);
	if (fseek(inp, 0, SEEK_END) < 0)
		err(1, "fseeko(%s)", input);
	file_size = ftell(inp);
	if (file_size == -1)
		err(1, "ftell(%s)", input);
	rewind(inp);

	chunk_size = file_size / nr_outputs;
	printf("Chunk size %ld\n", chunk_size);

	for (x = 0; x < nr_outputs; x++) {
		char *output;
		FILE *out;
		unsigned long written_this_file;
		static char buffer[1 << 20];
		size_t read_this_time;
		size_t written_this_read;
		size_t written_this_time;
		unsigned to_read;

		/* Make sure the last chunk captures the bit which
		   isn't neatly divisible. */
		if (x == nr_outputs - 1)
			chunk_size += file_size % nr_outputs;

		asprintf(&output, "%s_%d", output_prefix, x);
		out = fopen(output, "w");

		for (written_this_file = 0; written_this_file < chunk_size; written_this_file += read_this_time) {
			to_read = sizeof(buffer);
			if (written_this_file + to_read > chunk_size)
				to_read = chunk_size - written_this_file;
			read_this_time = fread(buffer, 1, to_read, inp);
			if (read_this_time == 0) {
				if (feof(inp))
					errx(1, "%s seemed to shrink while we were reading it?",
					     input);
				err(1, "reading %s", input);
			}
			for (written_this_read = 0;
			     written_this_read < read_this_time;
			     written_this_read += written_this_time) {
				written_this_time = fwrite(buffer, 1, read_this_time - written_this_read, out);
				if (written_this_time == 0)
					err(1, "writing to %s", output);
			}
		}

		printf("Wrote %ld to %s\n", written_this_file, output);
		if (fclose(out) == EOF)
			err(1, "closing %s", output);
		free(output);
	}

	return 0;
}
