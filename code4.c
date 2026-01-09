#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define DEFAULT_SAMPLE_SIZE 10000

// Simple safe strdup
char *safe_strdup(const char *s) {
if (!s) return NULL;
char *p = malloc(strlen(s) + 1);
if (!p) {
perror("malloc");
exit(1);
}
strcpy(p, s);
return p;
}

int main(int argc, char *argv[]) {
if (argc < 4 || argc > 5) {
fprintf(stderr,
"Usage: %s <input_tsv> <full_output_txt> <sample_output_txt> [sample_size]\n",
argv[0]);
fprintf(stderr,
"Example: %s data.tsv all_rows.txt sample_10000.txt 10000\n",
argv[0]);
return 1;
}

const char *input_path = argv[1];
const char *full_path = argv[2];
const char *sample_path = argv[3];

long sample_size = DEFAULT_SAMPLE_SIZE;
if (argc == 5) {
sample_size = atol(argv[4]);
if (sample_size <= 0) {
fprintf(stderr, "Invalid sample_size: %s\n", argv[4]);
return 1;
}
}

FILE *in = fopen(input_path, "r");
FILE *full = fopen(full_path, "w");
FILE *samp = fopen(sample_path, "w");

if (!in) {
perror("Error opening input TSV");
return 1;
}
if (!full || !samp) {
perror("Error opening output TXT file(s)");
fclose(in);
if (full) fclose(full);
if (samp) fclose(samp);
return 1;
}

// We'll store the header line separately,
// and then use reservoir sampling on data rows (excluding header).
char *line = NULL;
size_t len = 0;
ssize_t read;

char *header_line = NULL;

// Reservoir for sample lines (data rows only)
char **sample_lines = malloc(sample_size * sizeof(char *));
if (!sample_lines) {
perror("malloc");
fclose(in);
fclose(full);
fclose(samp);
return 1;
}
long sample_count = 0; // how many we actually have in the reservoir

srand((unsigned int)time(NULL));

long line_index = 0; // counts data lines (excluding header)

// ---------- Read the file line by line ----------
while ((read = getline(&line, &len, in)) != -1) {
// Write every line to the full-output TXT as-is
fputs(line, full);

if (header_line == NULL) {
// First line = header
header_line = safe_strdup(line);
continue;
}

// From here on, these are "data" rows
line_index++;

// Reservoir sampling on data rows (line_index starts at 1 for first data row)
if (sample_count < sample_size) {
// Still filling the reservoir
sample_lines[sample_count++] = safe_strdup(line);
} else {
// Reservoir full: replace a random existing entry with probability sample_size/line_index
long r = rand() % line_index; // random number in [0, line_index-1]
if (r < sample_size) {
// Replace sample_lines[r] with this new line
free(sample_lines[r]);
sample_lines[r] = safe_strdup(line);
}
}
}

// ---------- Write sample output ----------
if (header_line) {
fputs(header_line, samp);
}

for (long i = 0; i < sample_count; ++i) {
fputs(sample_lines[i], samp);
}

printf("Total data rows (excluding header): %ld\n", line_index);
printf("Sample size: %ld (or fewer if file had fewer rows)\n", sample_count);
printf("Full transcription TXT: %s\n", full_path);
printf("Random sample TXT: %s\n", sample_path);

// ---------- Cleanup ----------
free(line);
if (header_line) free(header_line);
for (long i = 0; i < sample_count; ++i) {
free(sample_lines[i]);
}
free(sample_lines);

fclose(in);
fclose(full);
fclose(samp);

return 0;
}