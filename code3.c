#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#define MAX_FIELD_BUF 8192
#define INITIAL_TABLE_CAP 1024
#define HASH_BUCKETS 200003 // large prime for hash table

// ---------------- Data structures ----------------

typedef struct {
char *appno; // application_number (key)
char **values; // array of column values (global columns)
} AppRow;

typedef struct {
AppRow *rows;
int count;
int capacity;
} AppTable;

typedef struct {
char **names; // global column names
int count;
int capacity;
} ColumnList;

// Simple chained hash table: key = application_number, value = row index in AppTable
typedef struct HashEntry {
char *key; // pointer to appno string in AppRow
int row_index;
struct HashEntry *next;
} HashEntry;

typedef struct {
HashEntry **buckets;
int size;
} HashTable;


// ---------------- Utility helpers ----------------

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

// Simple DJB2 hash
unsigned long hash_string(const char *s) {
unsigned long h = 5381;
int c;
while ((c = (unsigned char)*s++)) {
h = ((h << 5) + h) + c;
}
return h;
}

// ---------------- CSV parsing (handles quotes) ----------------

int parse_csv_line(const char *line, char ***out_fields) {
char **fields = NULL;
int field_count = 0;
int field_cap = 0;

char buf[MAX_FIELD_BUF];
int buf_idx = 0;
bool inQuote = false;

for (size_t i = 0;; ++i) {
char ch = line[i];

if (ch == '"') {
if (!inQuote) {
inQuote = true;
} else {
if (line[i + 1] == '"') {
// escaped quote ""
if (buf_idx < MAX_FIELD_BUF - 1) buf[buf_idx++] = '"';
i++;
} else {
inQuote = false;
}
}
} else if ((ch == ',' && !inQuote) || ch == '\0' || ch == '\n' || ch == '\r') {
// end of one field
buf[buf_idx] = '\0';

if (field_count >= field_cap) {
field_cap = field_cap == 0 ? 16 : field_cap * 2;
fields = realloc(fields, field_cap * sizeof(char *));
if (!fields) {
perror("realloc");
exit(1);
}
}
fields[field_count++] = safe_strdup(buf);
buf_idx = 0;

if (ch == '\0' || ch == '\n' || ch == '\r') {
break;
}
} else {
if (buf_idx < MAX_FIELD_BUF - 1) {
buf[buf_idx++] = ch;
}
}
}

*out_fields = fields;
return field_count;
}

// ---------------- Column management ----------------

void init_columns(ColumnList *cols) {
cols->names = NULL;
cols->count = 0;
cols->capacity = 0;
}

// Add a new global column if needed, return index
int add_or_get_column(ColumnList *cols, const char *name) {
// Check if already exists
for (int i = 0; i < cols->count; ++i) {
if (strcmp(cols->names[i], name) == 0) {
return i;
}
}
// New column
if (cols->count >= cols->capacity) {
cols->capacity = cols->capacity ? cols->capacity * 2 : 16;
cols->names = realloc(cols->names, cols->capacity * sizeof(char *));
if (!cols->names) {
perror("realloc");
exit(1);
}
}
cols->names[cols->count] = safe_strdup(name);
return cols->count++;
}

// Extend all existing rows to have new_col_count columns
void ensure_row_width(AppTable *table, int old_col_count, int new_col_count) {
if (new_col_count <= old_col_count) return;
for (int i = 0; i < table->count; ++i) {
AppRow *row = &table->rows[i];
row->values = realloc(row->values, new_col_count * sizeof(char *));
if (!row->values) {
perror("realloc");
exit(1);
}
// Initialize new columns to NULL
for (int c = old_col_count; c < new_col_count; ++c) {
row->values[c] = NULL;
}
}
}

// ---------------- AppTable management ----------------

void init_table(AppTable *table) {
table->rows = NULL;
table->count = 0;
table->capacity = 0;
}

AppRow *add_row(AppTable *table, const char *appno, int col_count) {
if (table->count >= table->capacity) {
table->capacity = table->capacity ? table->capacity * 2 : INITIAL_TABLE_CAP;
table->rows = realloc(table->rows, table->capacity * sizeof(AppRow));
if (!table->rows) {
perror("realloc");
exit(1);
}
}
AppRow *row = &table->rows[table->count++];
row->appno = safe_strdup(appno);
row->values = calloc(col_count, sizeof(char *)); // all NULL
if (!row->values) {
perror("calloc");
exit(1);
}
return row;
}

// ---------------- Hash table for appno -> row index ----------------

void init_hash(HashTable *ht, int bucket_count) {
ht->size = bucket_count;
ht->buckets = calloc(bucket_count, sizeof(HashEntry *));
if (!ht->buckets) {
perror("calloc");
exit(1);
}
}

// Get or create row for given appno
AppRow *get_or_create_row(HashTable *ht, AppTable *table,
const char *appno, int col_count) {
unsigned long h = hash_string(appno);
int idx = h % ht->size;
HashEntry *e = ht->buckets[idx];

while (e) {
if (strcmp(e->key, appno) == 0) {
return &table->rows[e->row_index];
}
e = e->next;
}

// Not found -> create new
AppRow *row = add_row(table, appno, col_count);

HashEntry *ne = malloc(sizeof(HashEntry));
if (!ne) {
perror("malloc");
exit(1);
}
ne->key = row->appno; // reuse pointer
ne->row_index = table->count - 1;
ne->next = ht->buckets[idx];
ht->buckets[idx] = ne;

return row;
}

// ---------------- CSV printing with quoting ----------------

void print_csv_field(FILE *f, const char *s) {
if (!s) {
// empty field
return;
}
bool need_quotes = false;
for (const char *p = s; *p; ++p) {
if (*p == ',' || *p == '"' || *p == '\n' || *p == '\r') {
need_quotes = true;
break;
}
}
if (!need_quotes) {
fputs(s, f);
} else {
fputc('"', f);
for (const char *p = s; *p; ++p) {
if (*p == '"') fputc('"', f); // escape quote
fputc(*p, f);
}
fputc('"', f);
}
}

// Sort rows by application_number for nice output
int cmp_row_appno(const void *a, const void *b) {
const AppRow *ra = (const AppRow *)a;
const AppRow *rb = (const AppRow *)b;
return strcmp(ra->appno, rb->appno);
}

// ---------------- Main ----------------

int main(int argc, char *argv[]) {
if (argc < 5) {
fprintf(stderr,
"Usage: %s <output_merged_csv> <min_fill_fraction> <input1.csv> <input2.csv> [input3.csv ...]\n",
argv[0]);
fprintf(stderr,
"Example: %s merged_filtered.csv 0.5 file1.csv file2.csv file3.csv\n", argv[0]);
return 1;
}

const char *out_path = argv[1];
double min_fill_fraction = atof(argv[2]);
if (min_fill_fraction < 0.0) min_fill_fraction = 0.0;
if (min_fill_fraction > 1.0) min_fill_fraction = 1.0;

int num_inputs = argc - 3;
char **inputs = &argv[3];

printf("Merging %d input CSV files.\n", num_inputs);
printf("Will only keep rows with at least %.3f of non-ID columns filled.\n\n",
min_fill_fraction);

ColumnList cols;
init_columns(&cols);

AppTable table;
init_table(&table);

HashTable ht;
init_hash(&ht, HASH_BUCKETS);

int global_col_count = 0;

// ---------- Read and merge each input file ----------
for (int fi = 0; fi < num_inputs; ++fi) {
const char *path = inputs[fi];
FILE *f = fopen(path, "r");
if (!f) {
perror("Error opening input CSV");
continue;
}

char *line = NULL;
size_t len = 0;
ssize_t read;

// ----- Header -----
read = getline(&line, &len, f);
if (read == -1) {
fclose(f);
free(line);
continue;
}
if (read > 0 && (line[read-1]=='\n' || line[read-1]=='\r')) {
line[--read] = '\0';
if (read > 0 && line[read-1]=='\r') line[--read] = '\0';
}

char **header_fields = NULL;
int header_count = parse_csv_line(line, &header_fields);
if (header_count == 0) {
fclose(f);
free(line);
continue;
}

// Map local columns to global columns
int *local_to_global = malloc(header_count * sizeof(int));
if (!local_to_global) {
perror("malloc");
exit(1);
}

// Column 0 is the application_number key.
if (cols.count == 0) {
// First time: define global column 0 as "application_number"
add_or_get_column(&cols, "application_number");
}
local_to_global[0] = 0; // all first columns map to global col 0

int old_cols = cols.count;
// For remaining columns, union them by name
for (int c = 1; c < header_count; ++c) {
int idx = add_or_get_column(&cols, header_fields[c]);
local_to_global[c] = idx;
}
int new_cols = cols.count;
if (new_cols > old_cols) {
ensure_row_width(&table, old_cols, new_cols);
}
global_col_count = new_cols;

// Free header_fields strings
for (int c = 0; c < header_count; ++c) free(header_fields[c]);
free(header_fields);

// ----- Data rows -----
while ((read = getline(&line, &len, f)) != -1) {
if (read > 0 && (line[read-1]=='\n' || line[read-1]=='\r')) {
line[--read] = '\0';
if (read > 0 && line[read-1]=='\r') line[--read] = '\0';
}

char **fields = NULL;
int field_count = parse_csv_line(line, &fields);
if (field_count == 0) {
free(fields);
continue;
}

const char *appno = (field_count > 0) ? fields[0] : NULL;
if (!appno || appno[0] == '\0') {
for (int i = 0; i < field_count; ++i) free(fields[i]);
free(fields);
continue;
}

// Get or create row for this application_number
AppRow *row = get_or_create_row(&ht, &table, appno, global_col_count);

// Ensure row->values has up-to-date width
row->values = realloc(row->values, global_col_count * sizeof(char*));
if (!row->values) {
perror("realloc");
exit(1);
}
// Newly added columns might be uninitialized; set them to NULL
// (Note: simplest is to assume ensure_row_width already did for older rows.)

// Fill in values for columns that exist in this file
for (int c = 1; c < header_count && c < field_count; ++c) {
int gidx = local_to_global[c];
// Free existing value if we overwrite (optional – last file wins)
if (row->values[gidx]) {
free(row->values[gidx]);
}
row->values[gidx] = safe_strdup(fields[c]);
}

for (int i = 0; i < field_count; ++i) free(fields[i]);
free(fields);
}

free(local_to_global);
free(line);
fclose(f);

printf("Merged file: %s\n", path);
}

printf("\nTotal unique application_numbers: %d\n", table.count);
printf("Total merged columns: %d\n", cols.count);

// ---------- Sort rows by application_number for nicer output ----------
qsort(table.rows, table.count, sizeof(AppRow), cmp_row_appno);

// ---------- Write merged & filtered CSV ----------
FILE *out = fopen(out_path, "w");
if (!out) {
perror("Error opening output file");
return 1;
}

// Write header
for (int c = 0; c < cols.count; ++c) {
if (c > 0) fputc(',', out);
print_csv_field(out, cols.names[c]);
}
fputc('\n', out);

int kept_rows = 0;

for (int i = 0; i < table.count; ++i) {
AppRow *row = &table.rows[i];

// Compute completeness (excluding column 0 which is ID)
int total_feature_cols = (cols.count > 1) ? (cols.count - 1) : 0;
int non_empty = 0;

for (int c = 1; c < cols.count; ++c) {
const char *val = row->values[c];
if (val && val[0] != '\0') {
non_empty++;
}
}

double frac = 0.0;
if (total_feature_cols > 0) {
frac = (double)non_empty / (double)total_feature_cols;
}

if (frac < min_fill_fraction) {
// Skip this row – too sparse
continue;
}

// Keep row: write it
kept_rows++;

// application_number (col 0)
print_csv_field(out, row->appno);
// other columns
for (int c = 1; c < cols.count; ++c) {
fputc(',', out);
print_csv_field(out, row->values[c]); // NULL -> empty cell
}
fputc('\n', out);
}

fclose(out);

printf("Rows written (after completeness filter): %d\n", kept_rows);
printf("Merged + filtered CSV written to: %s\n", out_path);

// ---------- Cleanup ----------
for (int i = 0; i < table.count; ++i) {
free(table.rows[i].appno);
for (int c = 0; c < cols.count; ++c) {
free(table.rows[i].values[c]);
}
free(table.rows[i].values);
}
free(table.rows);

for (int c = 0; c < cols.count; ++c) {
free(cols.names[c]);
}
free(cols.names);

for (int i = 0; i < ht.size; ++i) {
HashEntry *e = ht.buckets[i];
while (e) {
HashEntry *next = e->next;
free(e); // e->key is freed via rows above
e = next;
}
}
free(ht.buckets);

return 0;
}