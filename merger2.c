#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <time.h>

#define MAX_LINE_LEN 65536
#define INITIAL_CAP_ROWS 131072
#define INITIAL_CAP_COLS 256
#define HASH_BUCKETS 200003
#define OUTPUT_DELIM ','
#define MERGE_SEP " || "

static char *sdup(const char *s) {
if (!s) return NULL;
size_t n = strlen(s);
char *p = (char*)malloc(n + 1);
if (!p) { perror("malloc"); exit(1); }
memcpy(p, s, n + 1);
return p;
}
static void trim_eol(char *s) {
if (!s) return;
size_t n = strlen(s);
while (n && (s[n-1] == '\n' || s[n-1] == '\r')) s[--n] = '\0';
}
static void tolower_inplace(char *s) {
if (!s) return;
for (; *s; ++s) *s = (char)tolower((unsigned char)*s);
}
static void file_stem(const char *path, char *out, size_t outsz) {
const char *base = strrchr(path, '/');
#ifdef _WIN32
const char *base2 = strrchr(path, '\\');
if (!base || (base2 && base2 > base)) base = base2;
#endif
base = base ? base + 1 : path;
const char *dot = strrchr(base, '.');
size_t len = dot ? (size_t)(dot - base) : strlen(base);
if (len >= outsz) len = outsz - 1;
memcpy(out, base, len);
out[len] = '\0';
}

typedef struct {
char **fields;
int count;
int cap;
} Fields;
static void fields_init(Fields *f) { f->fields = NULL; f->count = 0; f->cap = 0; }
static void fields_clear(Fields *f) {
for (int i = 0; i < f->count; ++i) free(f->fields[i]);
free(f->fields);
f->fields = NULL;
f->count = 0;
f->cap = 0;
}
static void fields_push(Fields *f, const char *s, size_t n) {
if (f->count == f->cap) {
f->cap = f->cap ? f->cap * 2 : 16;
f->fields = (char**)realloc(f->fields, f->cap * sizeof(char*));
if (!f->fields) { perror("realloc"); exit(1); }
}
char *p = (char*)malloc(n + 1);
if (!p) { perror("malloc"); exit(1); }
memcpy(p, s, n);
p[n] = '\0';
f->fields[f->count++] = p;
}
static void parse_delim_line(const char *line, char delim, Fields *out) {
fields_clear(out);
const char *p = line;
int inQuotes = 0;
size_t bufcap = strlen(line) + 4;
char *buf = (char*)malloc(bufcap);
size_t b = 0;
for (; *p; ++p) {
char c = *p;
if (inQuotes) {
if (c == '"') {
if (p[1] == '"') { buf[b++] = '"'; ++p; }
else { inQuotes = 0; }
} else {
buf[b++] = c;
}
} else {
if (c == '"') {
inQuotes = 1;
} else if (c == delim) {
fields_push(out, buf, b);
b = 0;
} else {
buf[b++] = c;
}
}
}
fields_push(out, buf, b);
free(buf);
}

typedef struct Entry {
char *key;
int value;
struct Entry *next;
} Entry;
typedef struct {
Entry **buckets;
size_t nb;
} HMap;
static unsigned long djb2(const char *s) {
unsigned long h = 5381;
int c;
while ((c = (unsigned char)*s++)) {
h = ((h << 5) + h) + c;
}
return h;
}
static void hmap_init(HMap *m, size_t nb) {
m->nb = nb;
m->buckets = (Entry**)calloc(nb, sizeof(Entry*));
if (!m->buckets) { perror("calloc"); exit(1); }
}
static void hmap_free(HMap *m, int free_keys) {
for (size_t i = 0; i < m->nb; ++i) {
Entry *e = m->buckets[i];
while (e) {
Entry *next = e->next;
if (free_keys) free(e->key);
free(e);
e = next;
}
}
free(m->buckets);
m->buckets = NULL;
m->nb = 0;
}
static Entry* hmap_put(HMap *m, const char *key, int value, int own_copy) {
unsigned long h = djb2(key) % m->nb;
for (Entry *e = m->buckets[h]; e; e = e->next) {
if (strcmp(e->key, key) == 0) {
e->value = value;
return e;
}
}
Entry *newEntry = (Entry*)malloc(sizeof(Entry));
if (!newEntry) { perror("malloc"); exit(1); }
newEntry->key = own_copy ? sdup(key) : (char*)key;
newEntry->value = value;
newEntry->next = m->buckets[h];
m->buckets[h] = newEntry;
return newEntry;
}
static Entry* hmap_get(HMap *m, const char *key) {
unsigned long h = djb2(key) % m->nb;
for (Entry *e = m->buckets[h]; e; e = e->next) {
if (strcmp(e->key, key) == 0) return e;
}
return NULL;
}

typedef struct {
char *path;
char delim;
int id_col;
int is_claim;
int claim_year;
char **cols;
int ncols;
int n_nonid;
HMap id_to_idx;
char **ids;
char ***cells;
size_t nrows;
size_t caprows;
} FileStore;
static int detect_id_col(char **cols, int ncols) {
int best = -1;
for (int i = 0; i < ncols; ++i) {
char buf[512];
snprintf(buf, sizeof(buf), "%s", cols[i]);
tolower_inplace(buf);
if (strstr(buf, "application_number")) return i;
}
for (int i = 0; i < ncols; ++i) {
char buf[512];
snprintf(buf, sizeof(buf), "%s", cols[i]);
tolower_inplace(buf);
if (strstr(buf, "patent") && (strstr(buf, "number") || strstr(buf, "id"))) return i;
if (strstr(buf, "app") && strstr(buf, "number")) {
if (best == -1) best = i;
}
}
if (best != -1) return best;
return 0;
}
static int parse_claim_year_from_name(const char *path) {
char stem[512];
file_stem(path, stem, sizeof(stem));
tolower_inplace(stem);
char *p = strstr(stem, "claims_");
if (!p) return 0;
p += 7;
if (!isdigit((unsigned char)p[0])) return 0;
int year = 0;
for (int i = 0; i < 4 && isdigit((unsigned char)p[i]); ++i) {
year = year * 10 + (p[i] - '0');
}
return year;
}
static void filestore_init(FileStore *fileStore, const char *path) {
memset(fileStore, 0, sizeof(*fileStore));
fileStore->path = sdup(path);
fileStore->delim = ',';
fileStore->id_col = 0;
fileStore->is_claim = 0;
fileStore->claim_year = 0;
fileStore->cols = NULL;
fileStore->ncols = 0;
fileStore->n_nonid = 0;
hmap_init(&fileStore->id_to_idx, HASH_BUCKETS);
fileStore->ids = NULL;
fileStore->cells = NULL;
fileStore->nrows = 0;
fileStore->caprows = 0;
}
static void filestore_free(FileStore *fileStore) {
if (!fileStore) return;
free(fileStore->path);
for (int i = 0; i < fileStore->ncols; ++i) {
free(fileStore->cols[i]);
}
free(fileStore->cols);
for (size_t r = 0; r < fileStore->nrows; ++r) {
for (int c = 0; c < fileStore->n_nonid; ++c) {
free(fileStore->cells[r][c]);
}
free(fileStore->cells[r]);
free(fileStore->ids[r]);
}
free(fileStore->ids);
free(fileStore->cells);
hmap_free(&fileStore->id_to_idx, 0);
}
static int filestore_add_row(FileStore *fileStore, const char *id) {
if (fileStore->nrows == fileStore->caprows) {
fileStore->caprows = fileStore->caprows ? fileStore->caprows * 2 : INITIAL_CAP_ROWS;
fileStore->ids = (char**)realloc(fileStore->ids, fileStore->caprows * sizeof(char*));
fileStore->cells = (char***)realloc(fileStore->cells, fileStore->caprows * sizeof(char**));
if (!fileStore->ids || !fileStore->cells) { perror("realloc"); exit(1); }
}
fileStore->ids[fileStore->nrows] = sdup(id);
fileStore->cells[fileStore->nrows] = (char**)calloc((size_t)fileStore->n_nonid, sizeof(char*));
if (!fileStore->cells[fileStore->nrows]) { perror("calloc"); exit(1); }
hmap_put(&fileStore->id_to_idx, fileStore->ids[fileStore->nrows], (int)fileStore->nrows, 0);
return (int)(fileStore->nrows++);
}
static int filestore_get_or_create(FileStore *fileStore, const char *id) {
Entry *e = hmap_get(&fileStore->id_to_idx, id);
if (e) {
return e->value;
}
return filestore_add_row(fileStore, id);
}
static void append_cell_merge(char **dst, const char *src) {
if (!src || !*src) return;
if (!*dst) {
*dst = sdup(src);
return;
}
size_t a = strlen(*dst);
size_t b = strlen(src);
size_t s = strlen(MERGE_SEP);
char *out = (char*)malloc(a + s + b + 1);
if (!out) { perror("malloc"); exit(1); }
memcpy(out, *dst, a);
memcpy(out + a, MERGE_SEP, s);
memcpy(out + a + s, src, b);
out[a + s + b] = '\0';
free(*dst);
*dst = out;
}
static void load_file(FileStore *fileStore) {
FILE *fp = fopen(fileStore->path, "r");
if (!fp) { perror(fileStore->path); exit(1); }
char line[MAX_LINE_LEN];
if (!fgets(line, sizeof(line), fp)) {
fclose(fp);
return;
}
trim_eol(line);
if (strchr(line, '\t') && !strchr(line, ',')) {
fileStore->delim = '\t';
} else {
fileStore->delim = ',';
}
fileStore->claim_year = parse_claim_year_from_name(fileStore->path);
if (fileStore->claim_year > 0) fileStore->is_claim = 1;
Fields headerFields;
fields_init(&headerFields);
parse_delim_line(line, fileStore->delim, &headerFields);
fileStore->ncols = headerFields.count;
fileStore->cols = (char**)malloc((size_t)fileStore->ncols * sizeof(char*));
if (!fileStore->cols) { perror("malloc"); exit(1); }
for (int i = 0; i < fileStore->ncols; ++i) {
fileStore->cols[i] = sdup(headerFields.fields[i]);
}
fileStore->id_col = detect_id_col(fileStore->cols, fileStore->ncols);
fileStore->n_nonid = (fileStore->ncols > 0) ? (fileStore->ncols - 1) : 0;
Fields rowFields;
fields_init(&rowFields);
while (fgets(line, sizeof(line), fp)) {
trim_eol(line);
if (!line[0]) continue;
fields_clear(&rowFields);
parse_delim_line(line, fileStore->delim, &rowFields);
if (rowFields.count == 0) continue;
if (fileStore->id_col >= rowFields.count) continue;
const char *id = rowFields.fields[fileStore->id_col];
int idx = filestore_get_or_create(fileStore, id);
int k = 0;
for (int i = 0; i < rowFields.count; ++i) {
if (i == fileStore->id_col) continue;
if (k < fileStore->n_nonid) {
append_cell_merge(&fileStore->cells[idx][k], rowFields.fields[i]);
}
++k;
}
}
fields_clear(&headerFields);
fields_clear(&rowFields);
fclose(fp);
}

typedef struct {
char **v;
size_t n;
size_t cap;
} SVec;
static void svec_init(SVec *s) {
s->v = NULL;
s->n = 0;
s->cap = 0;
}
static void svec_push(SVec *s, char *owned) {
if (s->n == s->cap) {
s->cap = s->cap ? s->cap * 2 : 1024;
s->v = realloc(s->v, s->cap * sizeof(char*));
if (!s->v) { perror("realloc"); exit(1); }
}
s->v[s->n++] = owned;
}
static void svec_free(SVec *s, int free_items) {
if (free_items) {
for (size_t i = 0; i < s->n; ++i) {
free(s->v[i]);
}
}
free(s->v);
s->v = NULL;
s->n = s->cap = 0;
}

static void build_eligible_ids(FileStore *fileStores, int nfiles, SVec *eligibleIDs) {
HMap claimsUnion;
hmap_init(&claimsUnion, HASH_BUCKETS);
int have_claims = 0;
for (int i = 0; i < nfiles; ++i) {
if (fileStores[i].is_claim) {
have_claims = 1;
for (size_t r = 0; r < fileStores[i].nrows; ++r) {
hmap_put(&claimsUnion, fileStores[i].ids[r], -1, 1);
}
}
}
int baseIndex = -1;
for (int i = 0; i < nfiles; ++i) {
if (!fileStores[i].is_claim) {
if (baseIndex == -1 || fileStores[i].nrows < fileStores[baseIndex].nrows) {
baseIndex = i;
}
}
}
if (baseIndex == -1) {
if (!have_claims) {
fprintf(stderr, "No files provided.\n");
hmap_free(&claimsUnion, 1);
return;
}
for (size_t b = 0; b < claimsUnion.nb; ++b) {
for (Entry *e = claimsUnion.buckets[b]; e; e = e->next) {
svec_push(eligibleIDs, sdup(e->key));
}
}
hmap_free(&claimsUnion, 1);
return;
}
for (size_t r = 0; r < fileStores[baseIndex].nrows; ++r) {
const char *id = fileStores[baseIndex].ids[r];
int ok = 1;
for (int i = 0; i < nfiles && ok; ++i) {
if (i == baseIndex || fileStores[i].is_claim) continue;
if (!hmap_get(&fileStores[i].id_to_idx, id)) {
ok = 0;
}
}
if (!ok) continue;
if (have_claims) {
if (!hmap_get(&claimsUnion, id)) continue;
}
svec_push(eligibleIDs, sdup(id));
}
hmap_free(&claimsUnion, 1);
}

static void shuffle(char **a, size_t n) {
for (size_t i = 0; i < n; ++i) {
size_t j = i + (size_t)(rand() % (int)(n - i));
char *tmp = a[i];
a[i] = a[j];
a[j] = tmp;
}
}

static void write_header(FILE *out, FileStore *fileStores, int nfiles, const char *idName) {
HMap used;
hmap_init(&used, 4099);
fprintf(out, "%s", idName);
hmap_put(&used, idName, 1, 1);
for (int i = 0; i < nfiles; ++i) {
char stem[256];
file_stem(fileStores[i].path, stem, sizeof(stem));
for (int c = 0; c < fileStores[i].ncols; ++c) {
if (c == fileStores[i].id_col) continue;
char namebuf[1024];
if (fileStores[i].is_claim && fileStores[i].claim_year > 0) {
snprintf(namebuf, sizeof(namebuf), "%s_%d", fileStores[i].cols[c], fileStores[i].claim_year);
} else {
snprintf(namebuf, sizeof(namebuf), "%s", fileStores[i].cols[c]);
}
if (hmap_get(&used, namebuf)) {
char dedup[1200];
snprintf(dedup, sizeof(dedup), "%s[%s]", namebuf, stem);
fprintf(out, "%c%s", OUTPUT_DELIM, dedup);
hmap_put(&used, sdup(dedup), 1, 1);
} else {
fprintf(out, "%c%s", OUTPUT_DELIM, namebuf);
hmap_put(&used, sdup(namebuf), 1, 1);
}
}
}
fprintf(out, "\n");
hmap_free(&used, 1);
}

static void write_rows(FILE *out, FileStore *fileStores, int nfiles, char **ids, size_t nsel) {
for (size_t i = 0; i < nsel; ++i) {
const char *id = ids[i];
fprintf(out, "%s", id);
for (int f = 0; f < nfiles; ++f) {
Entry *e = hmap_get(&fileStores[f].id_to_idx, id);
int blanks = fileStores[f].ncols - 1;
if (e) {
int row = e->value;
for (int k = 0; k < fileStores[f].n_nonid; ++k) {
fprintf(out, "%c", OUTPUT_DELIM);
if (fileStores[f].cells[row][k]) {
fputs(fileStores[f].cells[row][k], out);
}
}
} else {
for (int k = 0; k < blanks; ++k) {
fprintf(out, "%c", OUTPUT_DELIM);
}
}
}
fprintf(out, "\n");
}
}

int main(int argc, char **argv) {
if (argc < 2) {
fprintf(stderr, "Wrong Usage, use this: %s -n <sample_size> -o <output.csv> <file1> <file2> ...\n", argv[0]);
return 1;
}
int sample = 100000;
const char *outpath = "merged.csv";
int argIndex = 1;
for (; argIndex < argc; ++argIndex) {
if (strcmp(argv[argIndex], "-n") == 0 && argIndex + 1 < argc) {
sample = atoi(argv[++argIndex]);
} else if (strcmp(argv[argIndex], "-o") == 0 && argIndex + 1 < argc) {
outpath = argv[++argIndex];
} else {
break;
}
}
int nfiles = argc - argIndex;
if (nfiles <= 0) {
fprintf(stderr, "Provide the input CSV/TSV files.\n");
return 1;
}
FileStore *fileStores = (FileStore*)calloc(nfiles, sizeof(FileStore));
if (!fileStores) { perror("calloc"); return 1; }
for (int i = 0; i < nfiles; ++i) {
filestore_init(&fileStores[i], argv[argIndex + i]);
load_file(&fileStores[i]);
}
SVec eligibleIDs;
svec_init(&eligibleIDs);
build_eligible_ids(fileStores, nfiles, &eligibleIDs);
if (eligibleIDs.n == 0) {
fprintf(stderr, "No ID that is shared across CSV file.\n");
for (int i = 0; i < nfiles; ++i) {
filestore_free(&fileStores[i]);
}
free(fileStores);
return 0;
}
if ((size_t)sample > eligibleIDs.n) {
sample = (int)eligibleIDs.n;
}
srand((unsigned)time(NULL));
shuffle(eligibleIDs.v, eligibleIDs.n);
FILE *out = fopen(outpath, "w");
if (!out) { perror(outpath); return 1; }
const char *idName = (fileStores[0].ncols > 0 && fileStores[0].id_col < fileStores[0].ncols)
? fileStores[0].cols[fileStores[0].id_col] : "patent_id";
write_header(out, fileStores, nfiles, idName);
write_rows(out, fileStores, nfiles, eligibleIDs.v, (size_t)sample);
fclose(out);
fprintf(stderr, "Merged %d files. Eligible IDs: %zu. Wrote %d rows to %s\n",
nfiles, eligibleIDs.n, sample, outpath);
svec_free(&eligibleIDs, 1);
for (int i = 0; i < nfiles; ++i) {
filestore_free(&fileStores[i]);
}
free(fileStores);
return 0;
}