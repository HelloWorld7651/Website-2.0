/*
 * merge_patents.c
 *
 * Merges many CSV/TSV files by a patent/application ID key.
 * - Auto-detects per-file delimiter (comma or tab).
 * - Auto-detects the ID column index by header name (tries common names; falls back to col 0).
 * - Files whose names contain "claims_<YYYY>" (case-insensitive) are grouped as "claims" files.
 *   A patent is eligible if it exists in ALL non-claims files AND in AT LEAST ONE claims file (if any provided).
 * - Randomly samples N eligible IDs (Fisher-Yates shuffle).
 * - Output: one merged CSV with:
 *     [ID] + [all non-claims columns] + [all claims columns suffixed with _YYYY]
 *
 * Notes:
 * - This parser supports basic CSV/TSV with quotes (") and escaped quotes ("") within a field.
 * - If your CSV has complex quoting/newlines inside fields, consider using a specialized CSV library.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <time.h>

/* ---------- TUNABLES ---------- */
#define MAX_LINE_LEN        65536
#define INITIAL_CAP_ROWS    131072
#define INITIAL_CAP_COLS    256
#define HASH_BUCKETS        200003  /* large-ish prime */
#define OUTPUT_DELIM        ','     /* change to '\t' for TSV output */

/* ---------- Small string helpers ---------- */
static char *sdup(const char *s) {
    if (!s) return NULL;
    size_t n = strlen(s);
    char *p = (char*)malloc(n+1);
    if (!p) { perror("malloc"); exit(1); }
    memcpy(p, s, n+1);
    return p;
}
static void trim_eol(char *s) {
    if (!s) return;
    size_t n = strlen(s);
    while (n && (s[n-1]=='\n' || s[n-1]=='\r')) s[--n] = '\0';
}

/* lowercase copy in-place of buffer */
static void tolower_inplace(char *s) {
    if (!s) return;
    for (; *s; ++s) *s = (char)tolower((unsigned char)*s);
}

/* strip dir and extension -> short name */
static void file_stem(const char *path, char *out, size_t outsz) {
    const char *base = strrchr(path, '/');
#ifdef _WIN32
    const char *base2 = strrchr(path, '\\');
    if (!base || (base2 && base2 > base)) base = base2;
#endif
    base = base ? base+1 : path;
    const char *dot = strrchr(base, '.');
    size_t len = dot ? (size_t)(dot - base) : strlen(base);
    if (len >= outsz) len = outsz-1;
    memcpy(out, base, len);
    out[len] = '\0';
}

/* ---------- CSV/TSV parsing with quotes ---------- */
typedef struct {
    char **fields;
    int count;
    int cap;
} Fields;

static void fields_init(Fields *f) { f->fields=NULL; f->count=0; f->cap=0; }
static void fields_clear(Fields *f) {
    for (int i=0;i<f->count;++i) free(f->fields[i]);
    free(f->fields);
    f->fields=NULL; f->count=0; f->cap=0;
}
static void fields_push(Fields *f, const char *s, size_t n) {
    if (f->count==f->cap) {
        f->cap = f->cap? f->cap*2 : 16;
        f->fields = (char**)realloc(f->fields, f->cap*sizeof(char*));
        if (!f->fields) { perror("realloc"); exit(1); }
    }
    char *p = (char*)malloc(n+1);
    if (!p) { perror("malloc"); exit(1); }
    memcpy(p,s,n); p[n]='\0';
    f->fields[f->count++] = p;
}

/* parse a delimited line (supports quotes " with "" escaped); delim is ',' or '\t' */
static void parse_delim_line(const char *line, char delim, Fields *out) {
    fields_clear(out); /* reuse container */
    const char *p = line;
    int inq = 0;
    const char *start = p;
    size_t bufcap = strlen(line) + 4;
    char *buf = (char*)malloc(bufcap);
    size_t b = 0;

    for (; *p; ++p) {
        char c = *p;
        if (inq) {
            if (c=='"') {
                if (p[1]=='"') { buf[b++]='"'; ++p; }
                else { inq = 0; }
            } else {
                buf[b++]=c;
            }
        } else {
            if (c=='"') {
                inq = 1; /* start quote */
            } else if (c==delim) {
                fields_push(out, buf, b);
                b=0;
            } else {
                buf[b++]=c;
            }
        }
    }
    /* last field */
    fields_push(out, buf, b);
    free(buf);
}

/* ---------- Hash set / map for strings ---------- */
typedef struct Entry {
    char *key;
    int   value;         /* generic index; -1 if just membership set */
    struct Entry *next;
} Entry;

typedef struct {
    Entry **b;
    size_t nb;
} HMap;

static unsigned long djb2(const char *s) {
    unsigned long h=5381; int c;
    while ((c=(unsigned char)*s++)) h=((h<<5)+h)+c;
    return h;
}
static void hmap_init(HMap *m, size_t nb) {
    m->nb = nb;
    m->b = (Entry**)calloc(nb, sizeof(Entry*));
    if (!m->b) { perror("calloc"); exit(1); }
}
static void hmap_free(HMap *m, int free_keys) {
    for (size_t i=0;i<m->nb;++i) {
        Entry *e=m->b[i];
        while (e) { Entry *n=e->next; if (free_keys) free(e->key); free(e); e=n; }
    }
    free(m->b); m->b=NULL; m->nb=0;
}
/* insert or set; returns pointer to entry */
static Entry* hmap_put(HMap *m, const char *key, int value, int own_copy) {
    unsigned long h = djb2(key) % m->nb;
    for (Entry *e=m->b[h]; e; e=e->next) {
        if (strcmp(e->key,key)==0) { e->value=value; return e; }
    }
    Entry *ne = (Entry*)malloc(sizeof(Entry));
    if (!ne) { perror("malloc"); exit(1); }
    ne->key = own_copy? sdup(key) : (char*)key;
    ne->value = value;
    ne->next = m->b[h];
    m->b[h] = ne;
    return ne;
}
static Entry* hmap_get(HMap *m, const char *key) {
    unsigned long h = djb2(key) % m->nb;
    for (Entry *e=m->b[h]; e; e=e->next) {
        if (strcmp(e->key,key)==0) return e;
    }
    return NULL;
}

/* ---------- Per-file storage ---------- */
typedef struct {
    char *path;
    char delim;         /* ',' or '\t' */
    int id_col;         /* detected ID column index */
    int is_claim;       /* 1 if claims_YYYY file */
    int claim_year;     /* parsed year or 0 */
    /* header columns (kept for output naming) */
    char **cols;
    int ncols;
    /* map: ID -> row index in arrays below */
    HMap id_to_idx;
    /* arrays of rows: for output we store all fields except ID as a single CSV-fragment string */
    char **ids;
    char **row_frag;    /* CSV-fragment string representing all non-ID columns */
    size_t nrows, caprows;
} FileStore;

static int detect_id_col(char **cols, int ncols) {
    /* heuristics: look for common names first (case-insensitive) */
    int best = -1;
    for (int i=0;i<ncols;++i) {
        char buf[512]; snprintf(buf,sizeof(buf),"%s", cols[i]); tolower_inplace(buf);
        if (strstr(buf,"application_number")) return i;
    }
    for (int i=0;i<ncols;++i) {
        char buf[512]; snprintf(buf,sizeof(buf),"%s", cols[i]); tolower_inplace(buf);
        if (strstr(buf,"patent") && (strstr(buf,"number") || strstr(buf,"id"))) return i;
        if (strstr(buf,"app") && strstr(buf,"number")) best = (best==-1? i:best);
    }
    if (best!=-1) return best;
    return 0; /* fallback */
}

static int parse_claim_year_from_name(const char *path) {
    /* case-insensitive find "claims_" then 4 digits */
    char stem[512]; file_stem(path, stem, sizeof(stem));
    tolower_inplace(stem);
    char *p = strstr(stem, "claims_");
    if (!p) return 0;
    p += 7;
    if (!isdigit((unsigned char)p[0])) return 0;
    int y=0;
    for (int i=0;i<4 && isdigit((unsigned char)p[i]);++i) {
        y = y*10 + (p[i]-'0');
    }
    return y;
}

static void filestore_init(FileStore *fs, const char *path) {
    memset(fs, 0, sizeof(*fs));
    fs->path = sdup(path);
    fs->delim = ','; /* default; will detect */
    fs->id_col = 0;
    fs->is_claim = 0;
    fs->claim_year = 0;
    fs->cols = NULL; fs->ncols=0;
    hmap_init(&fs->id_to_idx, HASH_BUCKETS);
    fs->ids=NULL; fs->row_frag=NULL; fs->nrows=0; fs->caprows=0;
}
static void filestore_free(FileStore *fs) {
    if (!fs) return;
    free(fs->path);
    for (int i=0;i<fs->ncols;++i) free(fs->cols[i]);
    free(fs->cols);
    for (size_t i=0;i<fs->nrows;++i) {
        free(fs->ids[i]);
        free(fs->row_frag[i]);
    }
    free(fs->ids); free(fs->row_frag);
    hmap_free(&fs->id_to_idx, 1);
}

static void filestore_push(FileStore *fs, const char *id, const char *frag) {
    if (fs->nrows == fs->caprows) {
        fs->caprows = fs->caprows? fs->caprows*2 : INITIAL_CAP_ROWS;
        fs->ids      = (char**)realloc(fs->ids,      fs->caprows*sizeof(char*));
        fs->row_frag = (char**)realloc(fs->row_frag, fs->caprows*sizeof(char*));
        if (!fs->ids || !fs->row_frag) { perror("realloc"); exit(1); }
    }
    fs->ids[fs->nrows] = sdup(id);
    fs->row_frag[fs->nrows] = sdup(frag);
    hmap_put(&fs->id_to_idx, fs->ids[fs->nrows], (int)fs->nrows, 0 /* key already owned */);
    fs->nrows++;
}

/* ---------- Load one file ---------- */
static void load_file(FileStore *fs) {
    FILE *fp = fopen(fs->path, "r");
    if (!fp) { perror(fs->path); exit(1); }

    char line[MAX_LINE_LEN];
    if (!fgets(line, sizeof(line), fp)) { fclose(fp); return; }
    trim_eol(line);

    /* detect delimiter on header */
    fs->delim = (strchr(line,'\t') && !strchr(line,',')) ? '\t' : ',';
    fs->is_claim = 0;
    fs->claim_year = parse_claim_year_from_name(fs->path);
    if (fs->claim_year > 0) fs->is_claim = 1;

    /* parse header */
    Fields hf; fields_init(&hf);
    parse_delim_line(line, fs->delim, &hf);
    fs->ncols = hf.count;
    fs->cols = (char**)malloc(fs->ncols * sizeof(char*));
    if (!fs->cols) { perror("malloc"); exit(1); }
    for (int i=0;i<fs->ncols;++i) fs->cols[i] = sdup(hf.fields[i]);
    fs->id_col = detect_id_col(fs->cols, fs->ncols);

    /* read data lines */
    Fields row; fields_init(&row);
    while (fgets(line, sizeof(line), fp)) {
        trim_eol(line);
        if (!line[0]) continue;
        fields_clear(&row);
        parse_delim_line(line, fs->delim, &row);
        if (row.count == 0) continue;
        if (fs->id_col >= row.count) continue; /* malformed row */

        const char *id = row.fields[fs->id_col];

        /* Build CSV-fragment of all non-ID columns separated by OUTPUT_DELIM */
        /* Also: standardize inner delimiter to OUTPUT_DELIM */
        size_t est = 1;
        for (int i=0;i<row.count;++i) if (i!=fs->id_col) est += strlen(row.fields[i]) + 1;
        char *frag = (char*)malloc(est + 16);
        if (!frag) { perror("malloc"); exit(1); }
        frag[0]='\0';
        int first=1;
        for (int i=0;i<row.count;++i) if (i!=fs->id_col) {
            if (!first) { size_t L=strlen(frag); frag[L]=OUTPUT_DELIM; frag[L+1]='\0'; }
            first=0;
            /* naive write: if value contains comma/quote, a robust CSV writer should quote it.
               For simplicity, we just write raw. */
            strcat(frag, row.fields[i]);
        }

        filestore_push(fs, id, frag);
        free(frag);
    }
    fields_clear(&hf);
    fields_clear(&row);
    fclose(fp);
}

/* ---------- Simple dynamic array of strings ---------- */
typedef struct {
    char **v; size_t n, cap;
} SVec;
static void svec_init(SVec *s){ s->v=NULL; s->n=0; s->cap=0; }
static void svec_push(SVec *s, char *owned){ if(s->n==s->cap){ s->cap=s->cap? s->cap*2:1024; s->v=realloc(s->v,s->cap*sizeof(char*)); if(!s->v){perror("realloc");exit(1);} } s->v[s->n++]=owned; }
static void svec_free(SVec *s, int free_items){ if(free_items){ for(size_t i=0;i<s->n;++i) free(s->v[i]); } free(s->v); s->v=NULL; s->n=s->cap=0; }

/* ---------- Build eligibility: intersect non-claims, union claims ---------- */
static void build_eligible_ids(FileStore *fs, int nfiles, SVec *eligible) {
    /* gather union of claims IDs */
    HMap claims_union; hmap_init(&claims_union, HASH_BUCKETS);
    int have_claims = 0;
    for (int i=0;i<nfiles;++i) if (fs[i].is_claim) {
        have_claims = 1;
        for (size_t r=0;r<fs[i].nrows;++r) hmap_put(&claims_union, fs[i].ids[r], -1, 1);
    }

    /* pick base: the smallest non-claims file for faster intersection */
    int base = -1;
    for (int i=0;i<nfiles;++i) if (!fs[i].is_claim) {
        if (base==-1 || fs[i].nrows < fs[base].nrows) base = i;
    }
    if (base==-1) {
        /* no non-claims files; eligibility is just claims union */
        if (!have_claims) { /* no files at all? */
            fprintf(stderr,"No files provided.\n");
            hmap_free(&claims_union,1);
            return;
        }
        /* all IDs in claims union are eligible */
        for (size_t b=0;b<claims_union.nb;++b) {
            for (Entry *e=claims_union.b[b]; e; e=e->next) svec_push(eligible, sdup(e->key));
        }
        hmap_free(&claims_union,1);
        return;
    }

    /* For each ID in base, check it is present in ALL other non-claims; and present in union(claims) if have_claims */
    for (size_t r=0;r<fs[base].nrows;++r) {
        const char *id = fs[base].ids[r];
        int ok = 1;
        /* must be in all non-claims files */
        for (int i=0;i<nfiles && ok;++i) {
            if (i==base || fs[i].is_claim) continue;
            if (!hmap_get(&fs[i].id_to_idx, id)) ok = 0;
        }
        if (!ok) continue;
        /* if we have claims files: must be in at least one claims file (i.e., in claims_union) */
        if (have_claims) {
            if (!hmap_get(&claims_union, id)) continue;
        }
        svec_push(eligible, sdup(id));
    }
    hmap_free(&claims_union,1);
}

/* ---------- Shuffle (Fisher-Yates) ---------- */
static void shuffle(char **a, size_t n) {
    for (size_t i=0; i<n; ++i) {
        size_t j = i + (size_t)(rand() % (int)(n - i));
        char *tmp=a[i]; a[i]=a[j]; a[j]=tmp;
    }
}

/* ---------- Build merged header ---------- */
/* We write: [IDName] + for each file: its non-ID columns; for claims files, suffix _YEAR to each column */
static void write_header(FILE *out, FileStore *fs, int nfiles, const char *id_name) {
    /* track used names to de-duplicate */
    HMap used; hmap_init(&used, 4099);
    /* print ID column */
    fprintf(out, "%s", id_name);
    hmap_put(&used, id_name, 1, 1);

    for (int i=0;i<nfiles;++i) {
        /* derive stem for optional prefixing if duplicate names occur */
        char stem[256]; file_stem(fs[i].path, stem, sizeof(stem));
        for (int c=0;c<fs[i].ncols;++c) {
            if (c==fs[i].id_col) continue;
            char namebuf[1024];
            if (fs[i].is_claim && fs[i].claim_year>0) {
                snprintf(namebuf,sizeof(namebuf), "%s_%d", fs[i].cols[c], fs[i].claim_year);
            } else {
                /* as-is; if duplicate, we will append suffix */
                snprintf(namebuf,sizeof(namebuf), "%s", fs[i].cols[c]);
            }
            /* de-dup if needed */
            if (hmap_get(&used, namebuf)) {
                char dedup[1200];
                snprintf(dedup,sizeof(dedup), "%s[%s]", namebuf, stem);
                fprintf(out, "%c%s", OUTPUT_DELIM, dedup);
                hmap_put(&used, sdup(dedup), 1, 1);
            } else {
                fprintf(out, "%c%s", OUTPUT_DELIM, namebuf);
                hmap_put(&used, sdup(namebuf), 1, 1);
            }
        }
    }
    fprintf(out, "\n");
    hmap_free(&used,1);
}

/* ---------- Write merged rows ---------- */
/* For each selected ID: print ID + for each file, its row fragment (non-ID cols).
   If a claims-year file lacks the ID, print blanks for that file's columns. */
static void write_rows(FILE *out, FileStore *fs, int nfiles, char **ids, size_t nsel) {
    for (size_t i=0;i<nsel;++i) {
        const char *id = ids[i];
        fprintf(out, "%s", id);
        for (int f=0; f<nfiles; ++f) {
            /* find index */
            Entry *e = hmap_get(&fs[f].id_to_idx, id);
            if (e) {
                fprintf(out, "%c%s", OUTPUT_DELIM, fs[f].row_frag[e->value]);
            } else {
                /* output blanks for this file's non-ID columns */
                int blanks = fs[f].ncols - 1;
                for (int k=0;k<blanks;++k) {
                    fprintf(out, "%c", OUTPUT_DELIM);
                    /* empty cell */
                }
            }
        }
        fprintf(out, "\n");
    }
}

/* ---------- Main ---------- */
int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s -n <sample_size> -o <output.csv> <file1> <file2> ...\n", argv[0]);
        return 1;
    }
    int sample = 100000;
    const char *outpath = "merged.csv";

    int ai=1;
    for (; ai<argc; ++ai) {
        if (strcmp(argv[ai], "-n")==0 && ai+1<argc) { sample = atoi(argv[++ai]); }
        else if (strcmp(argv[ai], "-o")==0 && ai+1<argc) { outpath = argv[++ai]; }
        else break;
    }
    int nfiles = argc - ai;
    if (nfiles <= 0) {
        fprintf(stderr, "Provide input CSV/TSV files.\n");
        return 1;
    }

    FileStore *stores = (FileStore*)calloc(nfiles, sizeof(FileStore));
    if (!stores) { perror("calloc"); return 1; }

    for (int i=0;i<nfiles;++i) { filestore_init(&stores[i], argv[ai+i]); load_file(&stores[i]); }

    /* Build eligible IDs per your rule:
       - Must be in ALL non-claims files
       - And in AT LEAST ONE claims_year file (if any claims files provided) */
    SVec eligible; svec_init(&eligible);
    build_eligible_ids(stores, nfiles, &eligible);

    if (eligible.n == 0) {
        fprintf(stderr, "No IDs satisfy the criteria (non-claims intersection and claims union).\n");
        for (int i=0;i<nfiles;++i) filestore_free(&stores[i]);
        free(stores);
        return 0;
    }

    /* Sample */
    if ((size_t)sample > eligible.n) sample = (int)eligible.n;
    srand((unsigned)time(NULL));
    shuffle(eligible.v, eligible.n);

    /* Prepare output */
    FILE *out = fopen(outpath, "w");
    if (!out) { perror(outpath); return 1; }

    /* Choose ID column name from the first file's detected ID column */
    const char *id_name = (stores[0].ncols>0 && stores[0].id_col < stores[0].ncols)
                        ? stores[0].cols[stores[0].id_col] : "patent_id";
    write_header(out, stores, nfiles, id_name);
    write_rows(out, stores, nfiles, eligible.v, (size_t)sample);
    fclose(out);

    fprintf(stderr, "Merged %d files. Eligible IDs: %zu. Wrote %d rows to %s\n",
            nfiles, eligible.n, sample, outpath);

    /* cleanup */
    svec_free(&eligible, 1);
    for (int i=0;i<nfiles;++i) filestore_free(&stores[i]);
    free(stores);
    return 0;
}


//for claim text, there is possibility that there is multiple claim text for the same patent ID. Can you merge everything inside that one claim id. 
//Example patent ID 1, there is a coloum for claim text, in claim text coloum there are 10 rows of word for the same patent ID. Can you merge them all together
//so that all the claim text section is taken in. This should also be the same for other documents
