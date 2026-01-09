#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAXLINE 65536
#define MAXCOLS 2048
#define MAX_OUT_ROWS 10

typedef struct {
    char *id;       
    char **cells;   
    int n;
} Rec;

typedef struct {
    char delim;
    char **cols;    
    int ncols;
    Rec *rows;
    int nrows;
    int cap;
} TableB;

static char *sdup(const char *s){
    if(!s) return NULL;
    size_t n = strlen(s);
    char *p = (char*)malloc(n+1);
    if(!p){ perror("malloc"); exit(1); }
    memcpy(p, s, n+1);
    return p;
}

static char *readline_stripped(FILE *fp, char *buf, size_t bufsz){
    if(!fgets(buf, bufsz, fp)) return NULL;
    size_t n = strlen(buf);
    while(n && (buf[n-1]=='\n' || buf[n-1]=='\r')) buf[--n] = '\0';
    return buf;
}


static int split_in_place(char *line, char delim, char **out, int max_tok){
    int n=0;
    char *p=line, *start=line;
    while(*p && n<max_tok){
        if(*p == delim){
            *p = '\0';
            out[n++] = start;
            start = p+1;
        }
        p++;
    }
    if(n < max_tok) out[n++] = start;
    return n;
}


static char detect_delim_from_header(const char *hdr){
    return (strchr(hdr, '\t') && !strchr(hdr, ',')) ? '\t' : ',';
}

static void tableb_init(TableB *tb){
    memset(tb, 0, sizeof(*tb));
}

static void tableb_push_row(TableB *tb, char **tok, int ntok){
    if(tb->nrows == tb->cap){
        tb->cap = tb->cap ? tb->cap * 2 : 1024;
        tb->rows = (Rec*)realloc(tb->rows, tb->cap * sizeof(Rec));
        if(!tb->rows){ perror("realloc"); exit(1); }
    }
    Rec *r = &tb->rows[tb->nrows++];
    r->n = tb->ncols;                     
    r->cells = (char**)calloc(tb->ncols, sizeof(char*));
    if(!r->cells){ perror("calloc"); exit(1); }

    for(int i=0;i<tb->ncols;i++){
        const char *src = (i < ntok) ? tok[i] : "";
        r->cells[i] = sdup(src);
    }
    r->id = sdup(r->cells[0]);          
}

static void tableb_free(TableB *tb){
    if(!tb) return;
    if(tb->cols){
        for(int i=0;i<tb->ncols;i++) free(tb->cols[i]);
        free(tb->cols);
    }
    if(tb->rows){
        for(int r=0;r<tb->nrows;r++){
            if(tb->rows[r].cells){
                for(int c=0;c<tb->rows[r].n;c++) free(tb->rows[r].cells[c]);
                free(tb->rows[r].cells);
            }
            free(tb->rows[r].id);
        }
        free(tb->rows);
    }
    memset(tb, 0, sizeof(*tb));
}

static int tableb_find_index(TableB *tb, const char *id){
    for(int i=0;i<tb->nrows;i++){
        if(strcmp(tb->rows[i].id, id) == 0) return i;
    }
    return -1;
}

static void load_table_b(const char *pathB, TableB *tb){
    tableb_init(tb);

    FILE *fp = fopen(pathB, "r");
    if(!fp){ perror(pathB); exit(1); }

    char buf[MAXLINE];
    char *tok[MAXCOLS];

    if(!readline_stripped(fp, buf, sizeof(buf))){
        fclose(fp);
        fprintf(stderr, "Empty file: %s\n", pathB);
        return;
    }
    tb->delim = detect_delim_from_header(buf);
    int hcnt = split_in_place(buf, tb->delim, tok, MAXCOLS);
    tb->ncols = hcnt;
    tb->cols = (char**)calloc(hcnt, sizeof(char*));
    if(!tb->cols){ perror("calloc"); exit(1); }
    for(int i=0;i<hcnt;i++) tb->cols[i] = sdup(tok[i]);

    while(readline_stripped(fp, buf, sizeof(buf))){
        if(!*buf) continue;
        int ntok = split_in_place(buf, tb->delim, tok, MAXCOLS);
        if(ntok == 0) continue;
        tableb_push_row(tb, tok, ntok);
    }

    fclose(fp);
}


static void write_header(FILE *out, const char *idA, char **colsA, int nA, char **colsB, int nB){
    const char *idname = (nA > 0 && idA) ? idA : "id";
    fprintf(out, "%s", idname);

    for(int i=1;i<nA;i++){
        fprintf(out, ",%s", colsA[i]);
    }

    for(int i=1;i<nB;i++){
        fprintf(out, ",%s", colsB[i]);
    }
    fprintf(out, "\n");
}


int main(int argc, char **argv){
    if(argc < 3){
        fprintf(stderr, "Usage: %s <fileA.csv/tsv> <fileB.csv/tsv> [out.csv]\n", argv[0]);
        return 1;
    }
    const char *pathA = argv[1];
    const char *pathB = argv[2];
    const char *outp  = (argc >= 4 ? argv[3] : "merged_simple.csv");

    TableB tb;
    load_table_b(pathB, &tb);

    FILE *fa = fopen(pathA, "r");
    if(!fa){ perror(pathA); tableb_free(&tb); return 1; }

    FILE *fo = fopen(outp, "w");
    if(!fo){ perror(outp); fclose(fa); tableb_free(&tb); return 1; }

    char buf[MAXLINE];
    char *tok[MAXCOLS];

    if(!readline_stripped(fa, buf, sizeof(buf))){
        fprintf(stderr, "Empty file: %s\n", pathA);
        fclose(fa); fclose(fo); tableb_free(&tb); return 1;
    }
    char delimA = detect_delim_from_header(buf);
    int nA = split_in_place(buf, delimA, tok, MAXCOLS);
    char **colsA = (char**)calloc(nA, sizeof(char*));
    for(int i=0;i<nA;i++) colsA[i] = sdup(tok[i]);

    write_header(fo, colsA[0], colsA, nA, tb.cols, tb.ncols);

    int out_count = 0;
    while(out_count < MAX_OUT_ROWS && readline_stripped(fa, buf, sizeof(buf))){
        if(!*buf) continue;
        int ntokA = split_in_place(buf, delimA, tok, MAXCOLS);
        if(ntokA == 0) continue;

        const char *id = tok[0];
        int idxB = tableb_find_index(&tb, id);
        if(idxB < 0){
            fprintf(stderr, "ID '%s' from A not found in B. Aborting.\n", id);
            fclose(fa); fclose(fo); tableb_free(&tb);
            for(int i=0;i<nA;i++) free(colsA[i]);
            free(colsA);
            return 2;
        }

        fprintf(fo, "%s", id);

        for(int i=1;i<nA;i++){
            const char *v = (i < ntokA) ? tok[i] : "";
            fprintf(fo, ",%s", v);
        }

        Rec *rB = &tb.rows[idxB];
        for(int j=1;j<tb.ncols;j++){
            const char *v = (j < rB->n && rB->cells[j]) ? rB->cells[j] : "";
            fprintf(fo, ",%s", v);
        }

        fprintf(fo, "\n");
        out_count++;
    }

    fclose(fa);
    fclose(fo);
    tableb_free(&tb);
    for(int i=0;i<nA;i++) free(colsA[i]);
    free(colsA);

    fprintf(stderr, "Wrote %d merged rows to %s\n", out_count, outp);
    return 0;
}
