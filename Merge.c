#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#define MAX_FIELD 8192

static char *sdup(const char *s){ if(!s) return NULL; size_t n=strlen(s); char *p=malloc(n+1); if(!p){perror("malloc"); exit(1);} memcpy(p,s,n+1); return p; }

static int parse_csv(const char *line, char ***out){
    char **f=NULL; int nf=0, cap=0; char buf[MAX_FIELD]; int bi=0; bool q=false;
    for(size_t i=0;;++i){
        char ch=line[i];
        if(ch=='"'){ if(!q) q=true; else { if(line[i+1]=='"'){ if(bi<MAX_FIELD-1) buf[bi++]='"'; i++; } else q=false; } }
        else if((ch==',' && !q) || ch=='\0' || ch=='\n' || ch=='\r'){
            buf[bi]='\0';
            if(nf>=cap){ cap=cap?cap*2:16; f=realloc(f,cap*sizeof(char*)); if(!f){perror("realloc"); exit(1);} }
            f[nf++]=sdup(buf); bi=0;
            if(ch=='\0'||ch=='\n'||ch=='\r') break;
        } else { if(bi<MAX_FIELD-1) buf[bi++]=ch; }
    }
    *out=f; return nf;
}

static void print_field(FILE *f, const char *s){
    if(!s){ return; }
    bool q=false; for(const char *p=s; *p; ++p) if(*p==','||*p=='"'||*p=='\n'||*p=='\r'){ q=true; break; }
    if(!q){ fputs(s,f); return; }
    fputc('"',f); for(const char *p=s; *p; ++p){ if(*p=='"') fputc('"',f); fputc(*p,f); } fputc('"',f);
}

int main(int argc, char **argv){
    if(argc!=4){ fprintf(stderr,"Usage: %s <file1.csv> <file2.csv> <out.csv>\n", argv[0]); return 1; }
    const char *p1=argv[1], *p2=argv[2], *po=argv[3];

    FILE *f1=fopen(p1,"r"); if(!f1){ perror(p1); return 1; }
    FILE *f2=fopen(p2,"r"); if(!f2){ perror(p2); fclose(f1); return 1; }

    char *line=NULL; size_t len=0; ssize_t rd;

    rd=getline(&line,&len,f1); if(rd==-1){ fprintf(stderr,"empty: %s\n",p1); goto fail; }
    if(rd>0&&(line[rd-1]=='\n'||line[rd-1]=='\r')){ line[--rd]='\0'; if(rd>0&&line[rd-1]=='\r') line[--rd]='\0'; }
    char **h1=NULL; int n1=parse_csv(line,&h1); if(n1==0){ goto fail; }

    rd=getline(&line,&len,f2); if(rd==-1){ fprintf(stderr,"empty: %s\n",p2); goto fail; }
    if(rd>0&&(line[rd-1]=='\n'||line[rd-1]=='\r')){ line[--rd]='\0'; if(rd>0&&line[rd-1]=='\r') line[--rd]='\0'; }
    char **h2=NULL; int n2=parse_csv(line,&h2); if(n2==0){ goto fail; }

    size_t cap2=1024, nrows2=0;
    char **keys2 = malloc(cap2*sizeof(char*));
    char ***vals2 = malloc(cap2*sizeof(char**));
    if(!keys2||!vals2){ perror("malloc"); goto fail; }

    while((rd=getline(&line,&len,f2))!=-1){
        if(rd>0&&(line[rd-1]=='\n'||line[rd-1]=='\r')){ line[--rd]='\0'; if(rd>0&&line[rd-1]=='\r') line[--rd]='\0'; }
        char **f=NULL; int nf=parse_csv(line,&f); if(nf==0){ free(f); continue; }
        if(nf<1 || f[0][0]=='\0'){ for(int i=0;i<nf;++i) free(f[i]); free(f); continue; }
        if(nrows2>=cap2){ cap2*=2; keys2=realloc(keys2,cap2*sizeof(char*)); vals2=realloc(vals2,cap2*sizeof(char**)); if(!keys2||!vals2){ perror("realloc"); goto fail; } }
        keys2[nrows2]=f[0]; 
        int m=(n2>1)?(n2-1):0;
        vals2[nrows2]= (char**)calloc((size_t)m, sizeof(char*));
        if(m && !vals2[nrows2]){ perror("calloc"); goto fail; }
        for(int j=0;j<m;++j){ vals2[nrows2][j] = (j+1<nf) ? f[j+1] : sdup(""); }
        for(int j=m+1;j<nf;++j) free(f[j]); 
        free(f);
        nrows2++;
    }

    FILE *out=fopen(po,"w"); if(!out){ perror(po); goto fail; }

    print_field(out, h1[0]);
    for(int i=1;i<n1;++i){ fputc(',',out); print_field(out,h1[i]); }
    for(int j=1;j<n2;++j){ fputc(',',out); print_field(out,h2[j]); }
    fputc('\n',out);

    while((rd=getline(&line,&len,f1))!=-1){
        if(rd>0&&(line[rd-1]=='\n'||line[rd-1]=='\r')){ line[--rd]='\0'; if(rd>0&&line[rd-1]=='\r') line[--rd]='\0'; }
        char **f=NULL; int nf=parse_csv(line,&f); if(nf==0){ free(f); continue; }
        if(nf<1 || f[0][0]=='\0'){ for(int i=0;i<nf;++i) free(f[i]); free(f); continue; }

        size_t k=0; for(; k<nrows2; ++k) if(strcmp(keys2[k], f[0])==0) break;
        if(k<nrows2){
            print_field(out, f[0]);
            for(int i=1;i<n1;++i){ fputc(',',out); if(i<nf) print_field(out,f[i]); }
            int m=(n2>1)?(n2-1):0;
            for(int j=0;j<m;++j){ fputc(',',out); print_field(out, vals2[k][j]); }
            fputc('\n',out);
        }

        for(int i=0;i<nf;++i) free(f[i]);
        free(f);
    }

    fclose(out);
    for(size_t r=0;r<nrows2;++r){ free(keys2[r]); int m=(n2>1)?(n2-1):0; for(int j=0;j<m;++j) free(vals2[r][j]); free(vals2[r]); }
    free(keys2); free(vals2);
    for(int i=0;i<n1;++i) free(h1[i]); free(h1);
    for(int i=0;i<n2;++i) free(h2[i]); free(h2);
    free(line); fclose(f1); fclose(f2);
    return 0;

fail:
    if(line) free(line);
    if(f1) fclose(f1);
    if(f2) fclose(f2);
    return 1;
}
