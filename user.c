// user.c — minimal: register, copy, read (size provided)
// Usage: ./user <user-name> <manager-ip> <manager-port> <m-port> <c-port>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define MAXN 16

static int sock_m;
static struct sockaddr_in mgr;

typedef struct { char name[32], ip[64]; int cport; } DiskInfo;
typedef struct { char dss[32]; int n; int b; DiskInfo disk[MAXN]; } DSSCfg;

static int parse_dss_reply(const char* line, DSSCfg* out){
  char tmp[4096]; strncpy(tmp,line,sizeof(tmp)-1); tmp[sizeof(tmp)-1]=0;
  char *save=NULL; char *tok=strtok_r(tmp,"|\n",&save);
  if(!tok||strcmp(tok,"OK")) return -1;
  char *dname=strtok_r(NULL,"|\n",&save); char *nstr=strtok_r(NULL,"|\n",&save);
  char *bstr=strtok_r(NULL,"|\n",&save); char *list=strtok_r(NULL,"|\n",&save);
  if(!dname||!nstr||!bstr||!list) return -1;
  strncpy(out->dss,dname,sizeof(out->dss)-1); out->n=atoi(nstr); out->b=atoi(bstr);
  int idx=0; char *save2=NULL; char *it=strtok_r(list,";",&save2);
  while(it&&idx<out->n){
    char piece[256]; strncpy(piece,it,sizeof(piece)-1); piece[sizeof(piece)-1]=0;
    char *save3=NULL; char *nm=strtok_r(piece,",",&save3);
    char *ip=strtok_r(NULL,",",&save3); char *cp=strtok_r(NULL,",",&save3);
    if(!nm||!ip||!cp) return -1;
    strncpy(out->disk[idx].name,nm,sizeof(out->disk[idx].name)-1);
    strncpy(out->disk[idx].ip,ip,sizeof(out->disk[idx].ip)-1);
    out->disk[idx].cport=atoi(cp); idx++; it=strtok_r(NULL,";",&save2);
  }
  return (idx==out->n)?0:-1;
}

static void xor_bytes(unsigned char* acc, const unsigned char* src, int len){
  for(int i=0;i<len;i++) acc[i]^=src[i];
}

static int send_block(const DiskInfo* d, const char* dss,long stripe,int diskIndex,int isParity,const unsigned char* data,int len){
  int s=socket(AF_INET,SOCK_DGRAM,0); if(s<0){perror("socket");return -1;}
  struct sockaddr_in dst={0}; dst.sin_family=AF_INET; dst.sin_port=htons(d->cport);
  if(inet_pton(AF_INET,d->ip,&dst.sin_addr)!=1){ fprintf(stderr,"bad disk ip\n"); close(s); return -1; }
  char header[256]; int hlen=snprintf(header,sizeof(header),"store|%s|%ld|%d|%d|%d\n",dss,stripe,diskIndex,isParity,len);
  unsigned char *pkt=malloc(hlen+len); memcpy(pkt,header,hlen); if(len>0) memcpy(pkt+hlen,data,len);
  int rc=sendto(s,pkt,hlen+len,0,(struct sockaddr*)&dst,sizeof(dst)); free(pkt); close(s); return (rc==hlen+len)?0:-1;
}

// fetch one block (returns malloc'd buffer and len via *outlen)
static unsigned char* fetch_block(const DiskInfo* d, const char* dss,long stripe,int diskIndex,int isParity,int* outlen){
  int s=socket(AF_INET,SOCK_DGRAM,0); if(s<0){perror("socket");return NULL;}
  struct sockaddr_in me={0}; me.sin_family=AF_INET; me.sin_addr.s_addr=INADDR_ANY; me.sin_port=0;
  if(bind(s,(struct sockaddr*)&me,sizeof(me))<0){perror("bind tmp"); close(s); return NULL;}
  struct sockaddr_in dst={0}; dst.sin_family=AF_INET; dst.sin_port=htons(d->cport);
  if(inet_pton(AF_INET,d->ip,&dst.sin_addr)!=1){fprintf(stderr,"bad disk ip\n"); close(s); return NULL;}
  char req[256]; int rlen=snprintf(req,sizeof(req),"fetch|%s|%ld|%d|%d\n",dss,stripe,diskIndex,isParity);
  if(sendto(s,req,rlen,0,(struct sockaddr*)&dst,sizeof(dst))<0){perror("sendto fetch"); close(s); return NULL;}

  unsigned char buf[70000]; struct sockaddr_in src; socklen_t sl=sizeof(src);
  ssize_t n=recvfrom(s,buf,sizeof(buf),0,(struct sockaddr*)&src,&sl);
  close(s); if(n<=0) return NULL;

  // parse header: block|dss|stripe|diskIndex|isParity|len\n<payload>
  int hdr_end=-1; for(int i=0;i<n;i++){ if(buf[i]=='\n'){ hdr_end=i; break; } }
  if(hdr_end<0) return NULL;
  char head[256]; memcpy(head,buf,hdr_end); head[hdr_end]=0;
  char *save=NULL; char *cmd=strtok_r(head,"|",&save);
  if(!cmd||strcmp(cmd,"block")) return NULL;
  strtok_r(NULL,"|",&save); // dss
  strtok_r(NULL,"|",&save); // stripe
  strtok_r(NULL,"|",&save); // diskIndex
  strtok_r(NULL,"|",&save); // isParity
  char *lstr=strtok_r(NULL,"|",&save); if(!lstr) return NULL;
  int blen=atoi(lstr);
  if(blen<0 || (int)n < hdr_end+1+blen) return NULL;

  unsigned char *out = malloc(blen?blen:1);
  if(blen>0) memcpy(out, buf+hdr_end+1, blen);
  *outlen = blen;
  return out;
}

// ask manager for DSS cfg via a temp socket
static int get_dss_cfg(const char* dss_name, DSSCfg* cfg){
  int tmp=socket(AF_INET,SOCK_DGRAM,0); if(tmp<0){perror("socket"); return -1;}
  struct sockaddr_in me_tmp={0}; me_tmp.sin_family=AF_INET; me_tmp.sin_addr.s_addr=INADDR_ANY; me_tmp.sin_port=0;
  if(bind(tmp,(struct sockaddr*)&me_tmp,sizeof(me_tmp))<0){perror("bind tmp"); close(tmp); return -1;}
  char req[256]; snprintf(req,sizeof(req),"get-dss|%s\n",dss_name);
  if(sendto(tmp,req,strlen(req),0,(struct sockaddr*)&mgr,sizeof(mgr))<0){perror("sendto tmp"); close(tmp); return -1;}
  char line[4096]; struct sockaddr_in src; socklen_t sl=sizeof(src);
  ssize_t n=recvfrom(tmp,line,sizeof(line)-1,0,(struct sockaddr*)&src,&sl); close(tmp);
  if(n<=0){ fprintf(stderr,"no reply to get-dss\n"); return -1; }
  line[n]=0; if(strncmp(line,"OK|",3)!=0){ printf("%s",line); return -1; }
  return parse_dss_reply(line,cfg);
}

static int do_copy(const char* dss, const char* path){
  DSSCfg cfg; if(get_dss_cfg(dss,&cfg)!=0){ fprintf(stderr,"bad dss\n"); return -1; }
  printf("[user] copy using DSS=%s, n=%d, b=%d\n", cfg.dss, cfg.n, cfg.b);
  FILE* f=fopen(path,"rb"); if(!f){ perror("fopen"); return -1; }
  long stripe=0; unsigned char *blk[MAXN]={0};
  for(;;){
    int k, len[MAXN]={0};
    for(k=0;k<cfg.n-1;k++){ if(!blk[k]) blk[k]=malloc(cfg.b); int g=fread(blk[k],1,cfg.b,f); len[k]=g; if(g==0) break; }
    if(k==0) break;
    int maxlen=0; for(int i=0;i<k;i++) if(len[i]>maxlen) maxlen=len[i];
    unsigned char *par=calloc(1, maxlen?maxlen:1);
    for(int i=0;i<k;i++) xor_bytes(par, blk[i], len[i]);
    int parityDisk = stripe % cfg.n, dbi=0;
    for(int di=0; di<cfg.n; di++){
      const DiskInfo* D=&cfg.disk[di];
      if(di==parityDisk) send_block(D,cfg.dss,stripe,di,1,par,maxlen);
      else { if(dbi<k) send_block(D,cfg.dss,stripe,di,0,blk[dbi],len[dbi]), dbi++; else { unsigned char z=0; send_block(D,cfg.dss,stripe,di,0,&z,0); } }
    }
    free(par); stripe++; if(k<cfg.n-1) break;
  }
  for(int i=0;i<MAXN;i++) free(blk[i]); fclose(f); printf("[user] copy complete.\n"); return 0;
}

static int do_read(const char* dss, const char* outpath, long total){
  DSSCfg cfg; if(get_dss_cfg(dss,&cfg)!=0){ fprintf(stderr,"bad dss\n"); return -1; }
  printf("[user] read using DSS=%s, n=%d, b=%d, size=%ld\n", cfg.dss, cfg.n, cfg.b, total);
  FILE* out=fopen(outpath,"wb"); if(!out){ perror("fopen out"); return -1; }
  long written=0, stripe=0;
  while(written < total){
    int parityDisk = stripe % cfg.n;
    for(int di=0; di<cfg.n && written < total; di++){
      int isParity = (di==parityDisk);
      int blen=0;
      unsigned char* data = fetch_block(&cfg.disk[di], cfg.dss, stripe, di, isParity, &blen);
      if(!data) blen=0;
      if(!isParity && blen>0){
        long need = total - written;
        int w = (blen > need) ? (int)need : blen;
        if(w>0) { fwrite(data,1,w,out); written += w; }
      }
      free(data);
    }
    stripe++;
  }
  fclose(out);
  printf("[user] read complete -> %s (%ld bytes)\n", outpath, written);
  return 0;
}

int main(int argc, char** argv){
  if(argc!=6){ fprintf(stderr,"usage: user <user-name> <manager-ip> <manager-port> <m-port> <c-port>\n"); return 1; }
  const char* uname=argv[1], *mip=argv[2];
  int mport_mgr=atoi(argv[3]), my_mport=atoi(argv[4]), my_cport=atoi(argv[5]);

  sock_m=socket(AF_INET,SOCK_DGRAM,0); if(sock_m<0){perror("socket"); return 1;}
  struct sockaddr_in me={0}; me.sin_family=AF_INET; me.sin_addr.s_addr=INADDR_ANY; me.sin_port=htons(my_mport);
  if(bind(sock_m,(struct sockaddr*)&me,sizeof(me))<0){ perror("bind"); return 1; }
  memset(&mgr,0,sizeof(mgr)); mgr.sin_family=AF_INET; mgr.sin_port=htons(mport_mgr);
  if(inet_pton(AF_INET,mip,&mgr.sin_addr)!=1){ fprintf(stderr,"bad manager ip\n"); return 1; }

  char reg[256]; snprintf(reg,sizeof(reg),"register-user|%s|%s|%d|%d\n", uname, "0.0.0.0", my_mport, my_cport);
  sendto(sock_m, reg, strlen(reg), 0, (struct sockaddr*)&mgr, sizeof(mgr));
  printf("[user] registered, ready.\n");

  char line[1024];
  while(fgets(line,sizeof(line),stdin)){
    if(!strncmp(line,"quit",4)) break;

    if(!strncmp(line,"copy|",5)){
      char L[1024]; strncpy(L,line,sizeof(L)-1); L[sizeof(L)-1]=0;
      char *save=NULL; strtok_r(L,"|\n",&save);
      char *dss=strtok_r(NULL,"|\n",&save);
      char *path=strtok_r(NULL,"|\n",&save);
      if(!dss||!path){ printf("[user] usage: copy|<DSS>|</abs/path/file>\n"); continue; }
      do_copy(dss,path); continue;
    }
    if(!strncmp(line,"read|",5)){
      char L[1024]; strncpy(L,line,sizeof(L)-1); L[sizeof(L)-1]=0;
      char *save=NULL; strtok_r(L,"|\n",&save);
      char *dss = strtok_r(NULL,"|\n",&save);
      char *out = strtok_r(NULL,"|\n",&save);
      char *szs = strtok_r(NULL,"|\n",&save);
      if(!dss||!out||!szs){ printf("[user] usage: read|<DSS>|</abs/path/out>|<bytes>\n"); continue; }
      long total = atol(szs); if(total<=0){ printf("[user] size must be >0\n"); continue; }
      do_read(dss,out,total); continue;
    }

    // pass-through to manager (e.g., configure-dss)
    sendto(sock_m, line, strlen(line), 0, (struct sockaddr*)&mgr, sizeof(mgr));{
  struct timeval tv = {1, 0};
  setsockopt(sock_m, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  struct sockaddr_in src; socklen_t sl = sizeof(src);
  char resp[2048];
  ssize_t n = recvfrom(sock_m, resp, sizeof(resp)-1, 0, (struct sockaddr*)&src, &sl);
  if (n > 0) {
    resp[n] = 0;
    printf("%s", resp);
    fflush(stdout);
  }
}
  }
  return 0;
}
