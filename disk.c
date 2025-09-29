// disk.c — registers; listens on m-port and c-port; handles "store" and "fetch"
#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

static int sock_m, sock_c;
static struct sockaddr_in mgr;
static char dname_glob[64];

static void ensure_dir(const char* path){
  mkdir(path, 0777);
}

void* listen_m(void* _) {
  char buf[2048]; struct sockaddr_in src; socklen_t sl=sizeof(src);
  for(;;){
    ssize_t n=recvfrom(sock_m, buf, sizeof(buf)-1, 0, (struct sockaddr*)&src, &sl);
    if(n<=0) continue;
    buf[n]=0;
    printf("[disk m-port %s] %s", dname_glob, buf); fflush(stdout);
  }
  return NULL;
}

static void send_block_reply(struct sockaddr_in* dst, socklen_t dl,
                             const char* dss, long stripe, int diskIndex, int isParity,
                             const unsigned char* data, int len)
{
  char header[256];
  int hlen = snprintf(header,sizeof(header),
                      "block|%s|%ld|%d|%d|%d\n", dss, stripe, diskIndex, isParity, len);
  unsigned char *pkt = malloc(hlen + len);
  memcpy(pkt, header, hlen);
  if(len>0) memcpy(pkt+hlen, data, len);
  sendto(sock_c, pkt, hlen+len, 0, (struct sockaddr*)dst, dl);
  free(pkt);
}

void* listen_c(void* _) {
  // "store|<dss>|<stripe>|<diskIndex>|<isParity>|<blockLen>\n<payload>"
  // "fetch|<dss>|<stripe>|<diskIndex>|<isParity>\n"
  unsigned char buf[65536];
  struct sockaddr_in src; socklen_t sl=sizeof(src);
  for(;;){
    ssize_t n=recvfrom(sock_c, buf, sizeof(buf), 0, (struct sockaddr*)&src, &sl);
    if(n<=0) continue;

    int hdr_end=-1;
    for(int i=0;i<n;i++){ if(buf[i]=='\n'){ hdr_end=i; break; } }
    if(hdr_end<0) continue;

    char header[2048]; memcpy(header, buf, hdr_end); header[hdr_end]=0;

    char *save=NULL;
    char *cmd=strtok_r((char*)header,"|",&save);
    if(!cmd) continue;

    if(!strcmp(cmd,"store")){
      char *dss   = strtok_r(NULL,"|",&save);
      char *sstr  = strtok_r(NULL,"|",&save);
      char *distr = strtok_r(NULL,"|",&save);
      char *pstr  = strtok_r(NULL,"|",&save);
      char *lstr  = strtok_r(NULL,"|",&save);
      if(!dss||!sstr||!distr||!pstr||!lstr) continue;

      long stripe    = atol(sstr);
      int diskIndex  = atoi(distr);
      int isParity   = atoi(pstr);
      int blen       = atoi(lstr);

      int payload_len = n - (hdr_end+1);
      if(payload_len < blen) continue;

      char base[512];  snprintf(base,sizeof(base),"diskdata_%s", dname_glob); ensure_dir(base);
      char dssdir[512]; snprintf(dssdir,sizeof(dssdir),"%s/%s", base, dss); ensure_dir(dssdir);

      char fname[1024];
      snprintf(fname,sizeof(fname),"%s/stripe%ld_%s_%d.bin",
               dssdir, stripe, isParity?"P":"D", diskIndex);

      FILE* f=fopen(fname,"wb");
      if(!f){ perror("fopen"); continue; }
      fwrite(buf+hdr_end+1, 1, blen, f);
      fclose(f);

      printf("[disk %s] stored %s (stripe %ld, idx %d, bytes %d) -> %s\n",
             dname_glob, dss, stripe, diskIndex, blen, fname);
      fflush(stdout);
    }
    else if(!strcmp(cmd,"fetch")){
      char *dss   = strtok_r(NULL,"|",&save);
      char *sstr  = strtok_r(NULL,"|",&save);
      char *distr = strtok_r(NULL,"|",&save);
      char *pstr  = strtok_r(NULL,"|",&save);
      if(!dss||!sstr||!distr||!pstr) continue;
      long stripe    = atol(sstr);
      int diskIndex  = atoi(distr);
      int isParity   = atoi(pstr);

      char base[512];  snprintf(base,sizeof(base),"diskdata_%s", dname_glob);
      char dssdir[512]; snprintf(dssdir,sizeof(dssdir),"%s/%s", base, dss);

      char fname[1024];
      snprintf(fname,sizeof(fname),"%s/stripe%ld_%s_%d.bin",
               dssdir, stripe, isParity?"P":"D", diskIndex);

      FILE* f = fopen(fname,"rb");
      if(!f){
        // file missing -> return empty block
        send_block_reply(&src, sl, dss, stripe, diskIndex, isParity, (unsigned char*)"", 0);
        continue;
      }
      fseek(f,0,SEEK_END); long sz=ftell(f); fseek(f,0,SEEK_SET);
      unsigned char *data = malloc(sz?sz:1);
      int got = fread(data,1,sz,f);
      fclose(f);

      send_block_reply(&src, sl, dss, stripe, diskIndex, isParity, data, got);
      free(data);
    }
  }
  return NULL;
}

int main(int argc, char** argv){
    if(argc!=6){
        fprintf(stderr,"usage: disk <disk-name> <manager-ip> <manager-port> <my-mport> <my-cport>\n");
        return 1;
    }
    const char* dname     = argv[1];
    const char* mgr_ip    = argv[2];      // manager IP (often 127.0.0.1 for local demo)
    int mgr_port          = atoi(argv[3]);
    int my_mport          = atoi(argv[4]);
    int my_cport          = atoi(argv[5]);

    // keep a copy of disk name for logs already used elsewhere
    strncpy(dname_glob, dname, sizeof(dname_glob)-1);
    dname_glob[sizeof(dname_glob)-1] = '\0';

    // create sockets
    sock_m = socket(AF_INET, SOCK_DGRAM, 0);
    sock_c = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock_m < 0 || sock_c < 0) { perror("socket"); return 1; }

    // help WSL/Windows fast re-bind
    int yes = 1;
    setsockopt(sock_m, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    setsockopt(sock_c, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    // bind local endpoints
    struct sockaddr_in me_m = {0}, me_c = {0};
    me_m.sin_family = AF_INET;  me_m.sin_port = htons(my_mport);
    me_c.sin_family = AF_INET;  me_c.sin_port = htons(my_cport);

    // For local demo (manager IP == 127.0.0.1), bind explicit to loopback.
    // Other fall back to ANY so it works on multi-host setups
    if (strcmp(mgr_ip, "127.0.0.1") == 0) {
        inet_pton(AF_INET, "127.0.0.1", &me_m.sin_addr);
        inet_pton(AF_INET, "127.0.0.1", &me_c.sin_addr);
    } else {
        me_m.sin_addr.s_addr = INADDR_ANY;
        me_c.sin_addr.s_addr = INADDR_ANY;
    }

    if (bind(sock_m, (struct sockaddr*)&me_m, sizeof(me_m)) < 0) { perror("bind m"); return 1; }
    if (bind(sock_c, (struct sockaddr*)&me_c, sizeof(me_c)) < 0) { perror("bind c"); return 1; }

    // manager address
    memset(&mgr, 0, sizeof(mgr));
    mgr.sin_family = AF_INET;
    mgr.sin_port   = htons(mgr_port);
    if (inet_pton(AF_INET, mgr_ip, &mgr.sin_addr) != 1) {
        fprintf(stderr, "bad manager ip: %s\n", mgr_ip);
        return 1;
    }

    // reg-disk
    char reg[256];
    snprintf(reg, sizeof(reg), "register-disk|%s|%s|%d|%d\n",
             dname, "127.0.0.1", my_mport, my_cport);
    sendto(sock_m, reg, strlen(reg), 0, (struct sockaddr*)&mgr, sizeof(mgr));

    // start listeners
    pthread_t tm, tc;
    pthread_create(&tm, NULL, listen_m, NULL);
    pthread_create(&tc, NULL, listen_c, NULL);
    pthread_join(tm, NULL);
    pthread_join(tc, NULL);
    return 0;
}
