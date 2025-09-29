// manager.c — milestone + get-dss for copy
#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

#define BUFSZ 4096
#define NAME  32
#define MAX_USERS 64
#define MAX_DISKS 64
#define MAX_DSS   16

typedef enum { FREE=0, INDSS=1 } DiskState;

typedef struct {
  char name[NAME], ip[64];
  int mport, cport;
  int used;
} User;

typedef struct {
  char name[NAME], ip[64];
  int mport, cport;
  DiskState state;   // FREE or INDSS
  int used;
} Disk;

typedef struct {
  char name[NAME];
  int n;          // number of disks
  int striping;   // bytes
  char disk_names[16][NAME]; // ordered members
  int used;
} DSS;

static User users[MAX_USERS];
static Disk disks[MAX_DISKS];
static DSS  dss[MAX_DSS];

static int is_pow2(int x){ return x && !(x & (x-1)); }

static void send_reply(int sock, const char* msg, struct sockaddr_in* cli) {
  sendto(sock, msg, strlen(msg), 0, (struct sockaddr*)cli, sizeof(*cli));
}

static int find_free_user_slot(){ for(int i=0;i<MAX_USERS;i++) if(!users[i].used) return i; return -1; }
static int find_free_disk_slot(){ for(int i=0;i<MAX_DISKS;i++) if(!disks[i].used) return i; return -1; }
static int find_free_dss_slot() { for(int i=0;i<MAX_DSS;i++) if(!dss[i].used)   return i; return -1; }

static int name_exists_user(const char* n){ for(int i=0;i<MAX_USERS;i++) if(users[i].used && !strcmp(users[i].name,n)) return 1; return 0; }
static int name_exists_disk(const char* n){ for(int i=0;i<MAX_DISKS;i++) if(disks[i].used && !strcmp(disks[i].name,n)) return 1; return 0; }
static int dss_exists(const char* n){ for(int i=0;i<MAX_DSS;i++) if(dss[i].used && !strcmp(dss[i].name,n)) return 1; return 0; }

static int port_in_use(int p){
  for(int i=0;i<MAX_USERS;i++) if(users[i].used && (users[i].mport==p || users[i].cport==p)) return 1;
  for(int i=0;i<MAX_DISKS;i++) if(disks[i].used && (disks[i].mport==p || disks[i].cport==p)) return 1;
  return 0;
}

static DSS* get_dss(const char* name){
  for(int i=0;i<MAX_DSS;i++) if(dss[i].used && !strcmp(dss[i].name,name)) return &dss[i];
  return NULL;
}
static Disk* get_disk_by_name(const char* name){
  for(int i=0;i<MAX_DISKS;i++) if(disks[i].used && !strcmp(disks[i].name,name)) return &disks[i];
  return NULL;
}

int main(int argc, char** argv){
  if(argc!=2){
    fprintf(stderr,"usage: manager <manager_listen_port>\n");
    return 1;
  }
  int port = atoi(argv[1]);
  int sock = socket(AF_INET, SOCK_DGRAM, 0);
  if(sock<0){ perror("socket"); return 1; }

  struct sockaddr_in me={0};
  me.sin_family = AF_INET;
  me.sin_addr.s_addr = INADDR_ANY;
  me.sin_port = htons(port);
  if(bind(sock,(struct sockaddr*)&me,sizeof(me))<0){ perror("bind"); return 1; }

  srand((unsigned)time(NULL));
  printf("[manager] listening on UDP %d\n", port);

  char buf[BUFSZ];
  for(;;){
    struct sockaddr_in cli; socklen_t cl=sizeof(cli);
    ssize_t n = recvfrom(sock, buf, sizeof(buf)-1, 0, (struct sockaddr*)&cli, &cl);
    if(n<=0) continue;
    buf[n]=0;

    char *saveptr=NULL;
    char *cmd = strtok_r(buf, "|\n", &saveptr);
    if(!cmd) continue;

    if(!strcmp(cmd,"register-user")){
      char *name = strtok_r(NULL,"|\n",&saveptr);
      char *ip   = strtok_r(NULL,"|\n",&saveptr);
      char *mp   = strtok_r(NULL,"|\n",&saveptr);
      char *cp   = strtok_r(NULL,"|\n",&saveptr);
      if(!name||!ip||!mp||!cp){ send_reply(sock,"ERR|bad-args\n",&cli); continue; }
      int mport = atoi(mp), cport = atoi(cp);
      if(name_exists_user(name) || port_in_use(mport) || port_in_use(cport)){ send_reply(sock,"ERR|duplicate\n",&cli); continue; }
      int slot=find_free_user_slot(); if(slot<0){ send_reply(sock,"ERR|capacity\n",&cli); continue; }
      users[slot]=(User){0}; users[slot].used=1;
      strncpy(users[slot].name,name,NAME-1);
      strncpy(users[slot].ip,ip,63);
      users[slot].mport=mport; users[slot].cport=cport;
      send_reply(sock,"OK|\n",&cli);
    }
    else if(!strcmp(cmd,"register-disk")){
      char *name = strtok_r(NULL,"|\n",&saveptr);
      char *ip   = strtok_r(NULL,"|\n",&saveptr);
      char *mp   = strtok_r(NULL,"|\n",&saveptr);
      char *cp   = strtok_r(NULL,"|\n",&saveptr);
      if(!name||!ip||!mp||!cp){ send_reply(sock,"ERR|bad-args\n",&cli); continue; }
      int mport = atoi(mp), cport = atoi(cp);
      if(name_exists_disk(name) || port_in_use(mport) || port_in_use(cport)){ send_reply(sock,"ERR|duplicate\n",&cli); continue; }
      int slot=find_free_disk_slot(); if(slot<0){ send_reply(sock,"ERR|capacity\n",&cli); continue; }
      disks[slot]=(Disk){0}; disks[slot].used=1; disks[slot].state=FREE;
      strncpy(disks[slot].name,name,NAME-1);
      strncpy(disks[slot].ip,ip,63);
      disks[slot].mport=mport; disks[slot].cport=cport;
      send_reply(sock,"OK|\n",&cli);
    }
    else if(!strcmp(cmd,"configure-dss")){
      char *dname = strtok_r(NULL,"|\n",&saveptr);
      char *nstr  = strtok_r(NULL,"|\n",&saveptr);
      char *bstr  = strtok_r(NULL,"|\n",&saveptr);
      if(!dname||!nstr||!bstr){ send_reply(sock,"ERR|bad-args\n",&cli); continue; }
      int n_disks = atoi(nstr);
      int strip   = atoi(bstr);
      if(n_disks < 3 || n_disks > 16 || !is_pow2(strip) || strip < 128 || strip > 1024*1024){ send_reply(sock,"ERR|params\n",&cli); continue; }
      if(dss_exists(dname)){ send_reply(sock,"ERR|exists\n",&cli); continue; }
      int freeIdx[MAX_DISKS], cnt=0;
      for(int i=0;i<MAX_DISKS;i++) if(disks[i].used && disks[i].state==FREE) freeIdx[cnt++]=i;
      if(cnt < n_disks){ send_reply(sock,"ERR|insufficient-disks\n",&cli); continue; }
      int dslot = find_free_dss_slot(); if(dslot<0){ send_reply(sock,"ERR|dss-capacity\n",&cli); continue; }
      dss[dslot]=(DSS){0}; dss[dslot].used=1; dss[dslot].n=n_disks; dss[dslot].striping=strip;
      strncpy(dss[dslot].name, dname, NAME-1);
      for(int k=0;k<n_disks;k++){
        int di = freeIdx[k];
        disks[di].state = INDSS;
        strncpy(dss[dslot].disk_names[k], disks[di].name, NAME-1);
      }
      char out[BUFSZ];
      snprintf(out,sizeof(out),"OK|%s|%d|%d|", dname, n_disks, strip);
      for(int k=0;k<n_disks;k++){
        Disk *dp=get_disk_by_name(dss[dslot].disk_names[k]);
        char chunk[128];
        snprintf(chunk,sizeof(chunk),"%s,%s,%d%s", dp->name, dp->ip, dp->cport, (k==n_disks-1)?"":";");
        strncat(out,chunk,sizeof(out)-strlen(out)-1);
      }
      strncat(out,"\n",sizeof(out)-strlen(out)-1);
      send_reply(sock,out,&cli);
    }
    else if(!strcmp(cmd,"get-dss")){
      char *dname = strtok_r(NULL,"|\n",&saveptr);
      DSS *ds = get_dss(dname);
      if(!ds){ send_reply(sock,"ERR|no-such-dss\n",&cli); continue; }
      char out[BUFSZ];
      snprintf(out,sizeof(out),"OK|%s|%d|%d|", ds->name, ds->n, ds->striping);
      for(int k=0;k<ds->n;k++){
        Disk *dp=get_disk_by_name(ds->disk_names[k]);
        char chunk[128];
        snprintf(chunk,sizeof(chunk),"%s,%s,%d%s", dp->name, dp->ip, dp->cport, (k==ds->n-1)?"":";");
        strncat(out,chunk,sizeof(out)-strlen(out)-1);
      }
      strncat(out,"\n",sizeof(out)-strlen(out)-1);
      send_reply(sock,out,&cli);
    }
    else if(!strcmp(cmd,"deregister-user")){
      char *name = strtok_r(NULL,"|\n",&saveptr);
      int ok=0; if(name){ for(int i=0;i<MAX_USERS;i++) if(users[i].used && !strcmp(users[i].name,name)){ users[i].used=0; ok=1; } }
      send_reply(sock, ok?"OK|\n":"ERR|no-user\n", &cli);
    }
    else if(!strcmp(cmd,"deregister-disk")){
      char *name = strtok_r(NULL,"|\n",&saveptr);
      int ok=0; if(name){ for(int i=0;i<MAX_DISKS;i++) if(disks[i].used && !strcmp(disks[i].name,name) && disks[i].state==FREE){ disks[i].used=0; ok=1; } }
      send_reply(sock, ok?"OK|\n":"ERR|in-dss-or-missing\n", &cli);
    }
    else {
      send_reply(sock,"ERR|unknown-cmd\n",&cli);
    }
  }
}
