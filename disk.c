// disk.c — DSS disk node (UDP). Stores blocks in memory and serves reads.
// Build: gcc -O2 -Wall -Wextra -pthread -o disk disk.c
// Run:   ./disk <disk-name> <manager-ip> <manager-port> <my-mport> <my-cport>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <pthread.h>

static void nobuf(void){ setvbuf(stdout,NULL,_IONBF,0); setvbuf(stderr,NULL,_IONBF,0); }

static int sock_m=-1,sock_c=-1;
static struct sockaddr_in mgr;
static char dname_glob[64]={0};

static void peer_str(const struct sockaddr_in *sa, char *out, size_t outsz){
    char ip[INET_ADDRSTRLEN]="?"; inet_ntop(AF_INET,&sa->sin_addr,ip,sizeof(ip)); unsigned p=ntohs(sa->sin_port); snprintf(out,outsz,"%s:%u",ip,p);
}

void* listen_m(void *arg){
    (void)arg; char buf[4096]; struct sockaddr_in src; socklen_t slen=sizeof(src);
    for(;;){
        ssize_t n=recvfrom(sock_m,buf,sizeof(buf)-1,0,(struct sockaddr*)&src,&slen);
        if(n<=0) continue; buf[n]='\0';
        char peer[64]; peer_str(&src,peer,sizeof(peer));
        printf("[disk %s] M %s | %s",dname_glob,peer,buf);
        if(buf[n-1]!='\n') printf("\n");
    }
    return NULL;
}

#define STORE_MAX 8192
static struct { int used; char key[128]; size_t len; unsigned char data[4096]; } store[STORE_MAX];

static int store_put(const char *key, const unsigned char *data, size_t len){
    for(int i=0;i<STORE_MAX;i++) if(store[i].used&&strcmp(store[i].key,key)==0){ store[i].len=len>sizeof(store[i].data)?sizeof(store[i].data):len; memcpy(store[i].data,data,store[i].len); return 0; }
    for(int i=0;i<STORE_MAX;i++) if(!store[i].used){ store[i].used=1; snprintf(store[i].key,sizeof(store[i].key),"%s",key); store[i].len=len>sizeof(store[i].data)?sizeof(store[i].data):len; memcpy(store[i].data,data,store[i].len); return 0; }
    return -1;
}
static int store_get(const char *key, unsigned char *out, size_t *len){
    for(int i=0;i<STORE_MAX;i++) if(store[i].used&&strcmp(store[i].key,key)==0){ size_t L=store[i].len; if(out&&len&&*len>=L){ memcpy(out,store[i].data,L); *len=L; return 0; } if(len) *len=L; return 0; }
    return -1;
}
static void store_clear(void){ memset(store,0,sizeof(store)); }

void* listen_c(void *arg){
    (void)arg; unsigned char buf[8192]; struct sockaddr_in src; socklen_t slen=sizeof(src);
    for(;;){
        ssize_t n=recvfrom(sock_c,buf,sizeof(buf)-1,0,(struct sockaddr*)&src,&slen);
        if(n<=0) continue; buf[n]='\0';
        if(!strncmp((char*)buf,"FAIL|",5)){ store_clear(); continue; }
        if(!strncmp((char*)buf,"WRITE|",6)){
            char dss[64]="",file[64]=""; long stripe=0,block=0,len=0;
            char *hdr=(char*)buf+6; char *nl=strchr(hdr,'\n'); if(!nl) continue; *nl='\0';
            sscanf(hdr,"dss=%63[^|]|file=%63[^|]|stripe=%ld|block=%ld|len=%ld",dss,file,&stripe,&block,&len);
            unsigned char *payload=(unsigned char*)(nl+1);
            if(len>0 && (size_t)len<=sizeof(buf)-(payload-buf)){ char key[128]; snprintf(key,sizeof(key),"%s|%s|%ld|%ld",dss,file,stripe,block); store_put(key,payload,(size_t)len); }
            continue;
        }
        if(!strncmp((char*)buf,"READ|",5)){
            char dss[64]="",file[64]=""; long stripe=0,block=0; char *hdr=(char*)buf+5;
            sscanf(hdr,"dss=%63[^|]|file=%63[^|]|stripe=%ld|block=%ld",dss,file,&stripe,&block);
            char key[128]; snprintf(key,sizeof(key),"%s|%s|%ld|%ld",dss,file,stripe,block);
            unsigned char out[4096]; size_t L=sizeof(out);
            if(store_get(key,out,&L)==0){
                unsigned char frame[4600]; int m=snprintf((char*)frame,sizeof(frame),"DATA|len=%zu\n",L);
                memcpy(frame+m,out,L); sendto(sock_c,frame,m+L,0,(struct sockaddr*)&src,sizeof(src));
            }else{
                const char *e="DATA|len=0\n"; sendto(sock_c,e,strlen(e),0,(struct sockaddr*)&src,sizeof(src));
            }
            continue;
        }
    }
    return NULL;
}

int main(int argc, char **argv){
    nobuf();
    if(argc!=6){ fprintf(stderr,"usage: disk <disk-name> <manager-ip> <manager-port> <my-mport> <my-cport>\n"); return 1; }
    const char *dname=argv[1]; const char *mgr_ip=argv[2]; int mgr_port=atoi(argv[3]); int my_mport=atoi(argv[4]); int my_cport=atoi(argv[5]);
    strncpy(dname_glob,dname,sizeof(dname_glob)-1);
    sock_m=socket(AF_INET,SOCK_DGRAM,0); sock_c=socket(AF_INET,SOCK_DGRAM,0); if(sock_m<0||sock_c<0){ perror("socket"); return 1; }
    int yes=1; setsockopt(sock_m,SOL_SOCKET,SO_REUSEADDR,&yes,sizeof(yes)); setsockopt(sock_c,SOL_SOCKET,SO_REUSEADDR,&yes,sizeof(yes));
    struct sockaddr_in me_m; memset(&me_m,0,sizeof(me_m)); me_m.sin_family=AF_INET; me_m.sin_port=htons(my_mport);
    struct sockaddr_in me_c; memset(&me_c,0,sizeof(me_c)); me_c.sin_family=AF_INET; me_c.sin_port=htons(my_cport);
    me_m.sin_addr.s_addr=htonl(INADDR_ANY); me_c.sin_addr.s_addr=htonl(INADDR_ANY);
    if(bind(sock_m,(struct sockaddr*)&me_m,sizeof(me_m))<0){ perror("bind m"); return 1; }
    if(bind(sock_c,(struct sockaddr*)&me_c,sizeof(me_c))<0){ perror("bind c"); return 1; }
    memset(&mgr,0,sizeof(mgr)); mgr.sin_family=AF_INET; mgr.sin_port=htons(mgr_port);
    if(inet_pton(AF_INET,mgr_ip,&mgr.sin_addr)!=1){ fprintf(stderr,"bad manager ip: %s\n",mgr_ip); return 1; }
    char my_ip[16]="127.0.0.1"; char reg[256];
    int n=snprintf(reg,sizeof(reg),"REGISTER DISK %s %s %d %d\n",dname,my_ip,my_mport,my_cport);
    if(n<0||n>=(int)sizeof(reg)){ fprintf(stderr,"register line too long\n"); return 1; }
    sendto(sock_m,reg,n,0,(struct sockaddr*)&mgr,sizeof(mgr));
    pthread_t tm,tc; if(pthread_create(&tm,NULL,listen_m,NULL)!=0){ perror("pthread_create"); return 1; } if(pthread_create(&tc,NULL,listen_c,NULL)!=0){ perror("pthread_create"); return 1; }
    pthread_detach(tm); pthread_detach(tc);
    char cmd[512];
    while(fgets(cmd,sizeof(cmd),stdin)){
        if(cmd[0]=='\n') continue;
        if(cmd[strlen(cmd)-1]!='\n'){ size_t r=sizeof(cmd)-strlen(cmd)-1; strncat(cmd,"\n",r>0?r:0); }
        sendto(sock_m,cmd,strlen(cmd),0,(struct sockaddr*)&mgr,sizeof(mgr));
    }
    return 0;
}
