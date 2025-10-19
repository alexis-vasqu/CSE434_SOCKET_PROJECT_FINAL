// manager.c — CSE434 DSS milestone manager (UDP)
// Accepts both "REGISTER USER ..." and "register-user ..." styles.
/// Build:   gcc -O2 -Wall -Wextra -o manager manager.c
// Run:     ./manager <listen_port>

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <ctype.h>
#include <unistd.h>
#include <stdbool.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>

#define BUFSZ 4096
#define NAME 64
#define IPSTR 64
#define MAX_USERS 64
#define MAX_DISKS 64
#define MAX_DSS 16
#define MAX_FILES 512

typedef struct {
    char name[NAME];
    char ip[IPSTR];
    int mport;
    int cport;
    int used;
    int assigned_n;
    char assigned_disks[NAME*MAX_DISKS];
} User;

typedef enum { D_FREE = 0, D_IN_USE = 1 } DiskState;
typedef struct {
    char name[NAME];
    char ip[IPSTR];
    int mport;
    int cport;
    DiskState state;
    char assigned_to[NAME];
    int used;
} Disk;

typedef struct {
    char fname[NAME];
    long long fsize;
    char owner[NAME];
} FileMeta;

typedef struct {
    int used;
    char name[NAME];
    int n;
    int striping_unit;
    char disk_name[MAX_DISKS][NAME];
    FileMeta files[MAX_FILES];
    int files_used;
    int copy_in_progress;
    int read_in_progress;
} DSS;

static User g_users[MAX_USERS];
static Disk g_disks[MAX_DISKS];
static DSS g_dss[MAX_DSS];

static void nobuf(void){ setvbuf(stdout,NULL,_IONBF,0); setvbuf(stderr,NULL,_IONBF,0); }

static void trim(char *s){ size_t n=strlen(s); while(n&&(s[n-1]=='\n'||s[n-1]=='\r'||isspace((unsigned char)s[n-1]))) s[--n]='\0'; }

static void strtoupper(char *s){ for(;*s;++s)*s=(char)toupper((unsigned char)*s); }

static const char* kvget(const char *line, const char *key, char *out, size_t outsz){
    const char *p=line; size_t klen=strlen(key);
    while((p=strstr(p,key))){
        if((p==line||isspace((unsigned char)p[-1]))&&p[klen]=='='){
            p+=klen+1; size_t i=0;
            while(*p&&!isspace((unsigned char)*p)&&i+1<outsz) out[i++]=*p++;
            out[i]='\0'; return out;
        }
        ++p;
    }
    return NULL;
}

static void send_line(int sock, const struct sockaddr *dst, socklen_t dlen, const char *fmt, ...){
    char line[BUFSZ]; va_list ap; va_start(ap,fmt); vsnprintf(line,sizeof(line),fmt,ap); va_end(ap);
    size_t len=strlen(line); int add_nl=(len==0||line[len-1]!='\n');
    char out[BUFSZ];
    if(add_nl) snprintf(out,sizeof(out),"%s\n",line); else { strncpy(out,line,sizeof(out)-1); out[sizeof(out)-1]='\0'; }
    sendto(sock,out,strlen(out),0,dst,dlen);
    printf("%s",out);
}

static int user_index(const char *name){ for(int i=0;i<MAX_USERS;i++) if(g_users[i].used&&strcmp(g_users[i].name,name)==0) return i; return -1; }
static int disk_index(const char *name){ for(int i=0;i<MAX_DISKS;i++) if(g_disks[i].used&&strcmp(g_disks[i].name,name)==0) return i; return -1; }
static int add_user(const char *name, const char *ip, int mport, int cport){
    if(user_index(name)>=0) return -2;
    for(int i=0;i<MAX_USERS;i++) if(!g_users[i].used){
        g_users[i].used=1;
        snprintf(g_users[i].name,NAME,"%s",name);
        snprintf(g_users[i].ip,IPSTR,"%s",ip);
        g_users[i].mport=mport; g_users[i].cport=cport;
        g_users[i].assigned_n=0; g_users[i].assigned_disks[0]='\0';
        return 0;
    }
    return -1;
}
static int add_disk(const char *name, const char *ip, int mport, int cport){
    if(disk_index(name)>=0) return -2;
    for(int i=0;i<MAX_DISKS;i++) if(!g_disks[i].used){
        g_disks[i].used=1;
        snprintf(g_disks[i].name,NAME,"%s",name);
        snprintf(g_disks[i].ip,IPSTR,"%s",ip);
        g_disks[i].mport=mport; g_disks[i].cport=cport;
        g_disks[i].state=D_FREE; g_disks[i].assigned_to[0]='\0';
        return 0;
    }
    return -1;
}
static void free_user(const char *name){
    int ui=user_index(name); if(ui<0) return;
    for(int i=0;i<MAX_DISKS;i++) if(g_disks[i].used&&g_disks[i].state==D_IN_USE&&strcmp(g_disks[i].assigned_to,name)==0){ g_disks[i].state=D_FREE; g_disks[i].assigned_to[0]='\0'; }
    memset(&g_users[ui],0,sizeof(User));
}
static void free_disk(const char *name){
    int di=disk_index(name); if(di<0) return;
    g_disks[di].state=D_FREE; g_disks[di].assigned_to[0]='\0';
    memset(&g_disks[di],0,sizeof(Disk));
}
static int allocate_disks(const char *user, int n, char *out_csv, size_t out_sz){
    int free_count=0; for(int i=0;i<MAX_DISKS;i++) if(g_disks[i].used&&g_disks[i].state==D_FREE) free_count++;
    if(free_count<n) return -free_count;
    out_csv[0]='\0'; int taken=0;
    for(int i=0;i<MAX_DISKS&&taken<n;i++) if(g_disks[i].used&&g_disks[i].state==D_FREE){
        g_disks[i].state=D_IN_USE; snprintf(g_disks[i].assigned_to,NAME,"%s",user);
        if(taken) strncat(out_csv,",",out_sz-strlen(out_csv)-1);
        strncat(out_csv,g_disks[i].name,out_sz-strlen(out_csv)-1);
        taken++;
    }
    return taken;
}
static int parse_int_token_or_kv(char **tokens, int ntok, const char *kvkey, int defval){
    for(int i=0;i<ntok;i++){
        if(strchr(tokens[i],'=')){ char k[32],v[32]; if(sscanf(tokens[i],"%31[^=]=%31s",k,v)==2){ if(strcasecmp(k,kvkey)==0) return atoi(v); } }
        else if(isdigit((unsigned char)tokens[i][0])) return atoi(tokens[i]);
    }
    return defval;
}
static int dss_index(const char *name){ for(int i=0;i<MAX_DSS;i++) if(g_dss[i].used&&strcmp(g_dss[i].name,name)==0) return i; return -1; }
static int dss_new_slot(void){ for(int i=0;i<MAX_DSS;i++) if(!g_dss[i].used) return i; return -1; }
static int dss_file_index(DSS *d, const char *fname){ for(int i=0;i<d->files_used;i++) if(strcmp(d->files[i].fname,fname)==0) return i; return -1; }
static int is_power_of_two(int x){ return x>0 && (x&(x-1))==0; }

int main(int argc, char **argv){
    nobuf();
    if(argc!=2){ fprintf(stderr,"usage: manager <manager_listen_port>\n"); return 1; }
    int port=atoi(argv[1]); if(port<=0||port>65535){ fprintf(stderr,"invalid port: %s\n",argv[1]); return 1; }

    int sock=socket(AF_INET,SOCK_DGRAM,0); if(sock<0){ perror("socket"); return 1; }
    int yes=1; setsockopt(sock,SOL_SOCKET,SO_REUSEADDR,&yes,sizeof(yes));
    struct sockaddr_in addr; memset(&addr,0,sizeof(addr));
    addr.sin_family=AF_INET; addr.sin_addr.s_addr=htonl(INADDR_ANY); addr.sin_port=htons((uint16_t)port);
    if(bind(sock,(struct sockaddr*)&addr,sizeof(addr))<0){ perror("bind"); close(sock); return 1; }

    for(;;){
        char buf[BUFSZ]; struct sockaddr_in src; socklen_t slen=sizeof(src);
        ssize_t n=recvfrom(sock,buf,sizeof(buf)-1,0,(struct sockaddr*)&src,&slen);
        if(n<0){ perror("recvfrom"); continue; }
        buf[n]='\0'; trim(buf);

        char ip[INET_ADDRSTRLEN]; inet_ntop(AF_INET,&src.sin_addr,ip,sizeof(ip));
        unsigned src_port=ntohs(src.sin_port);
        printf("%s:%u | %s\n",ip,src_port,buf);

        char uline[BUFSZ]; strncpy(uline,buf,sizeof(uline)-1); uline[sizeof(uline)-1]='\0';
        char linecpy[BUFSZ]; strncpy(linecpy,buf,sizeof(linecpy)-1); linecpy[sizeof(linecpy)-1]='\0';

        char *tokens[64]; int ntok=0; char *save=NULL;
        for(char *t=strtok_r(uline," \t\r\n",&save); t&&ntok<64; t=strtok_r(NULL," \t\r\n",&save)) tokens[ntok++]=t;
        if(ntok==0){ send_line(sock,(struct sockaddr*)&src,slen,"FAILURE"); continue; }

        char cmd0[64]="",cmd1[64]="";
        strncpy(cmd0,tokens[0],sizeof(cmd0)-1); strtoupper(cmd0);
        if(ntok>=2){ strncpy(cmd1,tokens[1],sizeof(cmd1)-1); strtoupper(cmd1); }

        if((strcmp(cmd0,"REGISTER")==0&&strcmp(cmd1,"USER")==0)||strcasecmp(tokens[0],"register-user")==0){
            char name[NAME]="",ipstr[IPSTR]=""; char tmp[64]; int mport=0,cport=0;
            if(kvget(linecpy,"name",name,sizeof(name))){
                kvget(linecpy,"ip",ipstr,sizeof(ipstr));
                if(kvget(linecpy,"mport",tmp,sizeof(tmp))) mport=atoi(tmp);
                if(kvget(linecpy,"cport",tmp,sizeof(tmp))) cport=atoi(tmp);
            }else{
                int off=(strcmp(cmd0,"REGISTER")==0?2:1);
                if(ntok>=off+4){ snprintf(name,sizeof(name),"%s",tokens[off]); snprintf(ipstr,sizeof(ipstr),"%s",tokens[off+1]); mport=atoi(tokens[off+2]); cport=atoi(tokens[off+3]); }
            }
            if(!name[0]||!ipstr[0]||mport<=0||cport<=0){ send_line(sock,(struct sockaddr*)&src,slen,"FAILURE"); continue; }
            int rc=add_user(name,ipstr,mport,cport);
            if(rc==0){ send_line(sock,(struct sockaddr*)&src,slen,"SUCCESS"); }
            else if(rc==-2){ send_line(sock,(struct sockaddr*)&src,slen,"FAILURE"); }
            else{ send_line(sock,(struct sockaddr*)&src,slen,"FAILURE"); }
            continue;
        }

        if((strcmp(cmd0,"REGISTER")==0&&strcmp(cmd1,"DISK")==0)||strcasecmp(tokens[0],"register-disk")==0){
            char name[NAME]="",ipstr[IPSTR]=""; char tmp[64]; int mport=0,cport=0;
            if(kvget(linecpy,"name",name,sizeof(name))){
                kvget(linecpy,"ip",ipstr,sizeof(ipstr));
                if(kvget(linecpy,"mport",tmp,sizeof(tmp))) mport=atoi(tmp);
                if(kvget(linecpy,"cport",tmp,sizeof(tmp))) cport=atoi(tmp);
            }else{
                int off=(strcmp(cmd0,"REGISTER")==0?2:1);
                if(ntok>=off+4){ snprintf(name,sizeof(name),"%s",tokens[off]); snprintf(ipstr,sizeof(ipstr),"%s",tokens[off+1]); mport=atoi(tokens[off+2]); cport=atoi(tokens[off+3]); }
            }
            if(!name[0]||!ipstr[0]||mport<=0||cport<=0){ send_line(sock,(struct sockaddr*)&src,slen,"FAILURE"); continue; }
            int rc=add_disk(name,ipstr,mport,cport);
            if(rc==0){ send_line(sock,(struct sockaddr*)&src,slen,"SUCCESS"); }
            else if(rc==-2){ send_line(sock,(struct sockaddr*)&src,slen,"FAILURE"); }
            else{ send_line(sock,(struct sockaddr*)&src,slen,"FAILURE"); }
            continue;
        }

        if((strcmp(cmd0,"CONFIGURE")==0&&strcmp(cmd1,"DSS")==0)||strcasecmp(tokens[0],"configure-dss")==0){
            char dss_name[NAME]=""; char tn[32]="",tsu[32]="";
            if(!kvget(linecpy,"dss",dss_name,sizeof(dss_name))){
                int off=(strcmp(cmd0,"CONFIGURE")==0?2:1);
                if(ntok>=off+3){ snprintf(dss_name,sizeof(dss_name),"%s",tokens[off]); snprintf(tn,sizeof(tn),"%s",tokens[off+1]); snprintf(tsu,sizeof(tsu),"%s",tokens[off+2]); }
            }else{
                kvget(linecpy,"n",tn,sizeof(tn));
                kvget(linecpy,"strip",tsu,sizeof(tsu));
            }
            int nreq=atoi(tn), su=atoi(tsu);
            if(!dss_name[0]||nreq<3||!is_power_of_two(su)||su<128||su>1048576||dss_index(dss_name)>=0){ send_line(sock,(struct sockaddr*)&src,slen,"FAILURE"); continue; }
            int free_count=0; for(int i=0;i<MAX_DISKS;i++) if(g_disks[i].used&&g_disks[i].state==D_FREE) free_count++;
            if(free_count<nreq){ send_line(sock,(struct sockaddr*)&src,slen,"FAILURE"); continue; }
            int di=dss_new_slot(); if(di<0){ send_line(sock,(struct sockaddr*)&src,slen,"FAILURE"); continue; }
            g_dss[di].used=1; snprintf(g_dss[di].name,NAME,"%s",dss_name); g_dss[di].n=nreq; g_dss[di].striping_unit=su; g_dss[di].files_used=0; g_dss[di].copy_in_progress=0; g_dss[di].read_in_progress=0;
            int taken=0;
            for(int i=0;i<MAX_DISKS&&taken<nreq;i++) if(g_disks[i].used&&g_disks[i].state==D_FREE){
                snprintf(g_dss[di].disk_name[taken],NAME,"%s",g_disks[i].name);
                g_disks[i].state=D_IN_USE; snprintf(g_disks[i].assigned_to,NAME,"%s",dss_name);
                taken++;
            }
            if(taken!=nreq){ send_line(sock,(struct sockaddr*)&src,slen,"FAILURE"); continue; }
            send_line(sock,(struct sockaddr*)&src,slen,"SUCCESS");
            continue;
        }

        if(strcasecmp(tokens[0],"ls")==0){
            int have=0;
            for(int i=0;i<MAX_DSS;i++){
                if(!g_dss[i].used) continue; have=1;
                char order[NAME*MAX_DISKS]="";
                for(int k=0;k<g_dss[i].n;k++){ if(k) strncat(order,",",sizeof(order)-strlen(order)-1); strncat(order,g_dss[i].disk_name[k],sizeof(order)-strlen(order)-1); }
                send_line(sock,(struct sockaddr*)&src,slen,"SUCCESS|DSS=%s|n=%d|su=%d|order=%s",g_dss[i].name,g_dss[i].n,g_dss[i].striping_unit,order);
                for(int f=0;f<g_dss[i].files_used;f++) send_line(sock,(struct sockaddr*)&src,slen,"FILE|dss=%s|name=%s|size=%lld|owner=%s",g_dss[i].name,g_dss[i].files[f].fname,g_dss[i].files[f].fsize,g_dss[i].files[f].owner);
            }
            if(!have) send_line(sock,(struct sockaddr*)&src,slen,"FAILURE");
            continue;
        }


if (strcasecmp(tokens[0],"copy")==0){
    char fname[NAME]="", fsize[32]="", owner[NAME]="", dss_name[NAME]="";
    kvget(linecpy,"file",fname,sizeof(fname));
    kvget(linecpy,"size",fsize,sizeof(fsize));
    kvget(linecpy,"owner",owner,sizeof(owner));
    kvget(linecpy,"dss",dss_name,sizeof(dss_name));

    int di = -1;
    if (dss_name[0]) di = dss_index(dss_name);
    if (di < 0) { for (int i=0;i<MAX_DSS;i++) if (g_dss[i].used) { di = i; break; } }

    if (di < 0 || !fname[0] || !owner[0]) { send_line(sock,(struct sockaddr*)&src,slen,"FAILURE"); continue; }
    if (g_dss[di].copy_in_progress) { send_line(sock,(struct sockaddr*)&src,slen,"FAILURE"); continue; }
    g_dss[di].copy_in_progress = 1;

    for (int k=0;k<g_dss[di].n;k++){
        int dd = disk_index(g_dss[di].disk_name[k]);
        if (dd>=0) send_line(sock,(struct sockaddr*)&src,slen,"DISK|%d|%s|%s|%d",k,g_disks[dd].name,g_disks[dd].ip,g_disks[dd].cport);
    }
    send_line(sock,(struct sockaddr*)&src,slen,"SUCCESS|DSS=%s|n=%d|su=%d|file=%s|size=%s",
              g_dss[di].name,g_dss[di].n,g_dss[di].striping_unit,fname,fsize);
    continue;
}




        if(strcasecmp(tokens[0],"copy-complete")==0){
            char dss_name[NAME]="",fname[NAME]="",owner[NAME]="",fsize[32]="";
            kvget(linecpy,"dss",dss_name,sizeof(dss_name));
            kvget(linecpy,"file",fname,sizeof(fname));
            kvget(linecpy,"owner",owner,sizeof(owner));
            kvget(linecpy,"size",fsize,sizeof(fsize));
            int di=dss_index(dss_name);
            if(di<0||!fname[0]||!owner[0]){ send_line(sock,(struct sockaddr*)&src,slen,"FAILURE"); continue; }
            if(g_dss[di].files_used>=MAX_FILES){ send_line(sock,(struct sockaddr*)&src,slen,"FAILURE"); continue; }
            int fi=g_dss[di].files_used++;
            snprintf(g_dss[di].files[fi].fname,NAME,"%s",fname);
            snprintf(g_dss[di].files[fi].owner,NAME,"%s",owner);
            g_dss[di].files[fi].fsize=atoll(fsize);
            g_dss[di].copy_in_progress=0;
            send_line(sock,(struct sockaddr*)&src,slen,"SUCCESS");
            continue;
        }

        if(strcasecmp(tokens[0],"read")==0){
            char dss_name[NAME]="",fname[NAME]="",uname[NAME]="";
            kvget(linecpy,"dss",dss_name,sizeof(dss_name));
            kvget(linecpy,"file",fname,sizeof(fname));
            kvget(linecpy,"user",uname,sizeof(uname));
            int di=dss_index(dss_name); if(di<0){ send_line(sock,(struct sockaddr*)&src,slen,"FAILURE"); continue; }
            int fi=dss_file_index(&g_dss[di],fname);
            if(fi<0||strcmp(g_dss[di].files[fi].owner,uname)!=0){ send_line(sock,(struct sockaddr*)&src,slen,"FAILURE"); continue; }
            if(g_dss[di].copy_in_progress){ send_line(sock,(struct sockaddr*)&src,slen,"FAILURE"); continue; }
            g_dss[di].read_in_progress++;
            for(int k=0;k<g_dss[di].n;k++){
                int dd=disk_index(g_dss[di].disk_name[k]);
                if(dd>=0) send_line(sock,(struct sockaddr*)&src,slen,"DISK|%d|%s|%s|%d",k,g_disks[dd].name,g_disks[dd].ip,g_disks[dd].cport);
            }
            send_line(sock,(struct sockaddr*)&src,slen,"SUCCESS|DSS=%s|n=%d|su=%d|file=%s|size=%lld",g_dss[di].name,g_dss[di].n,g_dss[di].striping_unit,g_dss[di].files[fi].fname,g_dss[di].files[fi].fsize);
            continue;
        }

        if(strcasecmp(tokens[0],"read-complete")==0){
            char dss_name[NAME]=""; kvget(linecpy,"dss",dss_name,sizeof(dss_name));
            int di=dss_index(dss_name); if(di>=0&&g_dss[di].read_in_progress>0) g_dss[di].read_in_progress--;
            send_line(sock,(struct sockaddr*)&src,slen,"SUCCESS");
            continue;
        }

        if(strcasecmp(tokens[0],"disk-failure")==0){
            char dss_name[NAME]=""; kvget(linecpy,"dss",dss_name,sizeof(dss_name));
            int di=dss_index(dss_name);
            if(di<0||g_dss[di].read_in_progress>0||g_dss[di].copy_in_progress){ send_line(sock,(struct sockaddr*)&src,slen,"FAILURE"); continue; }
            for(int k=0;k<g_dss[di].n;k++){
                int dd=disk_index(g_dss[di].disk_name[k]);
                if(dd>=0) send_line(sock,(struct sockaddr*)&src,slen,"DISK|%d|%s|%s|%d",k,g_disks[dd].name,g_disks[dd].ip,g_disks[dd].cport);
            }
            send_line(sock,(struct sockaddr*)&src,slen,"SUCCESS|DSS=%s|n=%d|su=%d",g_dss[di].name,g_dss[di].n,g_dss[di].striping_unit);
            continue;
        }

        if(strcasecmp(tokens[0],"recovery-complete")==0){
            send_line(sock,(struct sockaddr*)&src,slen,"SUCCESS");
            continue;
        }

        if((strcmp(cmd0,"DEREGISTER")==0&&strcmp(cmd1,"USER")==0)||strcasecmp(tokens[0],"deregister-user")==0){
            char name[NAME]="";
            if(!kvget(linecpy,"user",name,sizeof(name))){
                int off=(strcmp(cmd0,"DEREGISTER")==0?2:1);
                if(ntok>=off+1) snprintf(name,sizeof(name),"%s",tokens[off]);
            }
            if(!name[0]){ send_line(sock,(struct sockaddr*)&src,slen,"FAILURE"); continue; }
            if(user_index(name)<0){ send_line(sock,(struct sockaddr*)&src,slen,"FAILURE"); }
            else{ free_user(name); send_line(sock,(struct sockaddr*)&src,slen,"SUCCESS"); }
            continue;
        }

        if((strcmp(cmd0,"DEREGISTER")==0&&strcmp(cmd1,"DISK")==0)||strcasecmp(tokens[0],"deregister-disk")==0){
            char name[NAME]="";
            if(!kvget(linecpy,"disk",name,sizeof(name))){
                int off=(strcmp(cmd0,"DEREGISTER")==0?2:1);
                if(ntok>=off+1) snprintf(name,sizeof(name),"%s",tokens[off]);
            }
            int di=disk_index(name);
            if(di<0){ send_line(sock,(struct sockaddr*)&src,slen,"FAILURE"); continue; }
            if(g_disks[di].state==D_IN_USE){ send_line(sock,(struct sockaddr*)&src,slen,"FAILURE"); continue; }
            free_disk(name); send_line(sock,(struct sockaddr*)&src,slen,"SUCCESS");
            continue;
        }

        if(strcasecmp(tokens[0],"decommission-dss")==0){
            char dss_name[NAME]=""; kvget(linecpy,"dss",dss_name,sizeof(dss_name));
            int di=dss_index(dss_name); if(di<0){ send_line(sock,(struct sockaddr*)&src,slen,"FAILURE"); continue; }
            if(g_dss[di].copy_in_progress||g_dss[di].read_in_progress>0){ send_line(sock,(struct sockaddr*)&src,slen,"FAILURE"); continue; }
            for(int k=0;k<g_dss[di].n;k++){
                int dd=disk_index(g_dss[di].disk_name[k]);
                if(dd>=0){ g_disks[dd].state=D_FREE; g_disks[dd].assigned_to[0]='\0'; }
            }
            memset(&g_dss[di],0,sizeof(DSS));
            send_line(sock,(struct sockaddr*)&src,slen,"SUCCESS");
            continue;
        }

        send_line(sock,(struct sockaddr*)&src,slen,"FAILURE");
    }
    return 0;
}

