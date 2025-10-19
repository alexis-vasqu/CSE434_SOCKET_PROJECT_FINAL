// user.c — simple UDP user client for the DSS milestone
// Build:   gcc -O2 -Wall -Wextra -o user user.c
// Run:     ./user <user-name> <manager-ip> <manager-port> <m-port> <c-port>
// threads: none (single-thread read replies and sends commands to manager)
// I/O: read w short SO_RCVTIMEO to drain bursts from manager reply

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <ctype.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>

static void nobuf(void){ setvbuf(stdout,NULL,_IONBF,0); setvbuf(stderr,NULL,_IONBF,0); }

static void recv_and_print_with_timeout(int sock, int millis){
    struct timeval tv; tv.tv_sec=millis/1000; tv.tv_usec=(millis%1000)*1000;
    setsockopt(sock,SOL_SOCKET,SO_RCVTIMEO,&tv,sizeof(tv));
    char buf[4096]; ssize_t n=recvfrom(sock,buf,sizeof(buf)-1,0,NULL,NULL);
    if(n>0){ buf[n]='\0'; fputs(buf,stdout); }
    tv.tv_sec=0; tv.tv_usec=0; setsockopt(sock,SOL_SOCKET,SO_RCVTIMEO,&tv,sizeof(tv));
}

int main(int argc, char **argv){
    nobuf();
    if(argc!=6){ fprintf(stderr,"usage: user <user-name> <manager-ip> <manager-port> <m-port> <c-port>\n"); return 1; }
    const char *uname=argv[1]; const char *mgr_ip=argv[2]; int mgr_port=atoi(argv[3]); int my_mport=atoi(argv[4]); int my_cport=atoi(argv[5]);

    int s=socket(AF_INET,SOCK_DGRAM,0); if(s<0){ perror("socket"); return 1; }
    int yes=1; setsockopt(s,SOL_SOCKET,SO_REUSEADDR,&yes,sizeof(yes));

    struct sockaddr_in me; memset(&me,0,sizeof(me)); me.sin_family=AF_INET; me.sin_addr.s_addr=htonl(INADDR_ANY); me.sin_port=htons(my_mport);
    if(bind(s,(struct sockaddr*)&me,sizeof(me))<0){ perror("bind"); return 1; }

    struct sockaddr_in mgr; memset(&mgr,0,sizeof(mgr)); mgr.sin_family=AF_INET; mgr.sin_port=htons(mgr_port);
    if(inet_pton(AF_INET,mgr_ip,&mgr.sin_addr)!=1){ fprintf(stderr,"bad manager ip: %s\n",mgr_ip); return 1; }

    char my_ip[16]="127.0.0.1"; char reg[256];
    snprintf(reg,sizeof(reg),"REGISTER USER %s %s %d %d\n",uname,my_ip,my_mport,my_cport);
    if(sendto(s,reg,strlen(reg),0,(struct sockaddr*)&mgr,sizeof(mgr))<0){ perror("sendto"); return 1; }
    recv_and_print_with_timeout(s,500);
    recv_and_print_with_timeout(s,500);

    char cmd[1024];
    while(fgets(cmd,sizeof(cmd),stdin)){
        if(cmd[0]=='\n') continue;
        if(cmd[strlen(cmd)-1]!='\n'){ size_t r=sizeof(cmd)-strlen(cmd)-1; if(r>0) strncat(cmd,"\n",r); }
        if(sendto(s,cmd,strlen(cmd),0,(struct sockaddr*)&mgr,sizeof(mgr))<0){ perror("sendto"); continue; }

        for(;;){
            struct timeval tv; tv.tv_sec=0; tv.tv_usec=300000;
            setsockopt(s,SOL_SOCKET,SO_RCVTIMEO,&tv,sizeof(tv));
            char buf[4096]; ssize_t n=recvfrom(s,buf,sizeof(buf)-1,0,NULL,NULL);
            if(n<=0) break;
            buf[n]='\0'; fputs(buf,stdout);
        }
    }
    return 0;
}
