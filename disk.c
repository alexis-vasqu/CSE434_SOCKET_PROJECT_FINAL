// disk.c — DSS milestone disk client (UDP, two sockets + listener threads + REPL)
// Build: gcc -O2 -Wall -Wextra -pthread -o disk disk.c
// Usage: ./disk <disk-name> <manager-ip> <manager-port> <my-mport> <my-cport>
//

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

static void nobuf(void){
    setvbuf(stdout, NULL, _IONBF, 0);
    setvbuf(stderr, NULL, _IONBF, 0);
}

// Globals
int sock_m = -1, sock_c = -1;          // manage and client sockets
struct sockaddr_in mgr;                 // manager address
char dname_glob[64] = {0};              // disk name for logs

// listener threads (print any datagrams the disk receives)
static void peer_str(const struct sockaddr_in *sa, char *out, size_t outsz) {
    char ip[INET_ADDRSTRLEN] = "?";
    inet_ntop(AF_INET, &sa->sin_addr, ip, sizeof(ip));
    unsigned p = ntohs(sa->sin_port);
    snprintf(out, outsz, "%s:%u", ip, p);
}

void* listen_m(void *arg) {
    (void)arg;
    char buf[2048];
    struct sockaddr_in src; socklen_t slen = sizeof(src);
    for (;;) {
        ssize_t n = recvfrom(sock_m, buf, sizeof(buf)-1, 0, (struct sockaddr*)&src, &slen);
        if (n <= 0) continue;
        buf[n] = '\0';
        char peer[64]; peer_str(&src, peer, sizeof(peer));
        printf("[disk %s] M RECV %s | %s", dname_glob, peer, buf);
        if (buf[n-1] != '\n') printf("\n");
    }
    return NULL;
}

void* listen_c(void *arg) {
    (void)arg;
    char buf[2048];
    struct sockaddr_in src; socklen_t slen = sizeof(src);
    for (;;) {
        ssize_t n = recvfrom(sock_c, buf, sizeof(buf)-1, 0, (struct sockaddr*)&src, &slen);
        if (n <= 0) continue;
        buf[n] = '\0';
        char peer[64]; peer_str(&src, peer, sizeof(peer));
        printf("[disk %s] C RECV %s | %s", dname_glob, peer, buf);
        if (buf[n-1] != '\n') printf("\n");
    }
    return NULL;
}

int main(int argc, char **argv){
    nobuf();

    if (argc != 6) {
        fprintf(stderr, "usage: disk <disk-name> <manager-ip> <manager-port> <my-mport> <my-cport>\n");
        return 1;
    }

    const char *dname    = argv[1];
    const char *mgr_ip   = argv[2];
    int         mgr_port = atoi(argv[3]);
    int         my_mport = atoi(argv[4]);
    int         my_cport = atoi(argv[5]);

    // names for logs
    strncpy(dname_glob, dname, sizeof(dname_glob)-1);

    // Create sockets
    sock_m = socket(AF_INET, SOCK_DGRAM, 0);
    sock_c = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock_m < 0 || sock_c < 0) { perror("socket"); return 1; }

    int yes = 1;
    setsockopt(sock_m, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    setsockopt(sock_c, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    // Bind endpoints
    struct sockaddr_in me_m; memset(&me_m, 0, sizeof(me_m));
    struct sockaddr_in me_c; memset(&me_c, 0, sizeof(me_c));
    me_m.sin_family = AF_INET;  me_m.sin_port = htons(my_mport);
    me_c.sin_family = AF_INET;  me_c.sin_port = htons(my_cport);

    // For single-host demos, bind loopback; for multi-host, bind ANY
    if (strcmp(mgr_ip, "127.0.0.1") == 0) {
        inet_pton(AF_INET, "127.0.0.1", &me_m.sin_addr);
        inet_pton(AF_INET, "127.0.0.1", &me_c.sin_addr);
    } else {
        me_m.sin_addr.s_addr = htonl(INADDR_ANY);
        me_c.sin_addr.s_addr = htonl(INADDR_ANY);
    }

    if (bind(sock_m, (struct sockaddr*)&me_m, sizeof(me_m)) < 0) { perror("bind m"); return 1; }
    if (bind(sock_c, (struct sockaddr*)&me_c, sizeof(me_c)) < 0) { perror("bind c"); return 1; }

    // Manager address
    memset(&mgr, 0, sizeof(mgr));
    mgr.sin_family = AF_INET;
    mgr.sin_port   = htons(mgr_port);
    if (inet_pton(AF_INET, mgr_ip, &mgr.sin_addr) != 1) {
        fprintf(stderr, "bad manager ip: %s\n", mgr_ip);
        return 1;
    }

    // Reg with manager
    char my_ip[16] = "127.0.0.1";
    char reg[256];
    int n = snprintf(reg, sizeof(reg), "REGISTER DISK %s %s %d %d\n",
                     dname, my_ip, my_mport, my_cport);
    if (n < 0 || n >= (int)sizeof(reg)) { fprintf(stderr, "register line too long\n"); return 1; }

    if (sendto(sock_m, reg, n, 0, (struct sockaddr*)&mgr, sizeof(mgr)) < 0) {
        perror("sendto register-disk");
    }

    // Start listener threads
    pthread_t tm, tc;
    if (pthread_create(&tm, NULL, listen_m, NULL) != 0) { perror("pthread_create listen_m"); return 1; }
    if (pthread_create(&tc, NULL, listen_c, NULL) != 0) { perror("pthread_create listen_c"); return 1; }
    pthread_detach(tm);
    pthread_detach(tc);

    // REPL: allow operator to send commands to manager
    char cmd[256];
    while (fgets(cmd, sizeof(cmd), stdin)) {
        if (cmd[0] == '\n') continue;
        if (cmd[strlen(cmd)-1] != '\n') strcat(cmd, "\n");   // ensure newline
        if (sendto(sock_m, cmd, strlen(cmd), 0, (struct sockaddr*)&mgr, sizeof(mgr)) < 0) {
            perror("sendto cmd");
        }
    }

    return 0;
}
