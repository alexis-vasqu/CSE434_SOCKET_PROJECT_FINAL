// user.c — simple UDP user client for the DSS milestone
// Build:   gcc -O2 -Wall -Wextra -o user user.c
// Run:     ./user <user-name> <manager-ip> <manager-port> <m-port> <c-port>

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

static void nobuf(void) {
    setvbuf(stdout, NULL, _IONBF, 0);
    setvbuf(stderr, NULL, _IONBF, 0);
}

// recv one reply with a small timeout and print it if present
static void recv_and_print_with_timeout(int sock, int millis) {
    struct timeval tv;
    tv.tv_sec  = millis / 1000;
    tv.tv_usec = (millis % 1000) * 1000;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    char buf[2048];
    ssize_t n = recvfrom(sock, buf, sizeof(buf) - 1, 0, NULL, NULL);
    if (n > 0) {
        buf[n] = '\0';
        printf("%s", buf);            // print the entire line
    }

    // restore blocking 
    tv.tv_sec = 0; tv.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
}

int main(int argc, char **argv) {
    nobuf();

    if (argc != 6) {
        fprintf(stderr, "usage: user <user-name> <manager-ip> <manager-port> <m-port> <c-port>\n");
        return 1;
    }

    const char *uname    = argv[1];
    const char *mgr_ip   = argv[2];
    int         mgr_port = atoi(argv[3]);
    int         my_mport = atoi(argv[4]);
    int         my_cport = atoi(argv[5]);

    // socket + bind to mport (so manager can identify w/ src port)
    int s = socket(AF_INET, SOCK_DGRAM, 0);
    if (s < 0) { perror("socket"); return 1; }

    int yes = 1;
    setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    struct sockaddr_in me = {0};
    me.sin_family      = AF_INET;
    me.sin_addr.s_addr = htonl(INADDR_ANY);     // bind on all IFs
    me.sin_port        = htons(my_mport);
    if (bind(s, (struct sockaddr*)&me, sizeof(me)) < 0) {
        perror("bind");
        return 1;
    }

    // manager address
    struct sockaddr_in mgr = {0};
    mgr.sin_family = AF_INET;
    mgr.sin_port   = htons(mgr_port);
    if (inet_pton(AF_INET, mgr_ip, &mgr.sin_addr) != 1) {
        fprintf(stderr, "bad manager ip: %s\n", mgr_ip);
        return 1;
    }

    // REGISTER USER-
    char my_ip[16] = "127.0.0.1";
    char reg[256];
    snprintf(reg, sizeof(reg), "REGISTER USER %s %s %d %d\n",
             uname, my_ip, my_mport, my_cport);

    if (sendto(s, reg, strlen(reg), 0, (struct sockaddr*)&mgr, sizeof(mgr)) < 0) {
        perror("sendto register-user");
        return 1;
    }

    // print manager's reg reply
    recv_and_print_with_timeout(s, 500);
    recv_and_print_with_timeout(s, 500);

    // REPL: read commands, send to manager, print reply
    //   CONFIGURE DSS n=3 policy=raid0 block=4096
    //   DEREGISTER USER U1
    char cmd[512];
    while (fgets(cmd, sizeof(cmd), stdin)) {
        if (cmd[0] == '\n') continue;
        if (cmd[strlen(cmd)-1] != '\n') strcat(cmd, "\n");  // ensure newline

        if (sendto(s, cmd, strlen(cmd), 0, (struct sockaddr*)&mgr, sizeof(mgr)) < 0) {
            perror("sendto cmd");
            continue;
        }

        // print at least one reply
        recv_and_print_with_timeout(s, 1200);
        recv_and_print_with_timeout(s, 200);
    }

    return 0;
}
