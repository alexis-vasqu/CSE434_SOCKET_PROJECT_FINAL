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
#include <stdbool.h>
#include <unistd.h>
#include <time.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <arpa/inet.h>
#include <netinet/in.h>

#define BUFSZ      4096
#define NAME       64
#define IPSTR      64
#define MAX_USERS  64
#define MAX_DISKS  64

typedef struct {
    char name[NAME];
    char ip[IPSTR];
    int  mport;   // user/disk management port 
    int  cport;   // user/disk client/data port 
    int  used;    // 0=free slot, 1=in use (registered)
    // DSS assignment
    int  assigned_n;                // number of disks assigned (for users)
    char assigned_disks[NAME*MAX_DISKS]; // CSV of disk names (for users)
} User;

typedef enum { D_FREE = 0, D_IN_USE = 1 } DiskState;
typedef struct {
    char name[NAME];
    char ip[IPSTR];
    int  mport;
    int  cport;
    DiskState state;
    char assigned_to[NAME];  // user name if in use
    int  used;               // slot taken
} Disk;

// Global state
static User g_users[MAX_USERS];
static Disk g_disks[MAX_DISKS];

static void nobuf(void) {
    setvbuf(stdout, NULL, _IONBF, 0);
    setvbuf(stderr, NULL, _IONBF, 0);
}

static void trim(char *s) {
    // trim trail CR/LF/space
    size_t n = strlen(s);
    while (n && (s[n-1]=='\n' || s[n-1]=='\r' || isspace((unsigned char)s[n-1]))) {
        s[--n] = '\0';
    }
}

static void strtoupper(char *s) {
    for (; *s; ++s) *s = (char)toupper((unsigned char)*s);
}

// parse key=value (returns value or NULL)
static const char* kvget(const char *line, const char *key, char *out, size_t outsz) {
    const char *p = line;
    size_t klen = strlen(key);
    while ((p = strstr(p, key))) {
        if ((p==line || isspace((unsigned char)p[-1])) && p[klen]=='=') {
            p += klen + 1;
            size_t i=0;
            while (*p && !isspace((unsigned char)*p) && i+1<outsz) {
                out[i++] = *p++;
            }
            out[i] = '\0';
            return out;
        }
        ++p;
    }
    return NULL;
}

// reply helper: guarantee newline and logs
static void send_line(int sock, const struct sockaddr *dst, socklen_t dlen,
                      const char *line_fmt, ...) {
    char line[BUFSZ];
    va_list ap; va_start(ap, line_fmt);
    vsnprintf(line, sizeof(line), line_fmt, ap);
    va_end(ap);

    size_t len = strlen(line);
    int add_nl = (len==0 || line[len-1] != '\n');

    char out[BUFSZ];
    if (add_nl) {
        snprintf(out, sizeof(out), "%s\n", line);
    } else {
        strncpy(out, line, sizeof(out)-1);
        out[sizeof(out)-1] = '\0';
    }

    // send
    ssize_t m = sendto(sock, out, strlen(out), 0, dst, dlen);
    if (m < 0) perror("sendto");

    // log
    printf("[manager] SEND | %s", out);
}

static int user_index(const char *name) {
    for (int i=0;i<MAX_USERS;i++) {
        if (g_users[i].used && strcmp(g_users[i].name, name)==0) return i;
    }
    return -1;
}
static int disk_index(const char *name) {
    for (int i=0;i<MAX_DISKS;i++) {
        if (g_disks[i].used && strcmp(g_disks[i].name, name)==0) return i;
    }
    return -1;
}

static int add_user(const char *name, const char *ip, int mport, int cport) {
    if (user_index(name) >= 0) return -2; // exists
    for (int i=0;i<MAX_USERS;i++) {
        if (!g_users[i].used) {
            g_users[i].used = 1;
            strncpy(g_users[i].name, name, NAME-1);
            strncpy(g_users[i].ip, ip, IPSTR-1);
            g_users[i].mport = mport;
            g_users[i].cport = cport;
            g_users[i].assigned_n = 0;
            g_users[i].assigned_disks[0] = '\0';
            return 0;
        }
    }
    return -1; // full
}

static int add_disk(const char *name, const char *ip, int mport, int cport) {
    if (disk_index(name) >= 0) return -2; // exists
    for (int i=0;i<MAX_DISKS;i++) {
        if (!g_disks[i].used) {
            g_disks[i].used = 1;
            strncpy(g_disks[i].name, name, NAME-1);
            strncpy(g_disks[i].ip, ip, IPSTR-1);
            g_disks[i].mport = mport;
            g_disks[i].cport = cport;
            g_disks[i].state = D_FREE;
            g_disks[i].assigned_to[0] = '\0';
            return 0;
        }
    }
    return -1;
}

static void free_user(const char *name) {
    int ui = user_index(name);
    if (ui < 0) return;
    // free any disks that are assigned to this user
    if (g_users[ui].assigned_n > 0) {
        // scan all disks and free any that belonging to user
        for (int i=0;i<MAX_DISKS;i++) {
            if (g_disks[i].used &&
                g_disks[i].state == D_IN_USE &&
                strcmp(g_disks[i].assigned_to, name)==0) {
                g_disks[i].state = D_FREE;
                g_disks[i].assigned_to[0] = '\0';
            }
        }
    }
    memset(&g_users[ui], 0, sizeof(User));
}

static void free_disk(const char *name) {
    int di = disk_index(name);
    if (di < 0) return;
    // If part of DSS, mark free 
    g_disks[di].state = D_FREE;
    g_disks[di].assigned_to[0] = '\0';
    memset(&g_disks[di], 0, sizeof(Disk));
}

// Assign N free disks to user; returns CSV of names in out_csv
static int allocate_disks(const char *user, int n, char *out_csv, size_t out_sz) {
    int free_count = 0;
    for (int i=0;i<MAX_DISKS;i++) {
        if (g_disks[i].used && g_disks[i].state == D_FREE) free_count++;
    }
    if (free_count < n) return -free_count; // not enough

    out_csv[0] = '\0';
    int taken = 0;
    for (int i=0;i<MAX_DISKS && taken<n;i++) {
        if (g_disks[i].used && g_disks[i].state == D_FREE) {
            g_disks[i].state = D_IN_USE;
            strncpy(g_disks[i].assigned_to, user, NAME-1);
            if (taken) strncat(out_csv, ",", out_sz - strlen(out_csv) - 1);
            strncat(out_csv, g_disks[i].name, out_sz - strlen(out_csv) - 1);
            taken++;
        }
    }
    return taken; // should equal n
}

// Attempt parse an integer KV like "n=3" or token "3"
static int parse_int_token_or_kv(char **tokens, int ntok, const char *kvkey, int defval) {
        // Here: scan tokens for "n=3" or bare number
    for (int i=0;i<ntok;i++) {
        if (strchr(tokens[i], '=')) {
            char key[32], val[32];
            if (sscanf(tokens[i], "%31[^=]=%31s", key, val) == 2) {
                if (strcasecmp(key, kvkey)==0) return atoi(val);
            }
        } else {
            // bare number (ex, after "n")
            if (isdigit((unsigned char)tokens[i][0])) return atoi(tokens[i]);
        }
    }
    return defval;
}

int main(int argc, char **argv) {
    nobuf();

    if (argc != 2) {
        fprintf(stderr, "usage: manager <manager_listen_port>\n");
        return 1;
    }

    int port = atoi(argv[1]);
    if (port <= 0 || port > 65535) {
        fprintf(stderr, "invalid port: %s\n", argv[1]);
        return 1;
    }

    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) { perror("socket"); return 1; }

    int yes = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port        = htons((uint16_t)port);

    if (bind(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(sock);
        return 1;
    }

    printf("[manager] listening on UDP %d\n", port);

    // Main loop
    for (;;) {
        char buf[BUFSZ];
        struct sockaddr_in src;
        socklen_t slen = sizeof(src);

        ssize_t n = recvfrom(sock, buf, sizeof(buf)-1, 0, (struct sockaddr*)&src, &slen);
        if (n < 0) { perror("recvfrom"); continue; }
        buf[n] = '\0';
        trim(buf);

        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &src.sin_addr, ip, sizeof(ip));
        unsigned src_port = ntohs(src.sin_port);

        printf("[manager] RECV %s:%u | %s\n", ip, src_port, buf);

        // Make an uppercase copy to match commands
        char uline[BUFSZ];
        strncpy(uline, buf, sizeof(uline)-1);
        uline[sizeof(uline)-1] = '\0';

        // Tokenize and  keep og line for kvget.
        char linecpy[BUFSZ];
        strncpy(linecpy, buf, sizeof(linecpy)-1);
        linecpy[sizeof(linecpy)-1] = '\0';

        // Basic tokens
        char *tokens[64]; int ntok = 0;
        char *save = NULL;
        for (char *t = strtok_r(uline, " \t\r\n", &save); t && ntok < 64; t = strtok_r(NULL, " \t\r\n", &save)) {
            tokens[ntok++] = t;
        }
        if (ntok == 0) {
            send_line(sock, (struct sockaddr*)&src, slen, "ERR|empty");
            continue;
        }

        // Uppercase first 2 tokens for  matching
        char cmd0[64]="", cmd1[64]="";
        strncpy(cmd0, tokens[0], sizeof(cmd0)-1);
        strtoupper(cmd0);
        if (ntok >= 2) { strncpy(cmd1, tokens[1], sizeof(cmd1)-1); strtoupper(cmd1); }

        // REGISTER USER
        if ((strcmp(cmd0, "REGISTER")==0 && strcmp(cmd1, "USER")==0) ||
            (strcasecmp(tokens[0], "register-user")==0)) {

            // Accept positional and key=value:
            // Positional: REGISTER USER <name> <ip> <mport> <cport>
            char name[NAME]="", ipstr[IPSTR]="";
            char tmp[64];
            int mport=0, cport=0;

            if (kvget(linecpy, "name", name, sizeof(name))) {
                kvget(linecpy, "ip", ipstr, sizeof(ipstr));
                if (kvget(linecpy, "mport", tmp, sizeof(tmp))) mport = atoi(tmp);
                if (kvget(linecpy, "cport", tmp, sizeof(tmp))) cport = atoi(tmp);
            } else {
                // positional fallback
                // tokens: 0 REGISTERregister-user, 1 USER or (part register-user), 2 name, 3 ip, 4 mport, 5 cport
                int off = (strcmp(cmd0, "REGISTER")==0 ? 2 : 1);
                if (ntok >= off+4) {
                    strncpy(name, tokens[off], sizeof(name)-1);
                    strncpy(ipstr, tokens[off+1], sizeof(ipstr)-1);
                    mport = atoi(tokens[off+2]);
                    cport = atoi(tokens[off+3]);
                }
            }

            if (name[0]==0 || ipstr[0]==0 || mport<=0 || cport<=0) {
                send_line(sock, (struct sockaddr*)&src, slen, "ERR|bad-register-user");
                continue;
            }

            int rc = add_user(name, ipstr, mport, cport);
            if (rc == 0) {
                send_line(sock, (struct sockaddr*)&src, slen, "OK|USER_REGISTERED|user=%s", name);
                // plain OK for very simple clients
                send_line(sock, (struct sockaddr*)&src, slen, "OK");
            } else if (rc == -2) {
                send_line(sock, (struct sockaddr*)&src, slen, "ERR|user-exists|user=%s", name);
            } else {
                send_line(sock, (struct sockaddr*)&src, slen, "ERR|user-capacity");
            }
            continue;
        }

        // REGISTER DISK
        if ((strcmp(cmd0, "REGISTER")==0 && strcmp(cmd1, "DISK")==0) ||
            (strcasecmp(tokens[0], "register-disk")==0)) {

            char name[NAME]="", ipstr[IPSTR]="";
            char tmp[64];
            int mport=0, cport=0;

            if (kvget(linecpy, "name", name, sizeof(name))) {
                kvget(linecpy, "ip", ipstr, sizeof(ipstr));
                if (kvget(linecpy, "mport", tmp, sizeof(tmp))) mport = atoi(tmp);
                if (kvget(linecpy, "cport", tmp, sizeof(tmp))) cport = atoi(tmp);
            } else {
                int off = (strcmp(cmd0, "REGISTER")==0 ? 2 : 1);
                if (ntok >= off+4) {
                    strncpy(name, tokens[off], sizeof(name)-1);
                    strncpy(ipstr, tokens[off+1], sizeof(ipstr)-1);
                    mport = atoi(tokens[off+2]);
                    cport = atoi(tokens[off+3]);
                }
            }

            if (name[0]==0 || ipstr[0]==0 || mport<=0 || cport<=0) {
                send_line(sock, (struct sockaddr*)&src, slen, "ERR|bad-register-disk");
                continue;
            }

            int rc = add_disk(name, ipstr, mport, cport);
            if (rc == 0) {
                send_line(sock, (struct sockaddr*)&src, slen, "OK|DISK_REGISTERED|disk=%s", name);
                send_line(sock, (struct sockaddr*)&src, slen, "OK");
            } else if (rc == -2) {
                send_line(sock, (struct sockaddr*)&src, slen, "ERR|disk-exists|disk=%s", name);
            } else {
                send_line(sock, (struct sockaddr*)&src, slen, "ERR|disk-capacity");
            }
            continue;
        }

        // CONFIGURE DSS
        if ((strcmp(cmd0, "CONFIGURE")==0 && strcmp(cmd1, "DSS")==0) ||
            (strcasecmp(tokens[0], "configure-dss")==0)){ 

            // Find which user sent this (src address) OR accept explicit user=<name>
            // matching IP:port.
            char user_name[NAME] = "";
            // try kv first
            char tmp[64];
            if (!kvget(linecpy, "user", user_name, sizeof(user_name))) {
                // src ip:port matching registered user's mport or cport + ip
                for (int i=0;i<MAX_USERS;i++) {
                    if (!g_users[i].used) continue;
                    if (strcmp(g_users[i].ip, ip)==0 &&
                        (g_users[i].mport == (int)src_port || g_users[i].cport == (int)src_port)) {
                        strncpy(user_name, g_users[i].name, sizeof(user_name)-1);
                        break;
                    }
                }
            }
            if (user_name[0]==0) {
                // still not found—fallback to first active user
                for (int i=0;i<MAX_USERS;i++) if (g_users[i].used) { strncpy(user_name, g_users[i].name, sizeof(user_name)-1); break; }
            }
            if (user_name[0]==0) {
                send_line(sock, (struct sockaddr*)&src, slen, "ERR|no-user");
                continue;
            }

            // parse n= (req)
            int n_disks = 0;
            if (kvget(linecpy, "n", tmp, sizeof(tmp))) {
                n_disks = atoi(tmp);
            } else {
                // scan tokens for 'n=K' or a bare number at end
                n_disks = parse_int_token_or_kv(tokens, ntok, "n", 0);
            }
            if (n_disks <= 0) n_disks = 3; // for demo

            char disk_csv[NAME*MAX_DISKS];
            int rc = allocate_disks(user_name, n_disks, disk_csv, sizeof(disk_csv));
            if (rc == n_disks) {
                // store on user for teardown
                int ui = user_index(user_name);
                if (ui >= 0) {
                    g_users[ui].assigned_n = n_disks;
                    strncpy(g_users[ui].assigned_disks, disk_csv, sizeof(g_users[ui].assigned_disks)-1);
                }
                // OK (so clients that only print first token still show "OK")
                send_line(sock, (struct sockaddr*)&src, slen,
                          "OK|DSS_READY|user=%s|n=%d|disks=%s", user_name, n_disks, disk_csv);
                send_line(sock, (struct sockaddr*)&src, slen, "OK");
            } else {
                int have = -rc;
                send_line(sock, (struct sockaddr*)&src, slen,
                          "ERR|INSUFFICIENT_DISKS|need=%d|have=%d", n_disks, have);
            }
            continue;
        }

        // DEREGISTER USER
        if ((strcmp(cmd0, "DEREGISTER")==0 && strcmp(cmd1, "USER")==0) ||
            (strcasecmp(tokens[0], "deregister-user")==0)) {

            char name[NAME]="";
            if (!kvget(linecpy, "user", name, sizeof(name))) {
                int off = (strcmp(cmd0, "DEREGISTER")==0 ? 2 : 1);
                if (ntok >= off+1) strncpy(name, tokens[off], sizeof(name)-1);
            }
            if (name[0]==0) { send_line(sock, (struct sockaddr*)&src, slen, "ERR|bad-deregister-user"); continue; }

            if (user_index(name) < 0) {
                send_line(sock, (struct sockaddr*)&src, slen, "ERR|no-such-user|user=%s", name);
            } else {
                free_user(name);
                send_line(sock, (struct sockaddr*)&src, slen, "OK|USER_DEREGISTERED|user=%s", name);
                send_line(sock, (struct sockaddr*)&src, slen, "OK");
            }
            continue;
        }

        // DEREGISTER DISK
        if ((strcmp(cmd0, "DEREGISTER")==0 && strcmp(cmd1, "DISK")==0) ||
            (strcasecmp(tokens[0], "deregister-disk")==0)) {

            char name[NAME]="";
            if (!kvget(linecpy, "disk", name, sizeof(name))) {
                int off = (strcmp(cmd0, "DEREGISTER")==0 ? 2 : 1);
                if (ntok >= off+1) strncpy(name, tokens[off], sizeof(name)-1);
            }
            if (name[0]==0) { send_line(sock, (struct sockaddr*)&src, slen, "ERR|bad-deregister-disk"); continue; }

            if (disk_index(name) < 0) {
                send_line(sock, (struct sockaddr*)&src, slen, "ERR|no-such-disk|disk=%s", name);
            } else {
                free_disk(name);
                send_line(sock, (struct sockaddr*)&src, slen, "OK|DISK_DEREGISTERED|disk=%s", name);
                send_line(sock, (struct sockaddr*)&src, slen, "OK");
            }
            continue;
        }

        // Fallback
        send_line(sock, (struct sockaddr*)&src, slen, "ERR|unknown-cmd");
    }

    close(sock);
    return 0;
}
