#include <unistd.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <string.h>
#include <iostream>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <map>
#include <queue>
#include <thread>
#include <arpa/inet.h>
#include <cassert>
#include <time.h>
#include <signal.h>
#include <errno.h>
using namespace std;

#define RDSIZE (1 * 1024  * 1024)

#define LISTEN_PORT  443
#define BACKEND_PORT 80
#define CHECK_TIMEOUT -1
#define CONN_TIMEOUT 500 * 1000 // us

#define UP 0
#define DOWN 1
#define MPROC // use multiple worker processes to accept connections
#define NPROC 1

bool g_log = false;

#define cout if(g_log) cout // close log output
#define printf ;if(g_log) printf // close log output
#define fprintf ;if(g_log) fprintf // close log output

void set_addr(sockaddr_in& addr, const char* ipaddr, int port)
{
  bzero(&addr, sizeof(addr));
  addr.sin_family = AF_INET;
  inet_pton(AF_INET, ipaddr, &addr.sin_addr);
  addr.sin_port = htons(port);
}

void set_nonblock(int fd)
{
  int flags = fcntl(fd, F_GETFL);
  flags |= O_NONBLOCK;
  fcntl(fd, F_SETFL, flags);
}

void start_accept(struct sockaddr_in& cliaddr, socklen_t& cliaddrlen)
{
  bzero(&cliaddr, sizeof(cliaddr));
  cliaddrlen = 1;
}

int start_listen()
{
  int listenfd = socket(AF_INET, SOCK_STREAM, 0);
  assert(listenfd != -1);
  int one = 1;
  setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  set_nonblock(listenfd);
  int port = LISTEN_PORT;
  const char* ip = "127.0.0.1";
  if( port == 80 || port == 443 ) ip = "0.0.0.0";
  struct sockaddr_in  servaddr;
  bzero(&servaddr, sizeof(servaddr));
  servaddr.sin_family = AF_INET;
  servaddr.sin_port = htons(port);
  inet_pton(AF_INET, ip, &servaddr.sin_addr);

  assert(!bind(listenfd, (struct sockaddr*)&servaddr, sizeof(servaddr)));
  assert(!listen(listenfd, 128));

  return listenfd;
}

int epoll_unregister(int epfd, int fd)
{
  struct epoll_event ev;

  int ret = epoll_ctl(epfd, EPOLL_CTL_DEL, fd, &ev);
  //if( ret )
  //  perror("epoll_ctl del deleted fd");
}

int epoll_update_existing(int epfd, int fd, int flags)
{
  struct epoll_event ev;
  ev.data.fd = fd;
  ev.events  = flags | EPOLLET;

  int ret = epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
  if( ret )
  {
    assert( errno != EBADF );
    //if( errno == ENOENT ) ; // do nothing if fd not/un registered in epoll
  }

  return 0;
}

int epoll_register(int epfd, int fd, int flags)
{
  struct epoll_event ev;
  ev.data.fd = fd;
  ev.events  = flags | EPOLLET;

  int ret = epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);
  if( ret )
  {
    if( errno == EBADF )
    {
      epoll_unregister(epfd, fd);
      return -1;
    }
    else if ( errno == EEXIST )
       ret = epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);

    if( ret )
    {
      perror("epoll_ctl add error");
      return ret;
    }
  }

  return 0;
}

void logonoff_debug(int signo)
{
  g_log = !g_log;
  cerr << string(40, '=') << " log turned " << (g_log?"on":"off") << " "  << string(40, '=') << endl;
}

  // for up-down data switching
  map<int, int> updown;
  map<int, int> udmark;

void shownum_debug(int signo)
{
  cerr << string(40, '=')
       << " updown.size = " << updown.size()
       << " udmark.size = " << udmark.size()
  << " "  << string(40, '=') << endl;

  for(auto& item : udmark)
  {
    cerr << "#" << item.first << ", peer = " << updown[item.first] << ", updown = " << item.second << endl;
  }
}

int main()
{
  signal(SIGABRT, logonoff_debug);
  signal(SIGFPE,  shownum_debug);
  signal(SIGPIPE, SIG_IGN);
  map<int, queue<pair<string, int>>> wq;

  // for connect
  map<int, int> connecting;
  map<int, time_t> connecting_time;

  string _rbuf; _rbuf.resize(RDSIZE);
  char* rbuf = (char*)_rbuf.data();

  int listenfd = start_listen();

#ifdef MPROC
  int cid = 1;
  int is_parent = 1;
  for(int i = 0; i < NPROC - 1; i++)
    if( ( is_parent = fork() ) == 0 ) { cid += i; break;}
  int pid = getpid();
#endif

  int epfd = epoll_create(1024);
  struct epoll_event events[1024];

  epoll_register(epfd, listenfd, EPOLLIN);

  // for accept
  struct sockaddr_in cliaddr;
  socklen_t cliaddrlen;
  start_accept(cliaddr, cliaddrlen);

  cerr <<  "TCP proxy server started" << endl;
  while(true)
  {
    int n = epoll_wait(epfd, events, 1024, CHECK_TIMEOUT);
    time_t now; time(&now);
    //if ( n ) fprintf(stderr, "\n\n--epoll-- n = %d\n", n);
    cout << "======= epoll_wait returned " << n << endl;
    for(int i = 0; i < n; i++)
    {
      if (events[i].events & ~(EPOLLHUP | EPOLLERR | EPOLLIN | EPOLLOUT) )
      {
        cerr << "events = " << events[i].events << endl;
      }
      // if peer is closed abnormally
      if( (events[i].events & EPOLLHUP) || (events[i].events & EPOLLERR) )
      {
        error_cleanup:
        int f = events[i].data.fd;

        // connecting upstream failed, close downstream and upstream
        if(connecting.find(f) != connecting.end())
        {
          epoll_unregister(epfd, f);
          int downfd = connecting[f]; connecting.erase(f);

          cout << "close downfd #" << downfd << " and failed connecting #" << f << endl;
          close(downfd);
          close(f);
          continue;
        }

        // close corresponding upstream connections
        if( updown.find(f) != updown.end() )
        {
          int up = updown[f];

        if( connecting.find(up) != connecting.end() )
          connecting.erase(up);

        if( connecting_time.find(up) != connecting_time.end() )
          connecting_time.erase(up);

          wq.erase(up);
          updown.erase(up);
          udmark.erase(up);

          epoll_unregister(epfd, up);
          close(up);
        }

        if( connecting.find(f) != connecting.end() )
          connecting.erase(f);

        if( connecting_time.find(f) != connecting_time.end() )
          connecting_time.erase(f);

        wq.erase(f);
        updown.erase(f);
        udmark.erase(f);

        epoll_unregister(epfd, f);
        close(f);

        continue; // no later processings like conn/in/out
      } // if error happens on fd, clean local status

      // accept a connection fd
      if( (events[i].events & EPOLLIN) && listenfd == events[i].data.fd )
      {
        int downfd = accept(listenfd, (struct sockaddr*)&cliaddr, &cliaddrlen);
        if(downfd == -1)
        {
          assert( errno == EAGAIN || errno == ECONNABORTED || errno == EINTR );

          fprintf(stderr, "accept error on process #%d, errno=%d, errmsg=%s\n", cid, errno, strerror(errno));
          continue;
        }

        printf("accept by #%d\n", events[i].data.fd);
#ifdef MPROC
        if( NPROC > 1 ) cout << "process #" << cid << ", pid = " << pid << " ";
        cout << "accepts a connection #" << downfd << endl;
#endif
        assert(downfd != -1);
        cout << "connected downfd = " << downfd << endl;
        set_nonblock(downfd);

        // create upstream fd to connect to upstream, add to connecting
        int upfd = socket(AF_INET, SOCK_STREAM, 0);
        assert(upfd != -1);
        set_nonblock(upfd);

        // async connect upstream host
        struct sockaddr_in addr;
        set_addr(addr, "127.0.0.1", BACKEND_PORT);
        int ret = connect(upfd, (struct sockaddr*)&addr, sizeof(addr));
        if( ret == -1 && errno == EADDRNOTAVAIL )
        {
          //fprintf(stderr, "connect target host port number unavailable for proxied client: %s:%d\n", inet_ntoa(cliaddr.sin_addr), cliaddr.sin_port);
          close(upfd);   // close failed connector to target host
          close(downfd); // intentionally not close proxied client, it'll wait for timeout

          continue;
        }
        assert( ret == -1 && errno == EINPROGRESS );

        // remember downstream fd
        connecting[upfd] = downfd;

        // record connecting start time (for checking connect timeout)
        time_t ts; time(&ts);
        connecting_time[upfd] = ts;

        // register upfd to async connect state
        epoll_register(epfd, upfd, EPOLLOUT);
      } // accept fd

      // read incoming-data fd
      if( (events[i].events & EPOLLIN) && listenfd != events[i].data.fd )
      {
        int f = events[i].data.fd;
        if( updown.find(f) == updown.end() )
        {
          //cerr << "read on cleanup fd #" << f << endl;
          continue;
        }
        assert(listenfd != f);

        int nbytes = read(f, rbuf, RDSIZE);
        printf("read on #%d, nbytes = %d, events = %d\n", events[i].data.fd, nbytes, events[i].events);

        if( nbytes < 0 )
        {
          if( errno == EBADF )
            goto error_cleanup;

          assert( errno == EAGAIN );
          goto in_over;
        }

        // append new write request to write-queue
        assert( nbytes >= 0 );
#ifdef MPROC
        //fprintf(stderr, "process #%d, pid = %d reads %d bytes data\n", cid, pid, nbytes);
#endif
        assert( updown.find(f) != updown.end() );
        int otherfd = updown[f];

        wq[otherfd].push(make_pair(string(rbuf, nbytes), 0));
        epoll_register(epfd, otherfd, EPOLLIN | EPOLLOUT );

#if 0
        cout << "put into wq #" << otherfd <<  " nbytes = " << nbytes << endl;
        cout << string(80, 'v') << endl;
        cout << string(rbuf, nbytes) << endl;
        cout << string(80, '^') << endl;
#endif
      } // read fd
      in_over:

      // connect upstream ok or ng
      if( (events[i].events & EPOLLOUT) &&
          connecting.find(events[i].data.fd) != connecting.end() )
      {
        printf("connect req notified on upstream #%d\n", events[i].data.fd);
        // upstream
        int upfd = events[i].data.fd;
        epoll_unregister(epfd, upfd);

        // downstream
        int downfd = connecting[upfd];
        connecting.erase(upfd);

        // connect start time
        time_t conn_time = connecting_time[upfd];
        connecting_time.erase(upfd);

        /// connect ng or timeout
        if( events[i].events != EPOLLOUT
            || difftime(now, conn_time) * 1000000 > CONN_TIMEOUT )
        {
          close(upfd);
          close(downfd);
          continue;
        }
        //printf("conn diff = %lf\n", difftime(now, conn_time));

        cout << "upstream #" << upfd << " connected" << endl;
        cout << "upstream #" << upfd << " connect notify events = "<< events[i].events << endl;
        cout << "start io between #" << upfd << " and #" << downfd << endl;
        // register both up and down fds to epoll for reading but NOT writing
        epoll_register(epfd, upfd,   EPOLLIN);
        epoll_register(epfd, downfd, EPOLLIN);

        // put both fds to data switch
        updown[upfd]   = downfd; udmark[upfd]   = UP;
        updown[downfd] = upfd;   udmark[downfd] = DOWN;
        // finish connecting, continue to avoid being treated like a write fd
        // because EPOLLOUT is set and is removed from connecting, making it to be mistakenly treated like a ready-to-write fd
        continue;
      } // connect ok or ng

      // write wq data to ready-to-write outgoing fd
      if( (events[i].events & EPOLLOUT) &&
          connecting.find(events[i].data.fd) == connecting.end() )
      {
        assert( connecting.find(events[i].data.fd) == connecting.end() );

        int f = events[i].data.fd;
        if( updown.find(f) == updown.end() )
        {
          //cerr << "write on cleanup fd #" << f << endl;
          continue;
        }
        auto& q = wq[f];
        printf("write notified on #%d, wq.size = %d\n", events[i].data.fd, wq[f].size());
        max_throughput:
        if( !q.empty() )
        {
          auto& dic = q.front();
          if( dic.first.length() == 0 )
          {
            assert(updown.find(f) != updown.end());
            cout << "fd #" << f << " meets write end, close it and peer #" << updown[f] << endl;

            goto error_cleanup;
          }

          int wbytes = dic.first.length() - dic.second;
          // apply cap to write len
          //if(wbytes > 1) wbytes = 1;

          int nbytes = write(f, dic.first.data() + dic.second, wbytes);
          printf("write data on #%d, nbytes = %d\n", events[i].data.fd, nbytes);
          if( nbytes <= 0 )
          {
            if(errno == EPIPE || errno == EBADF || errno == ECONNRESET )
            {
              fprintf(stderr, "write failed on #%d, events=%ld\n", f, events[i].events);
              perror("write failed");
              goto error_cleanup;
            }
            assert( errno == EINTR || errno == EAGAIN );
            goto out_over; // write failed, goto next epoll_wait loop
          }
          assert( nbytes > 0 );

          cout << "write contents to #" << f << ":" << endl;
          cout << string(80, 'v') << endl;
          cout << string(dic.first.data() + dic.second, nbytes) << endl;
          cout << string(80, '^') << endl;

          // remove empty write req after increment of offset
          if( (dic.second += nbytes) == dic.first.length() ) q.pop();

          goto max_throughput;  // loop write untill EAGAIN errno is set
        }
        else
        {
          // fd's write queue is empty, remove f's write notification
          printf("write queue empty for #%d, close write notification\n", f);
          epoll_update_existing( epfd, f, EPOLLIN );
        }
      } // EPOLLOUT
      out_over:
      ;
    } // epoll_wait
  }

  // wait for child processes
  if( is_parent )
    while( wait(NULL) != -1 );

  return 0;
}
