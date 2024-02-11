#include <stdlib.h>
#include <string>
#include <cstring>
#include <pthread.h>
#include <iostream>
#include <cstdlib>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <sstream>
#include <queue>
#include <utility>
#include <unordered_map>

using namespace std;

struct Command
{
  string action; // includes all the read, write, delete, cout, end
  string key;    // includes the keys for the actions that req it
  string val;    // includes vales for write action
};

unordered_map<string, string> KV_Store;
pthread_mutex_t KV_Store_mutex = PTHREAD_MUTEX_INITIALIZER;
queue<int> client_sockets;

pthread_mutex_t client_sockets_mutex = PTHREAD_MUTEX_INITIALIZER;

queue<Command> parseInput(istream &inputStream)
{
  queue<Command> commands;
  string line;
  while (getline(inputStream, line))
  {
    Command cmd;
    cmd.action = line;

    if (line == "READ" || line == "DELETE")
    {
      if (!getline(inputStream, cmd.key))
      {
        cerr << "Invalid " << line << " command: Missing key" << endl;
        continue;
      }
    }
    else if (line == "WRITE")
    {
      if (!getline(inputStream, cmd.key) || !getline(inputStream, cmd.val))
      {
        cerr << "Invalid WRITE command: Missing key or value" << endl;
        continue;
      }
      auto pos = cmd.val.find(':');
      if (pos != string::npos)
      {
        cmd.val.erase(pos, 1);
      }
    }

    commands.push(cmd);
  }
  return commands;
}

void executeInput(queue<Command> inputQueue, int client_socket)
{
  while (!inputQueue.empty())
  {
    Command curr = inputQueue.front();
    int n;
    string res_str;
    if (curr.action == "COUNT")
    {
      pthread_mutex_lock(&KV_Store_mutex);
      int res = KV_Store.size();
      pthread_mutex_unlock(&KV_Store_mutex);
      res_str = to_string(res) + "\n";
    }
    else if (curr.action == "READ")
    {
      pthread_mutex_lock(&KV_Store_mutex);
      if (KV_Store.find(curr.key) == KV_Store.end())
      {
        res_str = "NULL\n";
      }
      else
      {
        res_str = KV_Store[curr.key] + "\n";
      }
      pthread_mutex_unlock(&KV_Store_mutex);
    }
    else if (curr.action == "DELETE")
    {
      pthread_mutex_lock(&KV_Store_mutex);
      if (KV_Store.find(curr.key) == KV_Store.end())
      {
        res_str = "NULL\n";
      }
      else
      {
        KV_Store.erase(curr.key);
        res_str = "FIN\n";
      }
      pthread_mutex_unlock(&KV_Store_mutex);
    }
    else if (curr.action == "WRITE")
    {
      pthread_mutex_lock(&KV_Store_mutex);
      KV_Store[curr.key] = curr.val;
      pthread_mutex_unlock(&KV_Store_mutex);
      res_str = "FIN\n";
    }
    else if (curr.action == "END")
    {
      close(client_socket);
      return;
    }
    else
    {
      res_str = curr.action + "\n";
    }
    n = write(client_socket, res_str.c_str(), res_str.size());
    if (n < 0)
    {
      perror("ERROR writing to socket \n");
      close(client_socket);
      pthread_exit(NULL);
    }
    inputQueue.pop();
  }
}

void *worker_thread(void *arg) {
  while (true) {
    int client_socket;
    {
      
      pthread_mutex_lock(&client_sockets_mutex);
      if (client_sockets.empty()) {
        pthread_mutex_unlock(&client_sockets_mutex);
        continue;
      }
      client_socket = client_sockets.front();
      client_sockets.pop();
      pthread_mutex_unlock(&client_sockets_mutex);
    }
    
    char buffer[256];
    int n;
    bzero(buffer, 256);
    n = read(client_socket, buffer, 255);
    if (n < 0)
    {
      perror("ERROR reading from socket \n");
      close(client_socket);
      pthread_exit(NULL);
    }

    istringstream inputBuffer(buffer);
    queue<Command> inputQueue = parseInput(inputBuffer);

    executeInput(inputQueue, client_socket);

    if (n < 0)
    {
      perror("ERROR writing to socket \n");
      close(client_socket);
      pthread_exit(NULL);
    }
    close(client_socket);
  }
}

int main(int argc, char **argv)
{
  int portno;
  int sockfd, newsockfd, clilen;
  struct sockaddr_in serv_addr, cli_addr;

  if (argc != 2)
  {
    cerr << "Use the command as : " << argv[0] << " <port_no>" << endl;
    exit(1);
  }

  portno = atoi(argv[1]);

  sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0)
  {
    cerr << "ERROR opening socket \n"
         << endl;
    exit(1);
  }

  bzero((char *)&serv_addr, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = INADDR_ANY;
  serv_addr.sin_port = htons(portno);

  if (bind(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
  {
    cerr << "ERROR on binding \n"
         << endl;
    exit(1);
  }

  listen(sockfd, 5);
  clilen = sizeof(cli_addr);
  cerr << "Listening to port : " << portno << endl;

  pthread_t thread_pool[4];
  for (int i = 0; i < 4; i++) {
    pthread_create(&thread_pool[i], NULL, worker_thread, NULL);
  }

  while (true)
  {
    newsockfd = accept(sockfd, (struct sockaddr *)&cli_addr, (socklen_t *)&clilen);
    if (newsockfd < 0)
    {
      cerr << "ERROR on accept \n"
           << endl;
      exit(1);
    }
    
    pthread_mutex_lock(&client_sockets_mutex);
    client_sockets.push(newsockfd);
    pthread_mutex_unlock(&client_sockets_mutex);
  }

  close(sockfd);
  return 0;
}

// delete bin
// cd bin/ && rm serial_server && cd .. && rmdir bin/

// rewrite the main.cpp file
// rm main.cpp && nano main.cpp && g++ main.cpp

// to see all ports (for processid)
// sudo netstat -lnp

// to delete an active port (here 8080)
// sudo kill $(sudo lsof -t -i:8080)


// lsof -i :8080
// kill -9 PID