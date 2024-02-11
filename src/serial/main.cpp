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
    string action; // includes all the read, write, delete, count, end
    string key;    // includes the keys for the actions that require it
    string val;    // includes values for write action
};
unordered_map<string, string> KV_Store;

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

string executeCommand(const Command &cmd)
{
    if (cmd.action == "COUNT")
    {
        return to_string(KV_Store.size()) + "\n";
    }
    else if (cmd.action == "READ")
    {
        auto it = KV_Store.find(cmd.key);
        if (it == KV_Store.end())
        {
            return "NULL\n";
        }
        else
        {
            return it->second + "\n";
        }
    }
    else if (cmd.action == "DELETE")
    {
        if (KV_Store.erase(cmd.key))
        {
            return "FIN\n";
        }
        else
        {
            return "NULL\n";
        }
    }
    else if (cmd.action == "WRITE")
    {
        KV_Store[cmd.key] = cmd.val;
        return "FIN\n";
    }
    else if (cmd.action == "END")
    {
        return "";
    }
    else
    {
        return cmd.action + "\n";
    }
}

void *handle_client(void *arg)
{
    int client_socket = *((int *)arg);
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

    while (!inputQueue.empty())
    {
        Command cmd = inputQueue.front();
        string response = executeCommand(cmd);
        n = write(client_socket, response.c_str(), response.size());
        if (n < 0)
        {
            perror("ERROR writing to socket \n");
            close(client_socket);
            pthread_exit(NULL);
        }
        inputQueue.pop();
    }

    close(client_socket);
    pthread_exit(NULL);
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

    while (true)
    {
        newsockfd = accept(sockfd, (struct sockaddr *)&cli_addr, (socklen_t *)&clilen);
        if (newsockfd < 0)
        {
            cerr << "ERROR on accept \n"
                 << endl;
            exit(1);
        }
        pthread_t thread_id;
        if (pthread_create(&thread_id, NULL, handle_client, (void *)&newsockfd) < 0)
        {
            cerr << "ERROR creating thread \n"
                 << endl;
            exit(1);
        }
    }

    close(sockfd);
    return 0;
}