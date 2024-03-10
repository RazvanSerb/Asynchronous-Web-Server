// SPDX-License-Identifier: BSD-3-Clause

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/sendfile.h>
#include <sys/eventfd.h>
#include <libaio.h>
#include <errno.h>

#include "aws.h"
#include "utils/util.h"
#include "utils/debug.h"
#include "utils/sock_util.h"
#include "utils/w_epoll.h"

/* server socket file descriptor */
static int listenfd;

/* epoll file descriptor */
static int epollfd;

static io_context_t ctx;

static int aws_on_path_cb(http_parser *p, const char *buf, size_t len)
{
	struct connection *conn = (struct connection *)p->data;

	memcpy(conn->request_path, buf, len);
	conn->request_path[len] = '\0';
	conn->have_path = 1;

	return 0;
}

static void connection_prepare_send_reply_header(struct connection *conn)
{
	/* TODO: Prepare the connection buffer to send the reply header. */
	conn->send_len = snprintf(conn->send_buffer, sizeof(conn->send_buffer),
							"HTTP/1.1 200 OK\r\n"
							"Server: Apache/2.2.9\r\n"
							"Accept-Ranges: bytes\r\n"
							"Content-Length: %ld\r\n"
							"Vary: Accept-Encoding\r\n"
							"Connection: close\r\n"
							"Content-Type: text/html\r\n"
							"\r\n", conn->file_size);
}

static void connection_prepare_send_404(struct connection *conn)
{
	/* TODO: Prepare the connection buffer to send the 404 header. */
	memset(conn->send_buffer, 0, BUFSIZ);
	conn->send_len = snprintf(conn->send_buffer, sizeof(conn->send_buffer),
							"HTTP/1.0 404 Not Found\r\n"
							"Content-Type: text/html\r\n"
							"Connection: close\r\n"
							"\r\n"
							"<html><body><h1>404 Not Found</h1></body></html>\n");
}

static enum resource_type connection_get_resource_type(struct connection *conn)
{
	/* TODO: Get resource type depending on request path/filename. Filename should
	 * point to the static or dynamic folder.
	 */
	if (strstr(conn->request_path, "static"))
		return RESOURCE_TYPE_STATIC;
	if (strstr(conn->request_path, "dynamic"))
		return RESOURCE_TYPE_DYNAMIC;
	return RESOURCE_TYPE_NONE;
}


struct connection *connection_create(int sockfd)
{
	/* TODO: Initialize connection structure on given socket. */
	struct connection *conn;

	conn = malloc(sizeof(*conn));
	DIE(conn == NULL, "malloc");
	conn->state = STATE_INITIAL;
	conn->fd = -1;
	conn->eventfd = eventfd(0, EFD_NONBLOCK);
	conn->sockfd = sockfd;
	conn->ctx = ctx;
	memset(conn->recv_buffer, 0, BUFSIZ);
	memset(conn->send_buffer, 0, BUFSIZ);
	conn->res_type = RESOURCE_TYPE_NONE;
	return conn;
}

void connection_start_async_io(struct connection *conn)
{
	/* TODO: Start asynchronous operation (read from file).
	 * Use io_submit(2) & friends for reading data asynchronously.
	 */
	conn->state = STATE_ASYNC_ONGOING;
	io_prep_pread(&conn->iocb, conn->fd, conn->send_buffer, BUFSIZ, conn->file_pos);
	conn->piocb[0] = &conn->iocb;
	io_set_eventfd(&conn->iocb, conn->eventfd);
	DIE(io_submit(conn->ctx, 1, conn->piocb) < 0, "io_submit");
	w_epoll_add_ptr_in(epollfd, conn->eventfd, conn);
	w_epoll_update_ptr_in(epollfd, conn->sockfd, conn);
}

void connection_remove(struct connection *conn)
{
	/* TODO: Remove connection handler. */
	conn->state = STATE_CONNECTION_CLOSED;
	if (conn->fd != -1)
		close(conn->fd);
	close(conn->eventfd);
	close(conn->sockfd);
	free(conn);
}

void handle_new_connection(void)
{
	/* TODO: Handle a new connection request on the server socket. */
	static int sockfd;
	struct sockaddr_in addr;
	socklen_t addrlen = sizeof(struct sockaddr_in);
	struct connection *conn;
	/* TODO: Accept new connection. */
	sockfd = accept(listenfd, (SSA *) &addr, &addrlen);
	DIE(sockfd < 0, "accept");
	/* TODO: Set socket to be non-blocking. */
	fcntl(sockfd, F_SETFL, fcntl(sockfd, F_GETFL, 0) | O_NONBLOCK);
	DIE(sockfd < 0, "set to be non-blocking");
	/* TODO: Instantiate new connection handler. */
	conn = connection_create(sockfd);
	/* TODO: Add socket to epoll. */
	DIE(w_epoll_add_ptr_in(epollfd, sockfd, conn) < 0, "w_epoll_add_in");
	/* TODO: Initialize HTTP_REQUEST parser. */
	http_parser_init(&conn->request_parser, HTTP_REQUEST);
}

void receive_data(struct connection *conn)
{
	/* TODO: Receive message on socket.
	 * Store message in recv_buffer in struct connection.
	 */
	conn->state = STATE_RECEIVING_DATA;
	while (1) {
		ssize_t bytes_received = recv(conn->sockfd, conn->recv_buffer + conn->recv_len, BUFSIZ, 0);

		if (bytes_received <= 0)
			break;
		conn->recv_len += bytes_received;
	}
	if (parse_header(conn) < 0 || connection_open_file(conn) < 0) {
		connection_prepare_send_404(conn);
		conn->state = STATE_SENDING_404;
		return;
	}
	conn->state = STATE_REQUEST_RECEIVED;
	DIE(w_epoll_update_ptr_out(epollfd, conn->sockfd, conn) < 0, "w_epoll_update_ptr_out");
}

int connection_open_file(struct connection *conn)
{
	/* TODO: Open file and update connection fields. */
	conn->fd = open(conn->filename, O_RDONLY);
	if (conn->fd < 0)
		return -1;
	struct stat file_stat;

	DIE(fstat(conn->fd, &file_stat) < 0, "fstat ");
	conn->file_size = file_stat.st_size;
	conn->file_pos = 0;
	return 0;
}

void connection_complete_async_io(struct connection *conn)
{
	/* TODO: Complete asynchronous operation; operation returns successfully.
	 * Prepare socket for sending.
	 */
	struct io_event events[10];

	DIE(io_getevents(ctx, 1, 1, events, NULL) < 0, "io_getevents");
	size_t bytes_read = events[0].res;

	conn->state = STATE_SENDING_DATA;
	w_epoll_update_ptr_out(epollfd, conn->sockfd, conn);
	w_epoll_remove_ptr(epollfd, conn->eventfd, conn);
	conn->send_len = bytes_read;
	conn->send_pos = 0;
	conn->file_pos += bytes_read;
}

int parse_header(struct connection *conn)
{
	/* TODO: Parse the HTTP header and extract the file path. */
	/* Use mostly null settings except for on_path callback. */
	http_parser_settings settings_on_path = {
		.on_message_begin = 0,
		.on_header_field = 0,
		.on_header_value = 0,
		.on_path = aws_on_path_cb,
		.on_url = 0,
		.on_fragment = 0,
		.on_query_string = 0,
		.on_body = 0,
		.on_headers_complete = 0,
		.on_message_complete = 0
	};
	conn->request_parser.data = conn;
	http_parser_execute(&conn->request_parser, &settings_on_path, conn->recv_buffer, conn->recv_len);
	if (conn->have_path) {
		conn->res_type = connection_get_resource_type(conn);
		if (conn->res_type == RESOURCE_TYPE_NONE)
			return -1;
		snprintf(conn->filename, BUFSIZ, "%s%s", AWS_DOCUMENT_ROOT, conn->request_path + 1);
	}
	return 0;
}

enum connection_state connection_send_static(struct connection *conn)
{
	/* TODO: Send static data using sendfile(2). */
	off_t off = conn->file_pos;
	int bytes_sent = sendfile(conn->sockfd, conn->fd, &off, conn->file_size - off);

	if (bytes_sent < 0) {
		connection_prepare_send_404(conn);
		return STATE_SENDING_404;
	}
	conn->file_pos += bytes_sent;
	if (conn->file_pos == conn->file_size)
		return STATE_DATA_SENT;
	return STATE_SENDING_DATA;
}

int connection_send_data(struct connection *conn)
{
	/* May be used as a helper function. */
	/* TODO: Send as much data as possible from the connection send buffer.
	 * Returns the number of bytes sent or -1 if an error occurred
	 */
	int bytes_sent = 0;

	while (1) {
		bytes_sent += send(conn->sockfd, conn->send_buffer + conn->send_pos, conn->send_len - conn->send_pos, 0);
		if (conn->send_pos > bytes_sent)
			return -1;
		conn->send_pos = bytes_sent;
		if (conn->send_pos >= conn->send_len)
			break;
	}
	return bytes_sent;
}


int connection_send_dynamic(struct connection *conn)
{
	/* TODO: Read data asynchronously.
	 * Returns 0 on success and -1 on error.
	 */
	connection_complete_async_io(conn);
	if (connection_send_data(conn) < 0)
		return -1;
	if (conn->file_pos == conn->file_size)
		conn->state = STATE_DATA_SENT;
	else
		conn->state = STATE_SENDING_DATA;
	return 0;
}


void handle_input(struct connection *conn)
{
	/* TODO: Handle input information: may be a new message or notification of
	 * completion of an asynchronous I/O operation.
	 */

	switch (conn->state) {
	case STATE_INITIAL:
		receive_data(conn);
		break;

	case STATE_SENDING_404:
		connection_send_data(conn);
		conn->state = STATE_404_SENT;
		break;

	case STATE_ASYNC_ONGOING:
		if (connection_send_dynamic(conn) < 0) {
			connection_prepare_send_404(conn);
			conn->state = STATE_SENDING_404;
		}
		break;

	case STATE_404_SENT:
		connection_remove(conn);
		break;

	default:
		printf("shouldn't get here %d\n", conn->state);
	}
}

void handle_output(struct connection *conn)
{
	/* TODO: Handle output information: may be a new valid requests or notification of
	 * completion of an asynchronous I/O operation or invalid requests.
	 */

	switch (conn->state) {
	case STATE_REQUEST_RECEIVED:
		connection_prepare_send_reply_header(conn);
		conn->state = STATE_SENDING_HEADER;
		break;

	case STATE_SENDING_DATA:
		if (conn->res_type == RESOURCE_TYPE_NONE) {
			connection_prepare_send_404(conn);
			conn->state = STATE_SENDING_404;
		} else if (conn->res_type == RESOURCE_TYPE_STATIC) {
			conn->state = connection_send_static(conn);
		} else if (conn->res_type == RESOURCE_TYPE_DYNAMIC) {
			conn->state = STATE_ASYNC_ONGOING;
		}
		break;

	case STATE_SENDING_HEADER:
		connection_send_data(conn);
		conn->state = STATE_HEADER_SENT;
		break;

	case STATE_SENDING_404:
		connection_send_data(conn);
		conn->state = STATE_404_SENT;
		break;

	case STATE_ASYNC_ONGOING:
		connection_start_async_io(conn);
		break;

	case STATE_DATA_SENT:
		connection_remove(conn);
		break;

	case STATE_HEADER_SENT:
		if (conn->res_type == RESOURCE_TYPE_NONE) {
			connection_prepare_send_404(conn);
			conn->state = STATE_SENDING_404;
		} else {
			conn->state = STATE_SENDING_DATA;
		}
		break;

	case STATE_404_SENT:
		connection_remove(conn);
		break;

	default:
		ERR("Unexpected state\n");
		exit(1);
	}
}

void handle_client(uint32_t event, struct connection *conn)
{
	/* TODO: Handle new client. There can be input and output connections.
	 * Take care of what happened at the end of a connection.
	 */
	if (event & EPOLLIN)
		handle_input(conn);
	else if (event & EPOLLOUT)
		handle_output(conn);
	else
		connection_remove(conn);
}

int main(void)
{
	/* TODO: Initialize asynchronous operations. */
	DIE(io_setup(1, &ctx) != 0, "io_setup");
	/* TODO: Initialize multiplexing. */
	epollfd = w_epoll_create();
	DIE(epollfd < 0, "w_epoll_create");
	/* TODO: Create server socket. */
	listenfd = tcp_create_listener(AWS_LISTEN_PORT, DEFAULT_LISTEN_BACKLOG);
	DIE(listenfd < 0, "tcp_create_listener");
	/* TODO: Add server socket to epoll object*/
	DIE(w_epoll_add_fd_in(epollfd, listenfd) < 0, "w_epoll_add_fd_in");
	/* Uncomment the following line for debugging. */
	// dlog(LOG_INFO, "Server waiting for connections on port %d\n", AWS_LISTEN_PORT);

	/* server main loop */
	while (1) {
		struct epoll_event rev;

		/* TODO: Wait for events. */
		DIE(w_epoll_wait_infinite(epollfd, &rev) < 0, "w_epoll_wait_infinite");
		/* TODO: Switch event types; consider
		 *   - new connection requests (on server socket)
		 *   - socket communication (on connection sockets)
		 */
		if (rev.data.fd == listenfd) {
			if (rev.events & EPOLLIN)
				handle_new_connection();
		} else {
			handle_client(rev.events, (struct connection *) rev.data.ptr);
		}
	}

	return 0;
}
