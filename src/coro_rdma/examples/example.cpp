#include <async_simple/Future.h>
#include <async_simple/coro/Lazy.h>
#include <async_simple/executors/SimpleExecutor.h>
#include <infiniband/verbs.h>
#include <rdmapp/cq.h>
#include <rdmapp/qp.h>

#include <asio/buffer.hpp>
#include <asio/dispatch.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/streambuf.hpp>
#include <ylt/coro_io/coro_io.hpp>
#include <ylt/coro_io/io_context_pool.hpp>

rdmapp::device_ptr g_device;
rdmapp::pd_ptr g_pd;
rdmapp::cq_ptr g_cq;
std::atomic<bool> g_stop;

async_simple::coro::Lazy<rdmapp::deserialized_qp> recv_qp(
    asio::ip::tcp::socket &socket) {
  std::array<uint8_t, rdmapp::deserialized_qp::qp_header::kSerializedSize>
      header;
  {
    auto [ec, size] =
        co_await coro_io::async_read(socket, asio::buffer(header));
    if (ec) {
      std::cerr << ec.message() << std::endl;
      throw std::runtime_error("read qp header failed");
    }
  }
  auto remote_qp = rdmapp::deserialized_qp::deserialize(header.data());
  if (remote_qp.header.user_data_size > 0) {
    remote_qp.user_data.resize(remote_qp.header.user_data_size);
    auto [ec, size] =
        co_await coro_io::async_read(socket, asio::buffer(remote_qp.user_data));
    if (ec) {
      std::cerr << ec.message() << std::endl;
      throw std::runtime_error("read qp user data failed");
    }
  }
  co_return remote_qp;
}

async_simple::coro::Lazy<std::error_code> send_qp(asio::ip::tcp::socket &socket,
                                                  rdmapp::qp_ptr qp) {
  auto serialized_qp = qp->serialize();
  auto [ec, size] =
      co_await coro_io::async_write(socket, asio::buffer(serialized_qp));
  co_return ec;
}

class rdma_qp_client {
  asio::io_context *out_ctx_;
  std::unique_ptr<coro_io::ExecutorWrapper<>> out_executor_ = nullptr;
  uint16_t port_;
  std::string address_;
  std::error_code errc_ = {};
  asio::ip::tcp::socket socket_;

 public:
  rdma_qp_client(asio::io_context &ctx, unsigned short port,
                 std::string address = "0.0.0.0")
      : out_ctx_(&ctx), port_(port), address_(address), socket_(ctx) {}

  async_simple::coro::Lazy<void> handle_qp(rdmapp::qp_ptr qp) {
    char buffer[6];
    auto buffer_mr = std::make_unique<rdmapp::local_mr>(
        qp->pd()->reg_mr(&buffer[0], sizeof(buffer)));

    /* Send/Recv */
    auto [n, _] = co_await qp->recv(buffer_mr.get());
    std::cout << "Received " << n << " bytes from server: " << buffer
              << std::endl;
    std::copy_n("world", sizeof(buffer), buffer);
    co_await qp->send(buffer_mr.get());
    std::cout << "Sent to server: " << buffer << std::endl;

    /* Read/Write */
    char remote_mr_serialized[rdmapp::remote_mr::kSerializedSize];
    auto remote_mr_header_buffer =
        std::make_unique<rdmapp::local_mr>(qp->pd()->reg_mr(
            &remote_mr_serialized[0], sizeof(remote_mr_serialized)));
    co_await qp->recv(remote_mr_header_buffer.get());
    auto remote_mr = rdmapp::remote_mr::deserialize(remote_mr_serialized);
    std::cout << "Received mr addr=" << remote_mr.addr()
              << " length=" << remote_mr.length()
              << " rkey=" << remote_mr.rkey() << " from server" << std::endl;
    auto wc = co_await qp->read(remote_mr, buffer_mr.get());
    std::cout << "Read " << wc.byte_len << " bytes from server: " << buffer
              << std::endl;
    std::copy_n("world", sizeof(buffer), buffer);
    co_await qp->write_with_imm(remote_mr, buffer_mr.get(), 1);

    /* Atomic Fetch-and-Add (FA)/Compare-and-Swap (CS) */
    char counter_mr_serialized[rdmapp::remote_mr::kSerializedSize];
    auto counter_mr_header_mr =
        std::make_unique<rdmapp::local_mr>(qp->pd()->reg_mr(
            &counter_mr_serialized[0], sizeof(counter_mr_serialized)));
    co_await qp->recv(counter_mr_header_mr.get());
    auto remote_counter_mr =
        rdmapp::remote_mr::deserialize(counter_mr_serialized);
    std::cout << "Received mr addr=" << remote_counter_mr.addr()
              << " length=" << remote_counter_mr.length()
              << " rkey=" << remote_counter_mr.rkey() << " from server"
              << std::endl;
    uint64_t counter = 0;
    auto local_counter_mr = std::make_unique<rdmapp::local_mr>(
        qp->pd()->reg_mr(&counter, sizeof(counter)));
    co_await qp->fetch_and_add(remote_counter_mr, local_counter_mr.get(), 1);
    std::cout << "Fetched and added from server: " << counter << std::endl;
    co_await qp->write_with_imm(remote_mr, buffer_mr.get(), 1);
    co_await qp->compare_and_swap(remote_counter_mr, local_counter_mr.get(), 43,
                                  4422);
    std::cout << "Compared and swapped from server: " << counter << std::endl;
    co_await qp->write_with_imm(remote_mr, buffer_mr.get(), 1);

    co_return;
  }

  async_simple::coro::Lazy<std::error_code> handle_server_connection(
      asio::ip::tcp::socket &socket) {
    auto qp = std::make_unique<rdmapp::qp>(g_pd, g_cq, g_cq);
    co_await send_qp(socket, qp.get());
    std::cerr << "send qp success" << std::endl;
    auto remote_qp = co_await recv_qp(socket);
    auto const remote_gid_str =
        rdmapp::device::gid_hex_string(remote_qp.header.gid);
    printf("received header gid=%s lid=%u qpn=%u psn=%u user_data_size=%u\n",
           remote_gid_str.c_str(), remote_qp.header.lid,
           remote_qp.header.qp_num, remote_qp.header.sq_psn,
           remote_qp.header.user_data_size);
    std::cerr << "recv qp success" << std::endl;
    qp->rtr(remote_qp.header.lid, remote_qp.header.qp_num,
            remote_qp.header.sq_psn, remote_qp.header.gid);
    qp->user_data() = std::move(remote_qp.user_data);
    qp->rts();
    co_await handle_qp(qp.get());
    co_return std::error_code{};
  }

  async_simple::coro::Lazy<void> run() {
    coro_io::ExecutorWrapper<> *executor;
    if (out_ctx_ == nullptr) {
      executor = out_executor_.get();
    }
    else {
      out_executor_ = std::make_unique<coro_io::ExecutorWrapper<>>(
          out_ctx_->get_executor());
      executor = out_executor_.get();
    }
    auto ec = co_await coro_io::async_connect(executor, socket_, address_,
                                              std::to_string(port_));
    if (ec) {
      throw std::system_error(ec, "connect failed");
    }
    co_await handle_server_connection(socket_);
  }
};

class rdma_qp_server {
  asio::io_context *out_ctx_;
  std::unique_ptr<coro_io::ExecutorWrapper<>> out_executor_ = nullptr;
  uint16_t port_;
  std::string address_;
  std::error_code errc_ = {};
  asio::ip::tcp::acceptor acceptor_;
  std::promise<void> acceptor_close_waiter_;

 public:
  rdma_qp_server(asio::io_context &ctx, unsigned short port,
                 std::string address = "0.0.0.0")
      : out_ctx_(&ctx), port_(port), address_(address), acceptor_(ctx) {}

  std::error_code listen() {
    asio::error_code ec;

    asio::ip::tcp::resolver::query query(address_, std::to_string(port_));
    asio::ip::tcp::resolver resolver(acceptor_.get_executor());
    asio::ip::tcp::resolver::iterator it = resolver.resolve(query, ec);

    asio::ip::tcp::resolver::iterator it_end;
    if (ec || it == it_end) {
      if (ec) {
        return ec;
      }
      return std::make_error_code(std::errc::address_not_available);
    }

    auto endpoint = it->endpoint();
    ec = acceptor_.open(endpoint.protocol(), ec);

    if (ec) {
      return ec;
    }
#ifdef __GNUC__
    ec = acceptor_.set_option(asio::ip::tcp::acceptor::reuse_address(true), ec);
#endif
    ec = acceptor_.bind(endpoint, ec);
    if (ec) {
      std::error_code ignore_ec;
      ignore_ec = acceptor_.cancel(ignore_ec);
      ignore_ec = acceptor_.close(ignore_ec);
      return ec;
    }
    ec = acceptor_.listen(asio::socket_base::max_listen_connections, ec);
    if (ec) {
      return ec;
    }
    auto local_ep = acceptor_.local_endpoint(ec);
    if (ec) {
      return ec;
    }
    port_ = local_ep.port();
    std::cerr << "listen success, port: " << port_ << std::endl;
    return {};
  }

  async_simple::coro::Lazy<std::error_code> handle_qp(rdmapp::qp_ptr qp) {
    /* Send/Recv */
    char buffer[6] = "hello";
    auto local_mr = std::make_unique<rdmapp::local_mr>(
        qp->pd()->reg_mr(&buffer[0], sizeof(buffer)));
    co_await qp->send(local_mr.get());
    std::cout << "Sent to client: " << buffer << std::endl;
    co_await qp->recv(local_mr.get());
    std::cout << "Received from client: " << buffer << std::endl;

    /* Read/Write */
    std::copy_n("hello", sizeof(buffer), buffer);
    auto local_mr_serialized = local_mr->serialize();
    auto local_mr_header_mr = std::make_unique<rdmapp::local_mr>(
        qp->pd()->reg_mr(&local_mr_serialized[0], local_mr_serialized.size()));
    co_await qp->send(local_mr_header_mr.get());
    std::cout << "Sent mr addr=" << local_mr->addr()
              << " length=" << local_mr->length()
              << " rkey=" << local_mr->rkey() << " to client" << std::endl;
    auto [_, imm] = co_await qp->recv(local_mr.get());
    assert(imm.has_value());
    std::cout << "Written by client (imm=" << imm.value() << "): " << buffer
              << std::endl;

    /* Atomic */
    uint64_t counter = 42;
    auto counter_mr = std::make_unique<rdmapp::local_mr>(
        qp->pd()->reg_mr(&counter, sizeof(counter)));
    auto counter_mr_serialized = counter_mr->serialize();
    auto counter_mr_header_mr = new rdmapp::local_mr(qp->pd()->reg_mr(
        &counter_mr_serialized[0], counter_mr_serialized.size()));
    co_await qp->send(counter_mr_header_mr);
    std::cout << "Sent mr addr=" << counter_mr->addr()
              << " length=" << counter_mr->length()
              << " rkey=" << counter_mr->rkey() << " to client" << std::endl;
    imm = (co_await qp->recv(local_mr.get())).second;
    assert(imm.has_value());
    std::cout << "Fetched and added by client: " << counter << std::endl;
    imm = (co_await qp->recv(local_mr.get())).second;
    assert(imm.has_value());
    std::cout << "Compared and swapped by client: " << counter << std::endl;

    co_return std::error_code{};
  }

  async_simple::coro::Lazy<std::error_code> handle_client_connection(
      asio::ip::tcp::socket &socket) {
    auto remote_qp = co_await recv_qp(socket);
    auto qp = new rdmapp::qp(remote_qp.header.lid, remote_qp.header.qp_num,
                             remote_qp.header.sq_psn, remote_qp.header.gid,
                             g_pd, g_cq, g_cq);
    std::cerr << "recv qp success" << std::endl;
    auto const remote_gid_str =
        rdmapp::device::gid_hex_string(remote_qp.header.gid);
    printf("received header gid=%s lid=%u qpn=%u psn=%u user_data_size=%u\n",
           remote_gid_str.c_str(), remote_qp.header.lid,
           remote_qp.header.qp_num, remote_qp.header.sq_psn,
           remote_qp.header.user_data_size);
    co_await send_qp(socket, qp);
    std::cerr << "send qp success" << std::endl;
    co_await handle_qp(qp);
    co_return std::error_code{};
  }

  async_simple::coro::Lazy<std::error_code> async_accept() {
    for (;;) {
      coro_io::ExecutorWrapper<> *executor;
      if (out_ctx_ == nullptr) {
        executor = out_executor_.get();
      }
      else {
        out_executor_ = std::make_unique<coro_io::ExecutorWrapper<>>(
            out_ctx_->get_executor());
        executor = out_executor_.get();
      }
      asio::ip::tcp::socket socket(executor->get_asio_executor());
      auto ec = co_await coro_io::async_accept(acceptor_, socket);
      if (ec) {
        std::cerr << "accept failed: " << ec.message() << std::endl;
        if (ec == asio::error::operation_aborted ||
            ec == asio::error::bad_descriptor) {
          acceptor_close_waiter_.set_value();
          co_return ec;
        }
        continue;
      }
      std::cout << "accpeted connection " << socket.remote_endpoint()
                << std::endl;
      co_await handle_client_connection(socket);
    }
  }
};

int main(int argc, char *argv[]) {
  // For server run ./example [port]
  // For client run ./example [ip] [port]
  asio::io_context socket_ctx;
  g_device = new rdmapp::device();
  g_pd = new rdmapp::pd(g_device);
  g_cq = new rdmapp::cq(g_device);
  auto work = std::make_unique<asio::io_context::work>(socket_ctx);
  std::jthread worker_thread([&socket_ctx] {
    while (!g_stop) {
      socket_ctx.poll_one();
      struct ibv_wc wc;
      auto has_wc = g_cq->poll(wc);
      if (has_wc) {
        if (wc.opcode & IBV_WC_RECV) {
          auto &recv_awaitable =
              *reinterpret_cast<rdmapp::qp::recv_awaitable *>(wc.wr_id);
          recv_awaitable.complete(wc);
        }
        else {
          auto &send_awaitable =
              *reinterpret_cast<rdmapp::qp::send_awaitable *>(wc.wr_id);
          send_awaitable.complete(wc);
        }
      }
    }
  });

  if (argc == 2) {
    std::cout << "server" << std::endl;
    std::string port_str = argv[1];
    rdma_qp_server server(socket_ctx, std::stoi(port_str));
    auto ec = server.listen();
    if (ec) {
      std::cerr << "listen failed: " << ec.message() << std::endl;
      return -1;
    }
    async_simple::coro::syncAwait(server.async_accept());
  }
  else if (argc == 3) {
    std::cout << "client" << std::endl;
    std::string ip = argv[1];
    std::string port_str = argv[2];
    rdma_qp_client client(socket_ctx, std::stoi(port_str), ip);
    async_simple::coro::syncAwait(client.run());
    g_stop = true;
  }
  else {
    std::cerr << "server usage: " << argv[0] << " [port]" << std::endl;
    std::cerr << "client usage: " << argv[0] << " [ip] [port]" << std::endl;
  }

  return 0;
}