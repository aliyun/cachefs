// Redefine ELOG_INFO to ELOG_DEBUG to suppress excessive ylt info logs
#define ELOG_INFO ELOG_DEBUG
#include <iostream>
#define cout cerr
#include <ylt/easylog.hpp>
#undef cout

#include "coro_rpc.h"
#include "ylt/coro_io/load_balancer.hpp"
#include <system_error>
#include <ylt/struct_pack.hpp>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

typedef struct {
  uint64_t req_id;
  struct_pack::compatible<int32_t, 20251201> svc_id;
} compat_load_request;

// svc_id has to stay a compatible member. Compatible members are left out of
// the type literal, and STRUCT_PACK_DISABLE_OUTER_BRACE then hashes a struct
// with one plain member as that member alone, which is what peers predating
// svc_id put on the wire. Declaring svc_id as a plain field changes the type
// code and every such peer fails with errc::invalid_rpc_arguments.
static_assert(struct_pack::get_type_code<compat_load_request>() ==
              struct_pack::get_type_code<uint64_t>());

inline char *create_copy_cstr(std::string msg) {
  char *buf = (char *)malloc(msg.size() + 1);
  memcpy(buf, msg.data(), msg.size());
  buf[msg.size()] = '\0';
  return buf;
}

inline void set_rpc_result(rpc_result &ret, coro_rpc::rpc_error ec) {
  ret.ec = ec.val();
  ret.err_msg = create_copy_cstr(ec.msg);
}

// rpc server
inline void ylt_load_service(coro_rpc::context<void> context, compat_load_request req_info) {
  auto ctx_ptr = std::make_unique<coro_rpc::context<void>>(std::move(context));
  uint64_t req_id = req_info.req_id;
  uint32_t svc_id = 0;
  if (req_info.svc_id.has_value()) {
    svc_id = req_info.svc_id.value();
  }
  load_service(ctx_ptr.release(), svc_id, req_id);
}

void response_error(void *ctx, uint16_t err_code, const char *err_msg) {
  std::unique_ptr<coro_rpc::context<void>> context(
      (coro_rpc::context<void> *)ctx);

  context->response_error(coro_rpc::err_code(err_code), err_msg);
}

void *response_msg(void *ctx, char *msg, uint64_t size) {
  std::unique_ptr<coro_rpc::context<void>> context(
      (coro_rpc::context<void> *)ctx);
  // wait for response finish;
  auto promise = std::make_unique<std::promise<std::error_code>>();
  context->get_context_info()->set_complete_handler(
      [p = promise.get()](const std::error_code &ec, std::size_t) {
        p->set_value(ec);
      });

  context->get_context_info()->set_response_attachment(
      std::string_view(msg, size));
  context->response_msg();
  return promise.release();
}

rpc_result wait_response_finish(void *p) {
  auto promise = (std::promise<std::error_code> *)p;
  auto ec = promise->get_future().get();
  delete promise;
  if (!ec) {
    return {};
  }

  char *buf = create_copy_cstr(ec.message());
  return rpc_result{0, buf, 0};
}

void *start_rpc_server(char *addr, server_config conf) {
  auto server = std::make_unique<coro_rpc::coro_rpc_server>(
      conf.parallel, addr, std::chrono::seconds(600));
  server->register_handler<ylt_load_service>();
  if (conf.enable_ib) {
    coro_io::ib_socket_t::config_t ib_conf{};
    if (conf.min_recv_buf_count != 0)
      ib_conf.recv_buffer_cnt = conf.min_recv_buf_count;

    if (conf.max_recv_buf_count != 0) {
      ib_conf.cap.max_recv_wr = conf.max_recv_buf_count;
    }

    ib_conf.cap.max_recv_wr =
        (std::max<uint32_t>)(ib_conf.cap.max_recv_wr, ib_conf.recv_buffer_cnt + 1);

    std::string nic_name;
    if (conf.device_name != nullptr) {
      nic_name = conf.device_name;
    }
    auto dev = coro_io::get_global_ib_device({.dev_name = nic_name});
    if (dev == nullptr) {
      return nullptr;
    }
    ib_conf.device = dev;

    server->init_ibv(ib_conf);
  }

  auto res = server->async_start();
  if (res.hasResult()) {
    ELOG_ERROR << "start server failed";
    return nullptr;
  }

  return server.release();
}

void stop_rpc_server(void *server) {
  if (server != nullptr) {
    auto s = (coro_rpc::coro_rpc_server *)server;
    s->stop();
    delete s;
  }
}

void config_buffer_pool(pool_config conf) {
  coro_io::ib_buffer_pool_t::config_t pool_conf{};
  if (conf.buffer_size != 0) {
    pool_conf.buffer_size = conf.buffer_size;
  }

  if (conf.max_memory_usage != 0) {
    pool_conf.max_memory_usage = conf.max_memory_usage;
  }

  coro_io::get_global_ib_device({.buffer_pool_config = pool_conf});
}

// rpc client
// The pair owns the client_pools registry and the load_balancer built on it,
// so that flush_client_pool can drain the idle connections of one address.
using client_pool_type =
    std::pair<std::unique_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>>,
              std::optional<coro_io::load_balancer<coro_rpc::coro_rpc_client>>>;

void *create_client_pool(char *addr, client_config conf) {
  std::vector<std::string_view> hosts{std::string_view(addr)};
  coro_io::client_pool<coro_rpc::coro_rpc_client>::pool_config pool_conf{};
  pool_conf.max_connection_life_time = std::chrono::seconds(3600);
  if (conf.enable_ib) {
    coro_io::ib_socket_t::config_t ib_conf{};
    if (conf.min_recv_buf_count != 0)
      ib_conf.recv_buffer_cnt = conf.min_recv_buf_count;

    if (conf.max_recv_buf_count != 0) {
      ib_conf.cap.max_recv_wr = conf.max_recv_buf_count;
    }

    ib_conf.cap.max_recv_wr =
        (std::max<uint32_t>)(ib_conf.cap.max_recv_wr, ib_conf.recv_buffer_cnt + 1);

    std::string device_name;
    if (conf.local_ip != nullptr) {
      device_name = conf.local_ip;
    }
    auto dev = coro_io::get_global_ib_device({.dev_name = device_name});
    if (dev == nullptr) {
      return nullptr;
    }
    ib_conf.device = dev;

    pool_conf.client_config.socket_config = ib_conf;
  }

  pool_conf.client_config.connect_timeout_duration =
      std::chrono::seconds{conf.connect_timeout_sec};

  pool_conf.client_config.request_timeout_duration =
      std::chrono::seconds{conf.req_timeout_sec};

  if (conf.idle_sec > 0) {
    pool_conf.idle_timeout = std::chrono::seconds{conf.idle_sec};
  }

  // negative means keep the library default; 0 means no retry.
  if (conf.connect_retry_count >= 0) {
    pool_conf.connect_retry_count =
        static_cast<uint32_t>(conf.connect_retry_count);
  }

  if (conf.local_ip != nullptr && !conf.enable_ib) {
    pool_conf.client_config.local_ip = conf.local_ip;
  }

  ELOG_INFO << "client config connect timeout seconds: "
            << conf.connect_timeout_sec
            << ", request timeout seconds: " << conf.req_timeout_sec
            << ", idle timeout seconds: " << conf.idle_sec
            << ", connect retry count: " << pool_conf.connect_retry_count
            << ", local_ip: " << pool_conf.client_config.local_ip;

  // create client_pools and a load_balancer.
  // the load_balancer uses client_pools and will release all connections when it is deleted.
  auto *pool = new client_pool_type(
      std::make_unique<coro_io::client_pools<coro_rpc::coro_rpc_client>>(
          pool_conf, coro_io::g_io_context_pool()),
      std::optional<coro_io::load_balancer<coro_rpc::coro_rpc_client>>{});
  pool->second = coro_io::load_balancer<coro_rpc::coro_rpc_client>::create(
      hosts, {pool_conf}, {}, *pool->first);
  return pool;
}

void free_client_pool(void *pool) {
  auto ptr = (client_pool_type *)pool;
  delete ptr;
}

uint64_t flush_client_pool(void *pool, char *addr) {
  auto *p = (client_pool_type *)pool;
  auto cp = p->first->at(std::string_view(addr));
  return static_cast<uint64_t>(cp->clear());
}

rpc_result load(void *pool, load_request req_info, char *dest, uint64_t dest_len) {
  using namespace async_simple::coro;
  rpc_result ret{};
  if (dest == nullptr || dest_len == 0) {
    set_rpc_result(ret,
                   coro_rpc::rpc_error(coro_rpc::errc::invalid_rpc_arguments));
    return ret;
  }

  auto ld = &((client_pool_type *)pool)->second.value();
  auto lazy = [&]() -> Lazy<void> {
    auto result =
        co_await ld->send_request([&](coro_rpc::coro_rpc_client &client,
                                      std::string_view hostname) -> Lazy<void> {
          client.set_resp_attachment_buf(std::span<char>(dest, dest_len));
          compat_load_request compat_req_info;
          compat_req_info.req_id = req_info.req_id;
          compat_req_info.svc_id = req_info.svc_id;
          auto result = co_await client.call<ylt_load_service>(compat_req_info);
          if (!result) {
            set_rpc_result(ret, result.error());
            co_return;
          }

          if (!client.is_resp_attachment_in_external_buf()) {
            set_rpc_result(
                ret, coro_rpc::rpc_error(coro_rpc::errc::message_too_large));
            co_return;
          }
          ret.len = client.get_resp_attachment().size();
        });
    if (!result) {
      if (result.error() == std::errc::connection_refused) {
        set_rpc_result(ret, coro_rpc::rpc_error(coro_rpc::errc::not_connected));
      } else {
        set_rpc_result(ret,
                       coro_rpc::rpc_error(
                           coro_rpc::errc::io_error,
                           std::make_error_code(result.error()).message()));
      }
    }
  };

  syncAwait(lazy());

  return ret;
}

uint64_t global_memory_usage() {
  return coro_io::ib_buffer_pool_t::global_memory_usage();
}

uint64_t global_history_max_memory_usage() {
  return coro_io::ib_buffer_pool_t::global_history_max_memory_usage();
}

char *get_all_rdma_devs() {
  auto dev_manager = coro_io::g_ib_device_manager();
  auto &list = dev_manager->get_dev_list();
  if (list.empty()) {
    return nullptr;
  }
  std::string all_dev_name;
  for (auto &[name, dev] : list) {
    all_dev_name.append(name).append(" ");
  }
  all_dev_name.pop_back();
  return create_copy_cstr(all_dev_name);
}

// log
void init_rpc_log(char *log_filename, int log_level, uint64_t max_file_size,
                  uint64_t max_file_num, bool async) {
  if (log_filename == nullptr) {
    easylog::set_min_severity((easylog::Severity)log_level);
    ELOG_DEBUG << "init log, log_level: " << log_level;
    return;
  }

  std::string filename(log_filename);
  easylog::init_log((easylog::Severity)log_level, filename, async, true,
                    max_file_size, max_file_num, false);
  ELOG_DEBUG << "init log " << log_level << ", " << filename << ","
             << max_file_size << "," << max_file_num;
}

void flush_rpc_log() { easylog::flush(); }
