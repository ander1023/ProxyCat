from wsgiref import headers
from modules.modules import ColoredFormatter, load_config, DEFAULT_CONFIG, check_proxies, check_for_updates, get_message, load_ip_list, print_banner, logos
import threading, argparse, logging, asyncio, time, socket, signal, sys, os
from concurrent.futures import ThreadPoolExecutor
from modules.proxyserver import AsyncProxyServer
from colorama import init, Fore, Style
from itertools import cycle
from tqdm import tqdm
import base64
from configparser import ConfigParser
from urllib.parse import urlparse

init(autoreset=True)

class ConnectionManager:
    """连接管理器，用于重用连接"""
    def __init__(self, max_connections=500, max_age=300):
        self.max_connections = max_connections
        self.max_age = max_age
        self.connections = {}
        self.lock = asyncio.Lock()
    
    async def get_connection(self, host, port):
        key = (host, port)
        async with self.lock:
            # 清理过期连接
            await self._cleanup()
            
            if key in self.connections:
                reader, writer, timestamp = self.connections[key]
                if time.time() - timestamp < self.max_age:
                    # 检查连接是否仍然有效
                    try:
                        writer.write(b'')
                        await writer.drain()
                        return reader, writer
                    except:
                        del self.connections[key]
            
            # 创建新连接
            reader, writer = await asyncio.open_connection(host, port)
            self.connections[key] = (reader, writer, time.time())
            
            return reader, writer
    
    async def _cleanup(self):
        """清理过期连接"""
        current_time = time.time()
        expired_keys = []
        for key, (_, _, timestamp) in self.connections.items():
            if current_time - timestamp > self.max_age:
                expired_keys.append(key)
        
        for key in expired_keys:
            reader, writer, _ = self.connections[key]
            try:
                writer.close()
                await writer.wait_closed()
            except:
                pass
            del self.connections[key]
    
    async def close_all(self):
        """关闭所有连接"""
        async with self.lock:
            for key, (reader, writer, _) in self.connections.items():
                try:
                    writer.close()
                    await writer.wait_closed()
                except:
                    pass
            self.connections.clear()

def setup_logging():
    """设置日志"""
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    formatter = ColoredFormatter(log_format)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logging.basicConfig(level=logging.INFO, handlers=[console_handler])

def update_status(server):
    """更新状态显示"""
    def print_proxy_info():
        status = f"{get_message('current_proxy', server.language)}: {server.current_proxy}"
        logging.info(status)

    def reload_server_config(new_config):
        old_use_getip = server.use_getip
        old_mode = server.mode
        old_port = int(server.config.get('port', '1080'))
        
        server.config.update(new_config)
        server._update_config_values(new_config)
        
        if old_use_getip != server.use_getip or old_mode != server.mode:
            server._handle_mode_change()
        
        if old_port != server.port:
            logging.info(get_message('port_changed', server.language, old_port, server.port))

    config_file = 'config/config.ini'
    ip_file = server.proxy_file
    last_config_modified_time = os.path.getmtime(config_file) if os.path.exists(config_file) else 0
    last_ip_modified_time = os.path.getmtime(ip_file) if os.path.exists(ip_file) else 0
    display_level = int(server.config.get('display_level', '1'))
    is_docker = os.path.exists('/.dockerenv')
    
    while True:
        try:
            if os.path.exists(config_file):
                current_config_modified_time = os.path.getmtime(config_file)
                if current_config_modified_time > last_config_modified_time:
                    logging.info(get_message('config_file_changed', server.language))
                    new_config = load_config(config_file)
                    reload_server_config(new_config)
                    last_config_modified_time = current_config_modified_time
                    continue
            
            if os.path.exists(ip_file) and not server.use_getip:
                current_ip_modified_time = os.path.getmtime(ip_file)
                if current_ip_modified_time > last_ip_modified_time:
                    logging.info(get_message('proxy_file_changed', server.language))
                    server._reload_proxies()
                    last_ip_modified_time = current_ip_modified_time
                    continue

            if display_level == 0:
                if not hasattr(server, 'last_proxy') or server.last_proxy != server.current_proxy:
                    print_proxy_info()
                    server.last_proxy = server.current_proxy
                time.sleep(1)
                continue

            if server.mode == 'loadbalance':
                if display_level >= 1:
                    print_proxy_info()
                time.sleep(5)
                continue

            time_left = server.time_until_next_switch()
            if time_left == float('inf'):
                if display_level >= 1:
                    print_proxy_info()
                time.sleep(5)
                continue
            
            if not hasattr(server, 'last_proxy') or server.last_proxy != server.current_proxy:
                print_proxy_info()
                server.last_proxy = server.current_proxy
                server.previous_proxy = server.current_proxy

            total_time = int(server.interval)
            elapsed_time = total_time - int(time_left)
            
            if display_level >= 1:
                if elapsed_time > total_time:
                    if hasattr(server, 'progress_bar'):
                        if not is_docker:
                            server.progress_bar.n = total_time
                            server.progress_bar.refresh()
                            server.progress_bar.close()
                        delattr(server, 'progress_bar')
                    if hasattr(server, 'last_update_time'):
                        delattr(server, 'last_update_time')
                    time.sleep(0.5)
                    continue
                
                if is_docker:
                    if not hasattr(server, 'last_update_time') or \
                       (time.time() - server.last_update_time >= (5 if display_level == 1 else 1) and elapsed_time <= total_time):
                        if display_level >= 2:
                            logging.info(f"{get_message('next_switch', server.language)}: {time_left:.0f} {get_message('seconds', server.language)} ({elapsed_time}/{total_time})")
                        else:
                            logging.info(f"{get_message('next_switch', server.language)}: {time_left:.0f} {get_message('seconds', server.language)}")
                        server.last_update_time = time.time()
                else:
                    if not hasattr(server, 'progress_bar'):
                        server.progress_bar = tqdm(
                            total=total_time,
                            desc=f"{Fore.YELLOW}{get_message('next_switch', server.language)}{Style.RESET_ALL}",
                            bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} ' + get_message('seconds', server.language),
                            colour='green'
                        )
                    
                    server.progress_bar.n = min(elapsed_time, total_time)
                    server.progress_bar.refresh()
                
        except Exception as e:
            if display_level >= 2:
                logging.error(f"Status update error: {e}")
            elif display_level >= 1:
                logging.error(get_message('status_update_error', server.language))
        time.sleep(1)

async def handle_client_wrapper(server, reader, writer, clients):
    """客户端处理包装器"""
    task = asyncio.create_task(server.handle_client(reader, writer))
    clients.add(task)
    try:
        await task
    except Exception as e:
        logging.error(get_message('client_handle_error', server.language, e))
    finally:
        clients.remove(task)

async def run_server(server):
    """运行服务器"""
    try:
        await server.start()
    except asyncio.CancelledError:
        logging.info(get_message('server_closing', server.language))
    except Exception as e:
        if not server.stop_server:
            logging.error(f"Server error: {e}")
    finally:
        await server.stop()

async def run_proxy_check(server):
    """运行代理检查"""
    if server.config.get('check_proxies', 'False').lower() == 'true':
        logging.info(get_message('proxy_check_start', server.language))
        valid_proxies = await check_proxies(server.proxies, server.test_url)
        if valid_proxies:
            server.proxies = valid_proxies
            server.proxy_cycle = cycle(valid_proxies)
            server.current_proxy = next(server.proxy_cycle)
            logging.info(get_message('valid_proxies', server.language, valid_proxies))
        else:
            logging.error(get_message('no_valid_proxies', server.language))
    else:
        logging.info(get_message('proxy_check_disabled', server.language))

class ProxyCat:
    """优化的代理服务器主类"""
    def __init__(self):
        # 优化线程池配置
        cpu_count = os.cpu_count() or 1
        max_workers = min(64, max(4, cpu_count * 2))
        
        self.executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="proxy_worker",
        )

        # 设置事件循环
        loop = asyncio.get_event_loop()
        loop.set_default_executor(self.executor)
        
        if hasattr(loop, 'set_task_factory'):
            loop.set_task_factory(None)
        
        # 优化socket设置
        socket.setdefaulttimeout(30)
        if hasattr(socket, 'TCP_NODELAY'):
            socket.TCP_NODELAY = True
        if hasattr(socket, 'SO_KEEPALIVE'):
            socket.SO_KEEPALIVE = True
        
        if hasattr(socket, 'SO_REUSEADDR'):
            socket.SO_REUSEADDR = True
        if hasattr(socket, 'SO_REUSEPORT') and os.name != 'nt':
            socket.SO_REUSEPORT = True
        
        # 优化文件描述符限制
        if os.name != 'nt':
            import resource
            try:
                soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
                target_limit = min(65536, hard)
                resource.setrlimit(resource.RLIMIT_NOFILE, (target_limit, hard))
                logging.info(f"File descriptor limit set to: {target_limit}")
            except (ValueError, resource.error) as e:
                logging.warning(f"Could not increase file descriptor limit: {e}")

        # 连接管理器
        self.connection_manager = ConnectionManager(
            max_connections=500,
            max_age=300
        )
        
        # 性能优化参数
        self.buffer_size = 64 * 1024  # 64KB缓冲区
        self.max_header_size = 8 * 1024  # 8KB最大头部大小
        self.connect_timeout = 10
        self.read_timeout = 30
        self.write_timeout = 30
        self.max_connections = 10000  # 提高最大连接数
        
        # 连接控制
        self.semaphore = asyncio.Semaphore(self.max_connections)
        self.running = True
        self.tasks = set()
        
        # 统计信息
        self._setup_monitoring()
        
        # 信号处理
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        
        # 加载配置
        self.config = load_config('config/config.ini')
        self.language = self.config.get('language', 'cn').lower()
        
        # 用户认证
        self.users = {}
        config = ConfigParser()
        config.read('config/config.ini', encoding='utf-8')
        if config.has_section('Users'):
            self.users = dict(config.items('Users'))
        self.auth_required = bool(self.users)
        
        # 代理池
        self.proxy_pool = {}

    def _setup_monitoring(self):
        """设置性能监控"""
        self.stats = {
            'connections_total': 0,
            'connections_active': 0,
            'bytes_transferred': 0,
            'errors_total': 0,
            'start_time': time.time()
        }

    async def start_monitoring(self):
        """启动监控任务"""
        while self.running:
            await asyncio.sleep(30)  # 每30秒记录一次
            self._log_stats()
    
    def _log_stats(self):
        """记录统计信息"""
        uptime = time.time() - self.stats['start_time']
        
        # 使用系统命令获取内存信息（替代 psutil）
        memory_usage = self._get_memory_usage()
        
        logging.info(
            f"Performance Stats - "
            f"Connections: {self.stats['connections_active']} active, "
            f"{self.stats['connections_total']} total, "
            f"Memory: {memory_usage:.2f} MB, "
            f"Transferred: {self.stats['bytes_transferred'] / 1024 / 1024:.2f} MB, "
            f"Errors: {self.stats['errors_total']}, "
            f"Uptime: {uptime:.0f}s"
        )
    
    def _get_memory_usage(self):
        """获取内存使用量（替代 psutil）"""
        try:
            if os.name == 'posix':
                # Linux/Unix 系统
                with open('/proc/self/status', 'r') as f:
                    for line in f:
                        if line.startswith('VmRSS:'):
                            memory_kb = int(line.split()[1])
                            return memory_kb / 1024.0
            elif os.name == 'nt':
                # Windows 系统
                import ctypes
                class MEMORYSTATUSEX(ctypes.Structure):
                    _fields_ = [
                        ("dwLength", ctypes.c_ulong),
                        ("dwMemoryLoad", ctypes.c_ulong),
                        ("ullTotalPhys", ctypes.c_ulonglong),
                        ("ullAvailPhys", ctypes.c_ulonglong),
                        ("ullTotalPageFile", ctypes.c_ulonglong),
                        ("ullAvailPageFile", ctypes.c_ulonglong),
                        ("ullTotalVirtual", ctypes.c_ulonglong),
                        ("ullAvailVirtual", ctypes.c_ulonglong),
                        ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
                    ]
                
                memory_status = MEMORYSTATUSEX()
                memory_status.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
                ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(memory_status))
                return (memory_status.ullTotalPhys - memory_status.ullAvailPhys) / (1024 * 1024)
        except:
            pass
        return 0.0

    async def start_server(self):
        """启动服务器"""
        try:
            # 创建socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if hasattr(socket, 'SO_REUSEPORT') and os.name != 'nt':
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            
            # 设置socket缓冲区
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 65536)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 65536)
            
            host = self.config.get('SERVER', 'host')
            port = int(self.config.get('SERVER', 'port'))
            sock.bind((host, port))
            
            # 启动服务器
            server = await asyncio.start_server(
                self.handle_client,
                sock=sock,
                backlog=4096  # 增加backlog
            )
            
            logging.info(get_message('server_running', self.language, host, port))
            
            # 启动监控任务
            monitor_task = asyncio.create_task(self.start_monitoring())
            
            async with server:
                await server.serve_forever()
                
        except Exception as e:
            logging.error(get_message('server_start_error', self.language, e))
            sys.exit(1)
        finally:
            await self.connection_manager.close_all()

    def handle_shutdown(self, signum, frame):
        """处理关闭信号"""
        logging.info(get_message('server_shutting_down', self.language))
        self.running = False
        self.executor.shutdown(wait=False, timeout=5)
        sys.exit(0)

    async def handle_client(self, reader, writer):
        """处理客户端连接"""
        # 使用信号量限制并发连接
        async with self.semaphore:
            return await self._handle_client_internal(reader, writer)
    
    async def _handle_client_internal(self, reader, writer):
        """内部客户端处理"""
        task = asyncio.current_task()
        self.tasks.add(task)
        
        try:
            self.stats['connections_total'] += 1
            self.stats['connections_active'] += 1
            
            # 认证检查
            if self.auth_required:
                if not await self._check_auth(reader, writer):
                    return
            
            # 处理请求
            await self._process_request(reader, writer)
            
        except Exception as e:
            self.stats['errors_total'] += 1
            logging.error(get_message('client_process_error', self.language, e))
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except:
                pass
            self.tasks.remove(task)
            self.stats['connections_active'] -= 1

    async def _check_auth(self, reader, writer):
        """检查认证"""
        try:
            # 读取请求头
            request_line = await asyncio.wait_for(reader.readline(), timeout=5)
            if not request_line:
                return False
                
            # 检查认证头
            headers_data = await self._read_headers(reader)
            auth_header = headers_data.get('proxy-authorization')
            
            if not auth_header or not self._authenticate(auth_header):
                response = b'HTTP/1.1 407 Proxy Authentication Required\r\n'
                response += b'Proxy-Authenticate: Basic realm="Proxy"\r\n'
                response += b'Content-Length: 0\r\n\r\n'
                writer.write(response)
                await writer.drain()
                return False
                
            return True
            
        except asyncio.TimeoutError:
            logging.warning("Authentication timeout")
            return False
        except Exception as e:
            logging.error(f"Auth check error: {e}")
            return False

    async def _read_headers(self, reader):
        """读取HTTP头部"""
        headers = {}
        while True:
            line = await reader.readline()
            if line in (b'\r\n', b'\n'):
                break
            if b':' in line:
                key, value = line.decode().split(':', 1)
                headers[key.strip().lower()] = value.strip()
        return headers

    def _authenticate(self, auth_header):
        """认证验证"""
        if not self.users:
            return True
        
        try:
            scheme, credentials = auth_header.split()
            if scheme.lower() != 'basic':
                return False
            
            decoded_auth = base64.b64decode(credentials).decode()
            username, password = decoded_auth.split(':')
            
            return username in self.users and self.users[username] == password
        except:
            return False

    async def _process_request(self, reader, writer):
        """处理代理请求"""
        try:
            request_line = await asyncio.wait_for(reader.readline(), timeout=5)
            if not request_line:
                return
                
            parts = request_line.decode().strip().split(' ')
            if len(parts) != 3:
                return
                
            method, target, version = parts
            
            if method == 'CONNECT':
                await self._handle_connect(target, reader, writer)
            else:
                await self._handle_http(method, target, version, reader, writer)
                
        except asyncio.TimeoutError:
            logging.warning("Request timeout")
        except Exception as e:
            logging.error(get_message('request_handling_error', self.language, e))

    async def _handle_connect(self, target, reader, writer):
        """处理CONNECT请求"""
        try:
            host, port = target.split(':')
            port = int(port)
            
            # 使用连接池获取远程连接
            remote_reader, remote_writer = await asyncio.wait_for(
                self.connection_manager.get_connection(host, port),
                timeout=self.connect_timeout
            )
            
            # 发送成功响应
            response = b'HTTP/1.1 200 Connection Established\r\n\r\n'
            writer.write(response)
            await asyncio.wait_for(writer.drain(), timeout=self.write_timeout)
            
            # 建立双向数据通道
            await self._create_optimized_pipe(reader, writer, remote_reader, remote_writer)
            
        except asyncio.TimeoutError:
            logging.warning(f"CONNECT timeout to {target}")
            await self._safe_send_error(writer, 504, "Gateway Timeout")
        except Exception as e:
            logging.error(f"CONNECT error to {target}: {e}")
            await self._safe_send_error(writer, 502, "Bad Gateway")

    async def _handle_http(self, method, target, version, reader, writer):
        """处理HTTP请求"""
        try:
            url = urlparse(target)
            host = url.hostname
            port = url.port or (443 if url.scheme == 'https' else 80)
            
            # 使用连接池获取远程连接
            remote_reader, remote_writer = await asyncio.wait_for(
                self.connection_manager.get_connection(host, port),
                timeout=self.connect_timeout
            )
            
            # 重构请求
            path = url.path + ('?' + url.query if url.query else '')
            request_line = f'{method} {path} {version}\r\n'
            remote_writer.write(request_line.encode())
            
            # 转发头部
            headers_data = await self._read_headers(reader)
            for key, value in headers_data.items():
                if key not in ['proxy-connection', 'proxy-authorization']:
                    header_line = f'{key}: {value}\r\n'
                    remote_writer.write(header_line.encode())
            
            remote_writer.write(b'\r\n')
            await asyncio.wait_for(remote_writer.drain(), timeout=self.write_timeout)
            
            # 建立双向数据通道
            await self._create_optimized_pipe(reader, writer, remote_reader, remote_writer)
            
        except asyncio.TimeoutError:
            logging.warning(f"HTTP timeout to {target}")
            await self._safe_send_error(writer, 504, "Gateway Timeout")
        except Exception as e:
            logging.error(f"HTTP error to {target}: {e}")
            await self._safe_send_error(writer, 502, "Bad Gateway")

    async def _create_optimized_pipe(self, client_reader, client_writer, remote_reader, remote_writer):
        """创建优化的数据传输管道"""
        try:
            # 并行处理双向数据流
            await asyncio.gather(
                self._transfer_data(client_reader, remote_writer, "client->remote"),
                self._transfer_data(remote_reader, client_writer, "remote->client"),
                return_exceptions=True
            )
        except Exception as e:
            logging.error(f"Data pipe error: {e}")
        finally:
            # 确保资源清理
            await self._safe_close_connection(remote_writer)

    async def _transfer_data(self, reader, writer, direction):
        """高效数据传输"""
        try:
            transferred = 0
            while True:
                data = await asyncio.wait_for(
                    reader.read(self.buffer_size),
                    timeout=self.read_timeout
                )
                if not data:
                    break
                    
                writer.write(data)
                await asyncio.wait_for(writer.drain(), timeout=self.write_timeout)
                transferred += len(data)
                
            # 更新统计
            self.stats['bytes_transferred'] += transferred
            
        except asyncio.TimeoutError:
            logging.debug(f"Transfer timeout in {direction}")
        except (ConnectionResetError, BrokenPipeError):
            logging.debug(f"Connection closed in {direction}")
        except Exception as e:
            logging.error(f"Transfer error in {direction}: {e}")

    async def _safe_send_error(self, writer, code, message):
        """安全发送错误响应"""
        try:
            response = f'HTTP/1.1 {code} {message}\r\n\r\n'.encode()
            writer.write(response)
            await writer.drain()
        except:
            pass
        finally:
            await self._safe_close_connection(writer)

    async def _safe_close_connection(self, writer):
        """安全关闭连接"""
        try:
            writer.close()
            await asyncio.wait_for(writer.wait_closed(), timeout=2)
        except:
            pass

async def main():
    """主函数"""
    setup_logging()
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description=logos())
    parser.add_argument('-c', '--config', default='config/config.ini', help='配置文件路径')
    args = parser.parse_args()
    
    # 加载配置
    config = load_config(args.config)
    server = AsyncProxyServer(config)
    
    # 显示横幅
    print_banner(config)
    
    # 检查更新
    await check_for_updates(config.get('language', 'cn').lower())
    
    # 代理检查
    if not config.get('use_getip', 'False').lower() == 'true':
        await run_proxy_check(server)
    else:
        logging.info(get_message('api_mode_notice', server.language))
    
    # 启动状态线程
    status_thread = threading.Thread(target=update_status, args=(server,), daemon=True)
    status_thread.start()
    
    # 启动清理线程
    cleanup_thread = threading.Thread(target=lambda: asyncio.run(server.cleanup_clients()), daemon=True)
    cleanup_thread.start()
    
    try:
        # 运行服务器
        await run_server(server)
    except KeyboardInterrupt:
        logging.info(get_message('user_interrupt', server.language))
    finally:
        # 清理资源
        if hasattr(server, 'connection_manager'):
            await server.connection_manager.close_all()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Proxy server shutdown complete")
    except Exception as e:
        logging.error(f"Server fatal error: {e}")
        sys.exit(1)
