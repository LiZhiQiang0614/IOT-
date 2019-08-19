import queue
import socket
import ssl
import threading
import time
from log import TNLog

from Behavior.behaviors import Behaviors


class ServerSocket(object):
    QUEUE = queue.Queue()

    def __init__(self, ip, port):
        # 生成SSL上下文
        # 切记！！！！！加密一定要在跟客户端连接前进行加密，否则没有意义！！！！！！
        self.context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
        # 加载服务器所用证书(server.crt)和私钥(server.key)
        self.context.load_cert_chain('cert/server.crt', 'cert/server.key')
        self.port = port
        self.ip = ip
        self.sever_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sever_sock.settimeout(2000)   # 设置了超时时间 2s
        self.logger = TNLog()
        # print('>>>>上下文建立成功<<<<')

    @staticmethod
    def create_sever(ip, port):
        loger = TNLog()
        st = ServerSocket(ip, port)
        addr_config = (st.ip, st.port)
        st.sever_sock.bind(addr_config)
        st.sever_sock.listen(5)
        loger.info('timt:{t}等待连接~~~'.format(t=time.ctime()))
        return st

    def run_pro(self, act):
        self.logger.info('===将要执行任务===')
        # 生成线程去处理客户端的请求
        while True:
            # 从非空消息队列中获取客户端数据[类型:元组(client_sock,client_ip)]
            if not self.QUEUE.empty():
                _ = self.QUEUE.get()
                client_socket = _[0]
                client_ip = _[1]
                t = threading.Thread(target=act, args=(client_socket, client_ip))
                t.start()
            try:
                client_socket, addr = self.sever_sock.accept()
            except socket.timeout:
                # self.sever_sock.close()
                continue
            self.logger.info('===已经连接到客户端:{}==='.format(addr))
            # 设置服务端非阻塞的形势接受客户端的数据
            # client_socket.setblocking(0)
            # 将客户端socket添加入消息队列
            self.QUEUE.put((client_socket, addr))
            # 把消息队列加在




if __name__ == '__main__':
    # 定义一个容器对象来存放连接到服务端的客户端socket
    # 开启主线程来监听客户端的连接
    st = ServerSocket.create_sever('0.0.0.0', 8005)
    act = Behaviors()
    st.run_pro(act)
