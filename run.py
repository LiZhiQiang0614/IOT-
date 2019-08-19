from Sever.async_server import ServerSocket
from Behavior.behaviors import Behaviors

if __name__ == '__main__':
    server_sock = ServerSocket.create_sever('0.0.0.0', 8000)
    behavior = Behaviors()
    server_sock.run_pro(act=behavior)
