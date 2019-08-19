import queue
import threading
from mysqlhelper.mysqlhelper import DB
import time
from log import TNLog
import traceback


class DeviceOffline(object):
    QUEUE = queue.Queue()
    def __init__(self):
        self.logger = TNLog()

    def deviceStatus(self):
        self.logger.warning('对设备离线进行检测')
        stmp = 5
        start = 0
        db = DB()
        while True:
            try:
                res = db.select_data(start=start, stmp=stmp)
                if len(res) < 5:
                    start = 0
                    if res:
                        for i in res:
                            self.QUEUE.put(i)
                    del res
                    time.sleep(2)
                    continue
                start += stmp
                if res:
                    for i in res:
                        self.QUEUE.put(i)
                    del res
            except Exception as e :
                self.logger.warning(traceback.format_exc()+'对离线设备进行检测出错')
 
    def mkThred(self):
        '''
        创建线程去处理获取到的各个设备信息
        :return:
        '''
        T = threading.Thread(target=DeviceOffline.deviceStatus, args=(self,))

        t1 = threading.Thread(target=DeviceOffline.handle, args=(self,))

        t2 = threading.Thread(target=DeviceOffline.handle, args=(self,))

        t3 = threading.Thread(target=DeviceOffline.handle, args=(self,))

        T.start()
        t1.start()
        t2.start()
        t3.start()
        T.join()
        t1.join()
        t2.join()
        t3.join()

    def handle(self):
        '''
        根据提取的信息对用户是否已经下线进行判断
        :return:
        '''
        self.logger.warning('开始处理在线表中设备信息')
        # 时间容差
        TIME = 10   # s
        while True:
            try:
                data = self.QUEUE.get()
                sys_times, update_time, devicekey = data
                update_time = int(time.mktime(update_time.timetuple()))
                step = time.time()
                self.logger.info('现在的时差是{}-{}-{}:'.format(sys_times, update_time, TIME))
                if sys_times + update_time + TIME < step:
                    db = DB()
                    # 此时设备已下线，删除在线表中设备
                    db.delDevice(devicekey)
                    self.logger.warning('设备{}已经下线，在线表中该设备信息已经删除'.format(devicekey))
                else:
                    pass
            except Exception as e:
                self.logger.error('设备上下线问题出错，信息为：{}'.format(e))
                self.logger.warning(traceback.format_exc()+'判断用户是否下线时出错报错设备SN'+str(devicekey))


if __name__ == '__main__':
    deviceoffline = DeviceOffline()
    deviceoffline.mkThred()
