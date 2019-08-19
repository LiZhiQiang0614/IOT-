import time
import traceback
from mysqlhelper.mysqlhelper import DB
from log import TNLog


class Behaviors(object):
    """
    行为类:
        用于对客户端发送来的数据预处理
        例如:
            1.字符串转为python字典
            2.获取任务特征码
            3.验证客户端身份
            4.根据特征码请求数据库
            ...

    """

    def __init__(self):
        self.db = None
        self.logger = None

    @staticmethod
    def reverse_nonce(nonce):
        """
        将nonce数据逆转
        :param nonce:'123456'
        :return: 返回逆转之后的结果 <int>654321
        """
        nonce = str(nonce)[::-1]
        return int(nonce)

    @staticmethod
    def strAndDict(data):
        """
        执行数据转换,如果是字符串,那么就转换成为字典,如果之字典,那
        么就转换成为字符串
        :param data: 数据
        :return: 转换之后的数据
        """
        import json
        res = {}
        try:
            if isinstance(data, str):
                res = json.loads(data, encoding='utf-8')
            elif isinstance(data, dict):
                res = json.dumps(data, ensure_ascii=False)
            else:
                print('数据->{d}{t}<-不符合转换标准'.format(d=data, t=type(data)))
                res = {"asd": ''}
        except Exception as e:
            res = {"asd": ''}
        finally:
            return res



    def acitveSysn(self, data, deviceKey):
        """
        初始化设备之后要更新wifi信息,更新版本信息
        :param data:
        :param SN:
        :return:
        """
        big_info = {}
        new_version = data['body']['new_version']
        pre_version = data['body']['pre_version']
        if new_version == pre_version:
            self.logger.info('现在版本信息一致不需要更新表中的version信息!!')
            big_info.update({'status': 200})
            return big_info
        else:
            res = self.db.updateVersion(deviceKey, new_version)   # deviceinfo表中更新version版本信息
            if res:
                big_info.update({'status': 200})
                self.logger.info('<<<<同步数据命令提交成功,等待设备同步数据')
                # 这里添加删除99任务
                mod_id = data['body']['mod_id']
                status = data['body']['status']
                self.cleanTasks(mod_id=mod_id, SN=deviceKey, status=status)



                # 添加重启wifi模块的任务
                self.db.appendwifi(deviceKey)
                return big_info
            else:
                self.logger.info('<<<<<<同步数据失败')
                big_info.update({'status': 404})
                return big_info


    def version_analysis(self, version):
        '''
        对版本号进行解析，用于判断版本号
        :param version: 版本号
        :return: analysis解析后的版本值
        '''
        _ = str(version).split('b')[1]
        # print('>>>>>>>>>>>>>>>>_',_)
        analysis = _.split('t')[0]
        return analysis

    def compare(self, a: str, b: str):
        '''比较两个版本号的大小，需要按.分割后比较各个部分的大小'''
        lena = len(a.split('.'))  # 获取版本字符串的组成部分
        lenb = len(b.split('.'))
        a2 = a + '.0' * (lenb - lena)  # b比a长的时候补全a
        b2 = b + '.0' * (lena - lenb)
        # print(a2, b2)
        for i in range(max(lena, lenb)):  # 对每个部分进行比较，需要转化为整数进行比较
            if int(a2.split('.')[i]) > int(b2.split('.')[i]):
                return a
            elif int(a2.split('.')[i]) < int(b2.split('.')[i]):
                return b
            else:  # 比较到最后都相等，则返回第一个版本
                if i == max(lena, lenb) - 1:
                    return 0

    def activeDevice(self, data, SN):
        """ 
        根据设备发送来的数据,对设别验证后执行设备的激活操作
        1. 从数据库中查询token,active_flag,如果存在才能执行激活的逻辑
        :param data: 设备提交的数据
        :param SN: 设备的SN
        :return: 任务执行的结果 success:{"activate_status": 1} false:{"activate_status": 0}
        """
        # 查询激活设备标志：  0是未出场 1是可以首次激活  2是设备已完成首次激活
        data_sn = data['body']['devicekey']
        batt = data['body']['batt']
        rssi = data['body']['rssi']

        ###### 拿到设备传过来的product_id
        product_id = data['body']['product']


        data_sn = self.processingTokenOrSN(data_sn)
        data_sn = data_sn.strip()
        # 初始化激活状态返回值
        activate_res = {'activate_status': 0}
        self.logger.info('>>>>进入激活逻辑{}'.format(data_sn))
        self.logger.info('>>>>>>>现在的sn是{}'.format(SN))

        if data_sn == SN:
            self.logger.info('设备的sn号相同执行下一步操作++++++++++++++++++++++++++++')
            res_isactive_flag = self.db.active_device(SN)
            # 根据激活标识来激活设备
            if res_isactive_flag == 0:
                self.logger.info('<<<设备初始化激活状态是0,请联系厂家进行设备的出厂！')
            else:
                activate_res = {'activate_status': 1}
                if res_isactive_flag == 1:
                    # 平台初始化
                    self.db.initDeviceData(SN, product_id, batt, rssi)
                    # 更改设备的状态为激活状态
                    self.db.update_fist(SN)



                elif res_isactive_flag == 2:
                    self.logger.info('<<<获取到设备激活标志{}'.format(res_isactive_flag))
                    self.acitveSysn(data, product_id, SN)

                    # 查询数据库待执行任务中此设备执行状态为 2 的201任务
                    # ota_flag = self.db.select_wait_otatask(SN)
                    # if ota_flag > 0:
                    #     rom_version = data['body']['rom_version']
                    #     db_version = self.db.select_version(SN)
                    #     rom_ver = self.version_analysis(rom_version)  # 对设备的版本号解析
                    #     db_ver = self.version_analysis(db_version)  # 对数据库中版本号解析
                    #     res = self.compare(db_ver, rom_ver)
                    #     status = 0
                    #     if res == rom_ver:
                    #         # 设备版本号高,升级成功，修改数据库中版本信息并把待执行任务ota升级的run_flag为0的状态改为status=0
                    #         self.db.ota_version_update(rom_version, SN)
                    #         status = 200
                    #     else:
                    #         # 设备版本号跟数据库之前版本一致待执行任务ota升级的执行次数不超出重复上限刷新任务状态重复执行
                    #         self.logger.info('当前设备OTA升级任务已刷新')
                    #     self.cleanTasks(201, SN, status)
                    self.logger.info('<<<当前设备{}已被激活初始化,刷新成功'.format(SN))
        return activate_res



    def judgingTaskType(self, SN, product):
        """
        判断任务类型
        根据解析后的数据中的taskId来判断任务类型
        类型有码:
            -. 101 温湿度设备修改配置
        :param SN 设备的SN号,设备的唯一标识
        :return: res 执行任务需要的参数 task_type 任务类型码
        """
        # 根据SN获取设备的待执行任务
        try:
            modeID = self.db.query_task(SN)   # 待执行任务的id
            modeID = int(modeID)
            self.logger.info('<<<获取到设备{}的待执行任务,任务码{}任务码类型{}'.format(SN, modeID, type(modeID)))
            get_task_res = {'mod_id': modeID}
            # 补充客户端返回数据
            if modeID == 0:
                get_task_res = {}


            # 温湿度数据同步
            elif modeID == int(101):
                info_set = self.db.synchronization(SN)
                wifi_set = self.db.wifisynchronization(SN)
                if info_set and wifi_set:
                    self.logger.info('>>>获取初始化阈值的信息是：{}'.format(info_set))
                    get_task_res.update({
                        "a": info_set[0],
                        "b": info_set[1],
                        "c": info_set[2],
                        "d": info_set[3],
                        "e": info_set[4],
                        "f": info_set[5],
                        "g": info_set[6],
                        "h": info_set[7],
                        "i": info_set[8],
                        "j": info_set[9],
                        "k": info_set[10],
                        "l": info_set[11],
                        "m": wifi_set[0]
                    })
                else:
                    self.logger.info('>>>接口获取数据同步数据失败<<<')
                pass


            # OTA升级  :拿到设备的版本信息返回给设备
            elif modeID == int(99):
                v_info = self.db.vinfo(product)
                if v_info:
                    get_task_res.update({'a': v_info[0]})

                # rom_version = data['body']['rom_version']
                # db_version = self.db.select_version(SN)
                # rom_ver = self.version_analysis(rom_version)  # 对设备的版本号解析
                # db_ver = self.version_analysis(db_version)  # 对数据库中版本号解析
                # res = self.compare(db_ver, rom_ver)
                # status = 0
                # if res == rom_ver:
                #     # 设备版本号高,升级成功，修改数据库中版本信息并把待执行任务ota升级的run_flag为0的状态改为status=0
                #     self.db.ota_version_update(rom_version, SN)
                #     status = 200
                # else:
                #     # 设备版本号跟数据库之前版本一致待执行任务ota升级的执行次数不超出重复上限刷新任务状态重复执行
                #     self.logger.info('当前设备OTA升级任务已刷新')
                # self.cleanTasks(201, SN, status)
                # self.logger.info('<<<当前设备{}已被激活初始化,刷新成功'.format(SN))


            # 扫描wifi
            elif modeID == int(98):
                self.logger.info('现在走到了任务查询的wifi扫描任务id等于98这里')
                try:
                    get_task_res.update({'status': 200})
                except Exception as e:
                    self.logger.debug('获取wifi信息错误')
                    self.logger.error('错误信息是{}'.format(e))


            # 配置wifi
            elif modeID == int(95):
                self.logger.info('现在走到了任务设置wifi的信息')
                put_wifi_info = self.db.put_wifi(SN)
                if put_wifi_info.get('status'):
                    self.logger.info('现在的获取信息为：{}'.format(put_wifi_info))
                    get_task_res.update({'status': 405})
                    return get_task_res
                else:
                    self.logger.info('现在拿到了wifi信息：{}'.format(put_wifi_info))
                    get_task_res.update({
                        'a': put_wifi_info['name'],
                        'b': put_wifi_info['password']
                    })


            elif modeID == int(103):
                info_set = self.db.synchronizationy(SN)
                wifi_set = self.db.wifisynchronization(SN)
                if info_set and wifi_set:
                    self.logger.info('>>>获取初始化阈值的信息是：{}'.format(info_set))
                    get_task_res.update({
                        "a": info_set[0],
                        "b": info_set[1],
                        "c": info_set[2],
                        "d": info_set[3],
                        "e": info_set[4],
                        "f": info_set[5],
                        "g": info_set[6],
                        "h": info_set[7],
                        "i": info_set[8],
                        "j": info_set[9],
                        "k": info_set[10],
                        "l": info_set[11],
                        "m": info_set[12],
                        "n": info_set[13],
                        "o": wifi_set[0]
                    })
                else:
                    self.logger.info('>>>接口获取数据同步数据失败<<<')
                pass


            # 主动获取wifi
            elif modeID == int(94):
                get_task_res.update({'status': 200})


            # 重启wifi模块任务
            elif modeID == int(93):
                get_task_res.update({'status': 200})

            else:
                self.logger.info(">>>get task id err!<<<")
                get_task_res = {}

            self.logger.info('嘿嘿：{}'.format(get_task_res))
            return get_task_res
        except Exception as e:
            self.logger.debug('>?>?>?>?>?>?>执行待执行任务表中的任务出错：{}'.format(e))
            self.logger.error('>?>?>?>?>?>?>执行待执行任务表中的任务出错：{}'.format(e))


    def validate(self, token, SN_from_device):
        """
        设备认证
        :param: token 为字符串,形如:'token xxxxxxx'
        :return: Boolean True or False
        """
        self.logger.info('>>>正在进行会话验证,设备提交的SN为:{}'.format(SN_from_device))
        SN_for_DB = self.db.validate_token(token)
        if not SN_for_DB:
            self.logger.info('<<<验证失败,无法从数据库获取到设备')
            return False
        else:
            if SN_for_DB[0] == SN_from_device and SN_for_DB[1] == 2:
                self.logger.info('<<<验证成功,设备一致')
                return True
            else:
                self.logger.info('<<<验证失败,设备不一致')
                return False

    def cleanTasks(self, mod_id, SN, status):
        """
        清空已完成的任务
        :param taskId 命令码
        :param SN 设备编号
        :param status wifi状态
        :return: success:{'status':200} false:{'status':403}
        """
        clean_task_res = self.db.clear_task(mod_id, SN, status)
        self.logger.info('>>>>>正在清除taskId：{}任务'.format(mod_id))
        self.logger.debug('>>>>>现在返回的clean_task_res的数据为{}'.format(mod_id))
        if clean_task_res.get('mod_id'):
            self.logger.info('现在返回设备的信息是{}'.format(clean_task_res))
            return clean_task_res
        elif clean_task_res:
            return {'status': 200}
        else:
            return {'status': 405}

    def performDataInsertion(self, data, SN):
        """
        token验证成功之后,执行数据插入到数据库的操作
        :param data: 收集的数据
        :return: 状态码
        """
        body = data['body']
        # 只插入业务数据
        insert_flag = self.db.activate_insert(body, SN)
        if insert_flag:
            return insert_flag
        else:
            return {'status': 406}


    def processingTokenOrSN(self, old_data):
        _ = old_data.split(' ')
        new_data = _[1].strip()
        return new_data









    def validateToken(self, data):
        '''
        第二步会话token验证
        :param data:
        :return:
        '''
        old_token = data['meta']['Authorization']
        token = self.processingTokenOrSN(old_token)  # 得到的就是token-->一串数字
        res_validate = False
        try:
            sn = self.db.validate_token(token)
            return sn
        except Exception as e:
            self.logger.info('<<<Token验证异常')
            self.logger.debug(e)
            self.logger.error(traceback.format_exc()+'报错设备token为'+str(token))
            return None

    def processingPath(self, path, SN, data):
        """
        处理path数据, 预处理结果为['A','B','C',...]
        :param path: {'path':'/A/B/C/'}
        :param SN: 设备SN
        :param data: 设备发过来的原始数据
        :return:
        """
        self.logger.info('>>>执行路径匹配,设备提交的path为:{}'.format(path))
        res = {'status': 0}

        _ = path.strip('/')
        _ = _.split('/')
        self.logger.info('现在获取到路径为：{}'.format(_))

        # 拿到激活标志判断如果没有激活就走插入逻辑返回错误提示
        act_fla = self.db.activation(SN)
        act_flag = int(act_fla)
        for action in _:
            if action == 'v1' or action == 'device':
                continue

            # 执行激活操作
            elif action == 'activate':
                self.logger.info('>>>正在执行激活操作')
                res = self.activeDevice(data=data, SN=SN)


            elif action == 'datapoints':
                try:
                    if act_flag == int(2):
                        # 执行数据库的插入操作
                        self.logger.info('>>>执行数据插入,SN:{}'.format(SN))
                        self.logger.info('''
                            >>>数据为:
                            {}
                            <<<
                        '''.format(data))
                         # 检查在线表设备是否存在
                        result = self.db.select_device(SN)
                        if result:
                        # 设备在线更新最新交互时间
                            self.db.update_time(SN)
                        else:
                            # 设备上线将相关数据插入到在线表
                            self.db.insert_devicedata(SN)
                        res = self.performDataInsertion(data=data, SN=SN)   # 执行数据插入数据库的操作
                        self.logger.info('>>>>执行数据插入操作的返回状态为{}<<<<'.format(res))
                    else:
                        res['status'] = 402
                        # 重复激活之前删除初始化的数据
                        self.db.clean_init(SN)
                except Exception as e:
                    self.logger.debug('现在数据插入时候验证是否激活出错：{}'.format(e))
                    self.logger.error('现在数据插入时候验证是否激活出错：{}'.format(e))


            elif action == 'get_wifi':
                # 执行扫描wifi操作
                wifi_information = data['body']
                wifi_status = wifi_information['status']
                wifi_count = wifi_information['count']
                wifi_data = wifi_information['wifidata']

                self.logger.info('''
                    >>>
                    从设备中获取到的wifi信息为：
                    {}
                    <<<
                '''.format(wifi_information))
                self.logger.info('>>>>wifi数据为{}{}'.format(wifi_status, wifi_count))
                self.logger.info('>>>>wifi_data====={}'.format(wifi_data))
                self.logger.info('>>>>将执行数据库命令<<<清理任务')
                res.update({'status': 200})


            elif action == 'set_status':
                status = data['body']['status']
                mod_id = data['body']['mod_id']
                res = self.cleanTasks(mod_id=mod_id, SN=SN, status=status)

            # 设置wifi
            elif action == 'put_wifi':
                info = data['body']['apname']
                status = data['body']['status']
                mod_id = int(94)
                res = self.db.setwifi(info, status, mod_id, SN)

            # 收到wifi信息路径
            elif action == 'ota_status':
                self.logger.info('这是OTA升级的路径!!!')
                res = self.acitveSysn(data, SN)



            else:
                # 执行任务失败
                res = {'status': 404}

        return res




    def deviceRun(self, str_msg):
        """
        设备主函数,主要功能包括:
            1. 数据解析(转换json)
            2. 分支判断
                1. 走激活路线?
                2. 走数据上传路线?
        :param str_msg:
        :return:res处理动作的状态,data['nonce']标识
        """
        # 数据异常时默认count值
        self.logger.info('哈哈哈哈哈{}'.format(str_msg))
        system_count = 1
        data = Behaviors.strAndDict(str_msg)   # str或者字典
        # 判断解析之后数据是否为空数据,空数据就不再执行以后的逻辑
        self.logger.info('hi嗯好IEhi嗯好IE{}'.format(data))
        if data:
            self.logger.info(
                '''
                ==获取客户端数据==:
                ↓
                {}
                ↑
                ==获取客户端数据==
                                '''.format(data)
            )
            # 通过判断数据来防止恶意访问
            if not data.get('path') or not data.get('body') or not data.get('meta'):
                res = {'activate_status': 0, 'status': 401, 'count': system_count}
                return res, None, None

            # 查询是否有待执行任务,根据data['body']['count'] == 0,以及初始化的返回值状态,来确定是否查询待执行任务
            device_count = data['body']['count']
            product = data['body']['product']

            flag = data['meta'].get('Authorization', None)
            self.logger.info('>>>>解析data中token==={}'.format(flag))
            if flag:
                # 验证token  返回的是sn号
                SN = self.validateToken(data)
                # 第三部token验证成功解析业务
                if SN:
                    """
                    init_count
                    获取数据库中的执行次数作为初始count
                    如果能够获取到任务,那么count = 0
                    如果不能够获取到任务,那么count初始为初始count
                    """
                    # 更新设备信息最近一次的设备数据，rssi,batt,最后一次与平台交互时间
                    # 如果是响应包则跳过更新
                    body = data['body']

                    status = body.get('status', None)
                    if status is None:
                        rssi = body['rssi']
                        batt = body['batt']
                        self.db.update_data(SN, rssi, batt)     # 更新设备最后的一次平台交互的时间
                    init_count = self.db.get_device_count(SN)   # 设备的同步周期 --> 返回的是数字
                    # 处理路径<动作>
                    old_path = data['path']
                    self.logger.info('处理路径下的路径最原始的是：{}'.format(old_path))

                    # 第三步,解析路径进行业务处理
                    res = self.processingPath(path=old_path, SN=SN, data=data)
                    self.logger.info('第三步，解析路径进行业务处理res====={}'.format(res))

                    # 第四步待执行任务查寻
                    if device_count == 0:
                        self.logger.info('>>>count:{}正在查询({})设备的待执行任务'.format(device_count, SN))
                        # 获取任务,如果获取到任务,那么将去处理任务
                        get_task_res = self.judgingTaskType(SN, product)
                        # 如果没有任务,那么判断结果为空{},如果有任务,那么返回{'action':任务类型,....数据}
                        if get_task_res:
                            res.update(get_task_res)
                            # OTA升级的暂时掠过
                            if get_task_res['mod_id'] == 99:
                                count = 0
                            else:
                                # 有任务,需要服务端持续连接客户端
                                count = 0
                            res.update({'count': count})
                        else:
                            # 将count设置成初始化的count
                            res.update({'count': init_count})
                        self.logger.info('现在：{}'.format(res))
                        self.logger.info('将来：{}'.format(get_task_res))
                        self.logger.info('''
                                >>>
                                执行task返回数据{}
                                path处理的结果为{}
                                <<<
                                                        '''.format(get_task_res, res))

                    else:
                        res.update({'count': device_count})
                        self.logger.info('''
                                >>>
                                path处理的结果为{},count=={}
                                <<<
                                                        '''.format(res, device_count))
                    return res, data['nonce'], device_count


            else:
                # token验证失败,拒绝连接
                res = {'activate_status': 0, 'status': 402, 'count': system_count}
                return res, data['nonce'], device_count
        else:
            res = {'activate_status': 0, 'status': 401, 'count': system_count}
            # 第五步返回客户端响应数据
            return res, None, None

    def __call__(self, client_sock, client_ip):
        self.db = DB()
        self.logger = TNLog()
        # 标识位,主动切断和客户端的通信
        CUTOFF = False
        # 轮询此时为16
        COUNT = 16
        res = {'status': 201}
        self.logger.info("===正在执行任务===")
        # 获取数据<byte>
        while True:
            nonce = ''
            device_count = ''
            try:
                byte_msg = client_sock.recv(1024)  # 接收到的数据
                str_msg = byte_msg.decode('utf-8')

                self.logger.info('>>>>>>>>>>>>>>>>>>str_msg==={}'.format(str_msg))  # 日志记录
                # 解析数据,处理业务请求
                res, nonce, device_count = self.deviceRun(str_msg)
                res['nonce'] = nonce
            except Exception as e:
                self.logger.error(traceback.format_exc())
                self.logger.debug(e)
                count = res.get('count', None)
                if count:
                    res = {'status': 401, 'count': count}
                else:
                    res = {'status': 401, 'count': COUNT}

            finally:
                if res['count'] == 0 or res['count'] == 1:

                    # 第六步返回客户端响应
                    str_send_msg = Behaviors.strAndDict(res)
                    self.logger.info(
                        '''
                     =======================
                     服务端向客户端发送的数据:
                     time:{t}  server-->client
                     msg:{m}
                     =======================
                                         '''.format(t=time.ctime(), m=str_send_msg)
                    )
                    client_sock.send(str_send_msg.encode('utf-8'))

                else:
                    if device_count == 0:
                        str_send_msg = Behaviors.strAndDict(res)
                        client_sock.send(str_send_msg.encode('utf-8'))
                    else:
                        self.logger.info('<<第六步无需发送数据，res{}>>'.format(res))
                    self.logger.info('>>>>>>>结束本次任务，断开连接')
                    client_sock.close()
                    self.logger.info('''
                        >>>
                        -服务端主动与客户端断开连接
                        -客户端没有待处理的命令
                        -等待客户端的主动连接~~~~
                        <<<
                                            ''')
                    break
        self.logger.info('>>>>>>>>>>>会话断开')
