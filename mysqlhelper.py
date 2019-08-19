# encoding = utf-8
import traceback
import pymysql
import sys
import time
import json
from log import TNLog


class DB(object):
    # MYSQL_HOST = "47.104.254.187"
    # MYSQL_HOST = "192.168.0.250"
    MYSQL_HOST = "127.0.0.1"
    MYSQL_PORT = 30388  # 必须为int
    # MYSQL_USER = "develop"
    MYSQL_USER = "version7"
    # MYSQL_PASS = "xunming123"
    # MYSQL_PASS = "123456"
    MYSQL_PASS = "vv77vv77vv77107"
    # MYSQL_DB = "db_xunmingiot4"
    MYSQL_DB = 'version7'
    # MYSQL_DB = 'db_local'

    # 失败任务重复执行次数上限
    TIMES = 2
    # 电池电量阈值
    BATT = 300


    def __init__(self):
        try:
            self.conn = pymysql.connect(
                host=self.MYSQL_HOST,
                port=self.MYSQL_PORT,
                user=self.MYSQL_USER,
                password=self.MYSQL_PASS,
                db=self.MYSQL_DB,
                # charset=self.MYSQL_CHR #数据库编码设置不正确会出错
            )
            self.cursor = self.conn.cursor()
            self.logger = TNLog()
        except Exception as e:
            self.logger.error("连接数据库时出错: %s" % e)
            sys.exit(1)
        else:
            self.logger.info("success MYSQL!")


    def update_data(self, SN, rssi, batt):
        '''
        通过查询状态码来判断是否更新还是第一次激活
        在告警表里查到设备告警信息干掉表示设备上线
        更新设备信息最近一次的设备数据，rssi,batt,最后一次与平台交互时间
        '''
        try:
            stemp = int(time.time())
            # 拿到设备的激活数值
            sql_one = "SELECT activate_flag FROM devicetoken WHERE device_id=%s"
            select_res = self.cursor.execute(sql_one, (SN,))
            if not select_res:
                return
            one_info = self.cursor.fetchone()[0]
            if one_info == 1:
                sql = "INSERT INTO c_d_0 (rssi,battery,update_time,device_id)VALUES(%s,%s,%s,%s)"
                self.cursor.execute(sql, (rssi, batt, stemp, SN,))
                self.conn.commit()
                self.logger.info('>>>>>>>>插入c_d_0表中数据')

                sql2 = "INSERT INTO h_d_0(rssi, battery, device_id) VALUES (%s,%s,%s)"
                self.cursor.execute(sql2, (rssi, batt, SN))
                self.conn.commit()
                self.logger.info('>>>>>>>>>>电量设备的信息首次激活插入成功')

            elif one_info == 2:
                sql = "UPDATE c_d_0 SET rssi=%s,battery = %s,update_time = %s WHERE device_id = %s"
                self.cursor.execute(sql, (rssi, batt, stemp, SN))
                self.logger.info('>>>>>>>>更新c_d_0表中数据rssi==={}battery===={}'.format(rssi, batt))

                # 告警表里去差把当前设备离线的告警干掉
                sql = "SELECT warning_type FROM warninglog WHERE device_id = %s AND warning_type = '设备离线'"
                self.cursor.execute(sql, (SN,))
                res = self.cursor.fetchone()
                if res:
                    sql = "DELETE FROM warninglog WHERE device_id = %s AND warning_type = '设备离线'"
                    self.cursor.execute(sql, (SN,))
                    self.conn.commit()
                    self.logger.warning('{}设备离线告警已经删除'.format(SN))
                else:
                    pass

                sql2 = "INSERT INTO h_d_0(rssi, battery, device_id) VALUES (%s,%s,%s)"
                self.cursor.execute(sql2, (rssi, batt, SN))
                self.conn.commit()

            self.logger.info('《》《》《》《现在他娘的激活还是啥的分开了》《》《》')
        except Exception as e:
            self.logger.debug(e)
            self.logger.error(traceback.format_exc() + str(SN))

    def active_device(self, body_devicekey):
        """
        提供设备SN和mac地址去获取设备的激活信息
        :param body_bssid: MAC地址
        :param body_devicekey:  设备SN
        :return: 1,1 激活 0,0不可激活
        """
        try:
            sql = "SELECT activate_flag FROM devicetoken WHERE device_id=%s;"
            self.logger.info(self.cursor.mogrify(sql, (body_devicekey,)))
            results = self.cursor.execute(sql, (body_devicekey))
            self.logger.info('>>>>{}<<<<<'.format(results))
            if results == 1:
                activeFlag = self.cursor.fetchone()[0]
                return activeFlag
            else:
                return 0
        except Exception as e:
            self.logger.debug(e)
            self.logger.error(traceback.format_exc() + '报错设备SN' + str(body_devicekey))

    def update_fist(self, deviceid):
        '''
        设备首次激活，将设备默认表中的信息同步到用户设备表
        :return:
        '''
        try:
            import time
            st_time = time.strftime('%Y-%m-%d %X')
            update_flag = "UPDATE devicetoken SET activate_flag=2 WHERE device_id=%s "
            update_activate_time = 'UPDATE deviceinfo SET activate_time=%s WHERE device_id=%s'
            self.logger.info('>>>>>>将设备激活标志改为2')
            self.cursor.execute(update_flag, (deviceid,))
            self.cursor.execute(update_activate_time, (st_time, deviceid,))
            self.conn.commit()
        except Exception as e:
            self.logger.debug('初次激活同步数据失败{}'.format(e))
            self.logger.error(traceback.format_exc() + '报错设备SN' + str(deviceid))

    def query_task(self, deviceKey):
        """
        查询待执行任务记录，并返回任务id
       """
        # 1、查询待执行任务
        try:
            step = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
            sql = "SELECT mode_id_id FROM waitexecution WHERE device_id = %s ORDER BY id DESC"
            tmp = self.cursor.execute(sql, (deviceKey,))
            self.logger.info('查询待执行任务的mod_id==={}'.format(tmp))
            if tmp:
                self.logger.info('>>>>>待执行任务有数据{}'.format(tmp))
                modeID = self.cursor.fetchone()[0]
                if int(modeID) == int(101):
                    mname = '修改配置'
                elif int(modeID) == int(99):
                    mname = 'ota升级'
                elif int(modeID) == int(95):
                    mname = '配置wifi'
                elif int(modeID) == int(103):
                    mname = '烟雾修改配置'
                elif int(modeID) == int(94):
                    mname = '接收wifi'
                elif int(modeID) == int(93):
                    mname = '重启wifi'
                else:
                    mname = '啥命令啊'

                # 取重复任务的最大值
                sql = "SELECT MAX(repetitions) FROM waitexecution WHERE device_id = %s AND mode_id_id = %s"
                self.cursor.execute(sql, (deviceKey, modeID,))
                res = self.cursor.fetchone()[0]
                if int(res) >= int(1):
                    if modeID == int(94) or modeID == int(95):
                        sql = "DELETE FROM waitexecution WHERE device_id = %s AND mode_id_id = %s"
                        self.cursor.execute(sql, (deviceKey, modeID,))
                        sql = "INSERT INTO waitexecution_his(mode_id,create_time,`status`,devicekey_id,`desc`)VALUES(%s,%s,2,%s,%s)"
                        self.cursor.execute(sql, (modeID, step, deviceKey, '任务超时',))
                        self.conn.commit()
                        self.logger.info('现在已经删除了执行任务次数超过2次的重复任务！')
                        return modeID
                    else:
                        sql = "DELETE FROM waitexecution WHERE device_id = %s AND mode_id_id = %s"
                        self.cursor.execute(sql, (deviceKey, modeID,))
                        sql = "INSERT INTO waitexecution_his(mode_id,create_time,`status`,devicekey_id,`desc`)VALUES(%s,%s,2,%s,%s)"
                        self.cursor.execute(sql, (modeID, step, deviceKey, mname,))
                        self.conn.commit()
                        self.logger.info('现在已经删除了执行任务次数超过2次的重复任务！')
                        return modeID

                update_flag = "UPDATE waitexecution SET run_flag = 2 WHERE device_id = %s  AND mode_id_id = %s"
                self.cursor.execute(update_flag, (deviceKey, modeID,))
                self.logger.info('>>>>>完成更改待执行任务表的执行状态为：{}>>>>'.format(2))
                update_repe = "UPDATE waitexecution SET repetitions = repetitions + 1 WHERE run_flag = 2 AND device_id = %s AND mode_id_id = %s"
                self.cursor.execute(update_repe, (deviceKey, modeID,))
                self.logger.info('<<<<现在完成更改待执行任务表中的执行次数+1<<<<<')
                # 提交更改
                self.conn.commit()
                # 3、返回提取的任务id
                self.logger.info('查询到任务码==={}'.format(modeID))
                return modeID
            else:
                self.logger.info('没有查询到任务id')
                return 0
        except Exception as e:
            self.logger.debug(e)
            self.logger.error(traceback.format_exc() + '报错设备SN' + str(deviceKey))

    def validate_token(self, data):
        """
        每次连接验证token是否正确
        """
        try:
            sql = "SELECT device_id FROM devicetoken WHERE token=%s;"
            tmp = self.cursor.execute(sql, (data,))  # 必须是元组形式
            if tmp:
                SN = self.cursor.fetchone()[0]
                self.logger.info("<<<从数据库获取到的SN为: %s<<Token验证成功!!" % SN)
                return SN
        except Exception as e:
            self.logger.debug("errer: db.py validate_token %s" % e)
            self.logger.error(traceback.format_exc() + '报错设备token为' + str(data))




    def activation(self, deviceKey):
        '''
        判断设备是否激活，只有激活才能插入数据
        '''
        try:
            sql = "SELECT activate_flag from devicetoken WHERE device_id = %s"
            tmp = self.cursor.execute(sql, (deviceKey,))  # 必须是元组形式
            if tmp:
                act_flag = self.cursor.fetchone()[0]
                self.logger.info("<<<从数据库获取到的激活标志为: %s<<是否激活验证成功!!" % act_flag)
                return act_flag
        except Exception as e:
            self.logger.debug('在devicetoken中验证是否激活失败：%s' % e)
            self.logger.error('在devicetoken中验证是否激活失败:%s' % e)


    # v7版本数据库温湿度数据的同步
    def synchronization(self, deviceKey):
        try:
            sql = "SELECT up_limit_tem,down_limit_tem,tem_offset,tem_rt,up_limit_hum,down_limit_hum,hum_offset,hum_rt,send_times,record_times,syn_times,lcd_id FROM conf_u_101 WHERE device_id = %s"
            tmp = self.cursor.execute(sql, (deviceKey,))
            if tmp:
                infoSet = self.cursor.fetchone()
                self.logger.info("新版数据库的温湿度数据同步信息是{}".format(infoSet))
                return infoSet
            else:
                return 0
        except Exception as e:
            self.logger.debug(e)
            self.logger.error(traceback.format_exc() + '报错设备SN' + str(deviceKey))


    def synchronizationy(self, deviceKey):
        try:
            sql = "SELECT up_limit_smoke,smoke_offset,smoke_rt,up_limit_tem,up_limit_hum,down_limit_tem,down_limit_hum," \
                  "tem_offset,hum_offset,tem_rt,hum_rt,send_times,record_times,syn_times FROM conf_u_103 WHERE device_id = %s"
            tmp = self.cursor.execute(sql, (deviceKey,))
            if tmp:
                infoSet = self.cursor.fetchone()
                self.logger.info("新版数据库的温湿度数据同步信息是{}".format(infoSet))
                return infoSet
            else:
                return 0
        except Exception as e:
            self.logger.debug(e)
            self.logger.error(traceback.format_exc() + '报错设备SN' + str(deviceKey))


    # 拿取表中的wifi名字
    def wifisynchronization(self,deviceKey):
        try:
            sql = "SELECT rssi_name FROM c_d_0 WHERE device_id = %s"
            tmp = self.cursor.execute(sql, (deviceKey,))
            if tmp:
                wifinfoset = self.cursor.fetchone()
                self.logger.info('现在拿到了设备最新的wifi名称：{}'.format(tmp))
                return wifinfoset
        except Exception as e:
            self.logger.debug('拿设备最新wifi名称出错！{}'.format(e))
            self.logger.error('拿设备最新wifi名称出错！{}'.format(e))



    # 通过平台待执行任务拿到wifi信息
    def put_wifi(self, deviceKey):
        try:
            sql = "SELECT meta FROM waitexecution WHERE device_id = %s"
            tmp = self.cursor.execute(sql, (deviceKey,))
            if tmp:
                wifiinfo = self.cursor.fetchone()[0]
                wifiinfo = json.loads(wifiinfo)
                self.logger.info('现在拿到设备的wifi信息：{}'.format(wifiinfo))
                return wifiinfo
            else:
                return {'status': 405}
        except Exception as e:
            self.logger.debug('wifi信息取值错误：{}'.format(e))
            self.logger.error('wifi信息取值错误：{}'.format(e))




    # v7版本数据库温湿度烟雾数据的同步
    def synchronizations(self, deviceKey):
        try:
            sql = "SELECT up_limit_smoke,smoke_offset,smoke_rt,up_limit_tem,up_limit_hum,down_limit_tem,down_limit_hum," \
                  "tem_offset,hum_offset,tem_rt,hum_rt,send_times,record_times,syn_times FROM conf_u_103 WHERE device_id = %s"
            tmp = self.cursor.execute(sql, (deviceKey,))
            if tmp:
                infoSet = self.cursor.fetchone()
                self.logger.info("新版数据库的温湿度数据同步信息是{}".format(infoSet))
                return infoSet
            else:
                return 0
        except Exception as e:
            self.logger.debug(e)
            self.logger.error(traceback.format_exc() + '报错设备SN' + str(deviceKey))

    # 查询最新产品的版本
    def vinfo(self, product):
        try:
            sql = "SELECT version_nu FROM versioninfo WHERE vid = (SELECT version_info_id FROM versionnow WHERE product_id = %s)"
            tem = self.cursor.execute(sql, (product,))
            if tem:
                vinfo = self.cursor.fetchone()
                self.logger.info('现在拿到了设备的最新版本数据：{}'.format(vinfo))
                return vinfo
            else:
                return 0
        except Exception as e:
            self.logger.debug(e)
            self.logger.error('取设备最新版本信息出错，出错问题是{}'.format(e))



    def get_alarm_set(self, deviceKey):
        """
        查询当前节点的告警设置
        """
        # 1、查询待执行任务
        # sql,查询到指定的数据
        try:
            sql = "SELECT up_limit_tem,down_limit_tem,up_limit_hum,down_limit_hum FROM conf_u_101 WHERE device_id = %s"
            # self.logger.info("执行的sql语句：%s" % runSql)
            tmp = self.cursor.execute(sql, (deviceKey,))
            if tmp:
                alarmSet = self.cursor.fetchone()
                self.logger.info("get_alarm_set= %s ,devicekey=%s" % (alarmSet, deviceKey))
                return alarmSet
            else:
                return 0
        except Exception as e:
            self.logger.debug(e)
            self.logger.error(traceback.format_exc() + '报错设备SN' + str(deviceKey))

    def get_smoke_set(self, deviceKey):
        try:
            sql = "SELECT up_limit_smoke FROM user_device_config_smoke WHERE device_id = %s"
            smo = self.cursor.execute(sql, (deviceKey,))
            if smo:
                smoke_set = self.cursor.fetchone()
                self.logger.info('《《《《《《《《《《《《《拿到烟雾设备的上限')
                return smoke_set
            else:
                return 0
        except Exception as e:
            self.logger.debug(e)
            self.logger.error(traceback.format_exc() + '取值烟雾信息报错设备SN' + str(deviceKey))

    def get_alarm_Info_set(self, deviceKey):
        """
        查询当前节点的传感器偏移量、告警回差设置设置
        """
        # 1、查询待执行任务
        try:
            sql = "SELECT tem_offset,tem_rt,hum_offset,hum_rt FROM conf_u_101 WHERE device_id = %s"  # sql,查询到指定的数据
            runSql = self.cursor.mogrify(sql, (deviceKey,))
            # self.logger.info("执行的sql语句：%s" % runSql)
            tmp = self.cursor.execute(sql, (deviceKey,))
            if tmp:
                alarmSet = self.cursor.fetchone()
                self.logger.info("get_alarm_info_set= %s ,devicekey=%s" % (alarmSet, deviceKey))
                return alarmSet
            else:
                return 0
        except Exception as e:
            self.logger.debug(e)
            self.logger.error(traceback.format_exc() + '报错设备SN' + str(deviceKey))
            # self.logger.error('报错设备SN为 %s' % deviceKey)

    def get_smoke_info_set(self, deviceKey):
        try:
            sql = "SELECT smoke_offset,smoke_rt FROM user_device_config_smoke WHERE device_id = %s"
            tmp = self.cursor.execute(sql, (deviceKey,))
            if tmp:
                smokSet = self.cursor.fetchone()
                self.logger.info("烟雾的偏移量已经获取到")
                return smokSet
            else:
                return 0
        except Exception as e:
            self.logger.debug(e)
            self.logger.error(traceback.format_exc() + '烟雾偏移量信息获取失败，报错设备SN' + str(deviceKey))

    def clear_task(self, mod_id, deviceKey, status):
        """
        清除已完成的任务
        """
        res = False
        if int(mod_id) == int(101):
            mname = '修改配置'
        elif int(mod_id) == int(99):
            mname = 'ota升级'
        elif int(mod_id) == int(95):
            mname = '配置wifi'
        elif int(mod_id) == int(103):
            mname = '烟雾设备修改配置'

        else:
            mname = '啥命令啊'
        try:
            # 修改命令执行记录表中的执行状态
            # 1 从待执行任务表中获取到任务id
            task_select = "SELECT repetitions FROM waitexecution WHERE device_id=%s AND mode_id_id=%s AND run_flag=2"
            select_res = self.cursor.execute(task_select, (deviceKey.strip(), mod_id,))
            data = self.cursor.fetchone()
            if data:
                repetitions = data[0]
            else:
                repetitions = self.TIMES

            res = {}
            step = time.strftime("%Y-%m-%d", time.localtime(time.time()))
            if status == 200:
                try:
                    sql_insert = "INSERT INTO waitexecution_his (`mode_id`,`devicekey_id`,`status`,`desc`) VALUES (%s,%s,%s,%s)"
                    self.cursor.execute(sql_insert, (mod_id, deviceKey, 3, mname,))
                    # 删除待执行任务表中的相应的数据
                    delete_sql = "DELETE FROM waitexecution WHERE device_id=%s AND run_flag=2 AND mode_id_id=%s"
                    self.logger.info('>>>已删除执行过的任务编号{}--类型{}')
                    self.cursor.execute(delete_sql, (deviceKey, mod_id,))
                    self.conn.commit()
                    if mod_id == int(95):
                        sql = "INSERT INTO waitexecution (run_flag,repetitions,device_id,mode_id_id,`desc`) VALUES (1,0,%s,94,'配置wifi')"
                        self.cursor.execute(sql, (deviceKey,))
                        self.conn.commit()
                        self.logger.info('现在他娘的插入了一条任务得到wifi名已经插入成功了')
                        res.update({'status': 200})
                    else:
                        res.update({'status': 200})
                    self.logger.info('现在返回设备的信息是{}'.format(res))
                except Exception as e:
                    self.logger.error('任务执行有错误{}'.format(e))
                    self.logger.debug('任务执行有错误')
            else:
                if repetitions < self.TIMES:
                    if select_res:
                        # 任务执行失败，将待执行任务run_flag改为1，历史任务status改为0
                        sql1 = "UPDATE waitexecution SET run_flag=1,repetitions=repetitions+1 WHERE device_id=%s AND mode_id_id=%s"
                        self.cursor.execute(sql1, (deviceKey, mod_id,))
                        self.logger.info('>>>任务执行失败，任务状态已经更新，类型{}'.format(mod_id))
                        self.conn.commit()
                        res.update({'status': 200})
                else:
                    sql_insert = "INSERT INTO waitexecution_his (`devicekey_id`,`status`,`mode_id`,`desc`) VALUES (%s,%s,%s,%s)"
                    self.cursor.execute(sql_insert, (deviceKey, 2, mod_id, mname,))
                    delete_sql = "DELETE FROM waitexecution WHERE device_id=%s AND run_flag=2 AND mode_id_id=%s"
                    self.cursor.execute(delete_sql, (deviceKey, mod_id,))
                    self.conn.commit()
                    self.logger.info('>>>已删除超出重复次数的任务{}')
                    # res = True
                    res.update({'status': 405})
        except Exception as e:
            self.conn.rollback()
            self.logger.debug('>>错误{}<<'.format(e))
            self.logger.error(traceback.format_exc() + '报错设备SN' + str(deviceKey))
        finally:
            return res



    def clear_tasks(self, mod_id, deviceKey, status):
        """
        清除已完成的任务
        """
        try:
            # 修改命令执行记录表中的执行状态
            # 1 从待执行任务表中获取到任务id
            task_select = "SELECT repetitions FROM waitexecution WHERE device_id=%s AND mode_id_id=%s AND run_flag=2"
            select_res = self.cursor.execute(task_select, (deviceKey.strip(), 94,))
            data = self.cursor.fetchone()
            if data:
                repetitions = data[0]
            else:
                repetitions = self.TIMES

            res = {}
            step = time.strftime("%Y-%m-%d", time.localtime(time.time()))
            if status == 200:
                try:
                    sql_insert = "INSERT INTO waitexecution_his (`mode_id`,`devicekey_id`,`status`,`desc`) VALUES (%s,%s,%s,%s)"
                    self.cursor.execute(sql_insert, (94, deviceKey, status, '获取wifi',))
                    # 删除待执行任务表中的相应的数据
                    delete_sql = "DELETE FROM waitexecution WHERE device_id=%s AND run_flag=2 AND mode_id_id=%s"
                    self.logger.info('>>>已删除执行过的任务编号{}--类型{}')
                    self.cursor.execute(delete_sql, (deviceKey, 94,))
                    self.conn.commit()
                    self.logger.info('》》》》》》》》》》》》》》》》94的任务已经删除》》》》》》》》》》》》》》')
                    res.update({'status': 200})
                    self.logger.info('现在返回设备的信息是{}'.format(res))
                except Exception as e:
                    self.logger.error('任务执行有错误{}'.format(e))
                    self.logger.debug('任务执行有错误')
            else:
                if repetitions < self.TIMES:
                    if select_res:
                        # 任务执行失败，将待执行任务run_flag改为1，历史任务status改为0
                        sql1 = "UPDATE waitexecution SET run_flag=1,repetitions=repetitions+1 WHERE device_id=%s AND mode_id_id=%s"
                        self.cursor.execute(sql1, (deviceKey, 94,))
                        self.logger.info('>>>任务执行失败，任务状态已经更新，类型{}'.format(mod_id))
                        self.conn.commit()
                        res.update({'status': 200})
                else:
                    sql_insert = "INSERT INTO waitexecution_his (`devicekey_id`,`status`,`mode_id`,`desc`) VALUES (%s,%s,%s,%s)"
                    self.cursor.execute(sql_insert, (deviceKey, 2, 94,  '获取wifi',))
                    delete_sql = "DELETE FROM waitexecution WHERE device_id=%s AND run_flag=2 AND mode_id_id=%s"
                    self.cursor.execute(delete_sql, (deviceKey, 94,))
                    self.conn.commit()
                    self.logger.info('>>>已删除超出重复次数的任务{}')
                    # res = True
                    res.update({'status': 405})
        except Exception as e:
            self.conn.rollback()
            self.logger.debug('>>错误{}<<'.format(e))
            self.logger.error(traceback.format_exc() + '报错设备SN' + str(deviceKey))
        finally:
            return res


    def appendwifi(self,deviceKey):
        try:
            sql = "INSERT INTO waitexecution (`run_flag`,`repetitions`,`device_id`,`mode_id_id`,`desc`) VALUES (1,0,%s,93,'重启wifi模块')"
            self.cursor.execute(sql, (deviceKey,))
            self.conn.commit()
            self.logger.info('重启wifi的93号任务已经添加成功')
        except Exception as e:
            self.logger.debug('插入93号任务失败：{}'.format(e))
            self.logger.error('插入93号任务失败：{}'.format(e))



    # 修改c_d_o的wifi名
    def setwifi(self, info, status, mod_id, deviceKey):
        try:
            sql = "UPDATE c_d_0  SET rssi_name = %s WHERE device_id = %s"
            self.cursor.execute(sql, (info, deviceKey,))
            self.conn.commit()
            self.logger.info('现在当前设备信息表更新wifi名成功！')
            sql = "SELECT mode_id_id FROM waitexecution WHERE device_id = %s"
            ret = self.cursor.execute(sql, (deviceKey,))
            if ret:
                self.clear_tasks(mod_id, deviceKey, status)
                self.logger.info('现在把任务表里的94号任务干掉了')
                return {'status': 200}
            else:
                self.logger.info('设备直接发put-wifi成功！！！！！')
                return {'status': 200}
        except Exception as e:
            self.logger.debug('更新c_d_0表中的wifi名出错：{}'.format(e))
            self.logger.error('更新c_d_0表中的wifi名出错：{}'.format(e))




    def synchronousConfiguration(self, deviceKey):
        # 添加同步任务
        sql5 = "INSERT INTO waitexecution (`run_flag`,`repetitions`,`device_id`,`mode_id_id`,`desc`) VALUES (1,0,%s,101,'修改配置');"
        res = 0
        try:
            res = self.cursor.execute(sql5, (deviceKey,))
            self.logger.info('<><><>待执行任务插入成功<><><>')
            self.conn.commit()
            res = 1
        except Exception as e:
            self.logger.debug('同步配置失败{}'.format(e))
            self.logger.error(traceback.format_exc() + '报错设备SN' + str(deviceKey))
        finally:
            return res


    def synchronousConfigurations(self, deviceKey):
        # 同步平台默认设置
        # v7版本数据库
        sql5 = "INSERT INTO waitexecution (`device_id`,`mode_id_id`,`run_flag`,`repetitions`) VALUES (%s,103,1,0);"
        res = 0
        try:
            res = self.cursor.execute(sql5, (deviceKey,))
            self.logger.info('<><><>待执行任务插入成功<><><>')
            self.conn.commit()
        except Exception as e:
            self.logger.debug('同步配置失败{}'.format(e))
            self.logger.error(traceback.format_exc() + '报错设备SN' + str(deviceKey))
        finally:
            return res


    # wifi 扫描
    def Scan_wifi(self,deviceKey):
        sql5 = "INSERT INTO waitexecution (`device_id`,`mode_id_id`,`run_flag`,`repetitions`) VALUES (%s,98,1,0);"
        res = 0
        try:
            res = self.cursor.execute(sql5, (deviceKey,))
            self.logger.info('<><><>扫描wifi的待执行任务插入成功<><><>')
            self.conn.commit()
        except Exception as e:
            self.logger.debug('同步配置失败{}'.format(e))
            self.logger.error(traceback.format_exc() + '报错设备SN' + str(deviceKey))
        finally:
            return res




    def select_wait_otatask(self, devicekey):
        '''
        查询待执行任务中的OTA升级任务的状态,
        :param devicekey:
        :return:查询记录的条数
        '''
        try:
            sql = "SELECT run_flag FROM waitexecution WHERE device_id=%s AND mode_id_id=201 AND run_flag=2"
            res = self.cursor.execute(sql, (devicekey,))
            self.logger.info('>>>>select_wait_otatask_res======{}'.format(res))
            return res
        except Exception as e:
            self.logger.debug(e)
            self.logger.error(traceback.format_exc() + '报错设备SN' + str(devicekey))


    def initDeviceData(self, deviceKey, product_id, batt, rssi):
        """
        设备首次登陆系统，需要初始化各表数据(product_id判断是初始化哪张表)
        FROM_UNIXTIME(`insertTime`) 即可将int型的时间戳转换成时间格式，支持格式化输出
        """
        try:
            # 最新数据缓存表
            stemp = int(time.time())
            self.logger.info('初始化设备表的时间：{}'.format(stemp))
            if product_id == 1:
                # 设备版本同步用户表中
                sql = "INSERT INTO conf_u_101(up_limit_tem,down_limit_tem,tem_offset,tem_rt,up_limit_hum,down_limit_hum," \
                      "hum_offset,hum_rt,send_times,record_times,syn_times,lcd_id,device_id)SELECT up_limit_tem," \
                      "down_limit_tem,tem_offset,tem_rt,up_limit_hum,down_limit_hum,hum_offset,hum_rt,send_times," \
                      "record_times,syn_times,lcd_id,%s AS device_id FROM conf_d_101"
                self.cursor.execute(sql, (deviceKey,))
                self.conn.commit()
                self.logger.info('<<<v7版本数据库温湿度传感器执行设备初始化成功')
                ######  添加数据
                sql_h = "INSERT INTO h_d_0(rssi,battery,device_id) VALUES(%s,%s,%s)"
                self.cursor.execute(sql_h, (rssi, batt, deviceKey,))
                self.conn.commit()
                self.logger.info('<<<温湿度设备WiFi等信息添加表数据成功！！！')
                # 温湿度公共表写入
                sql3 = "INSERT INTO c_d_101 (temperature,humidity,device_id)VALUES(NULL,NULL,%s)"
                self.cursor.execute(sql3, (deviceKey,))
                self.conn.commit()
                self.logger.info('<<<温湿度公共表写入设备成功！！！！！！！！！！！！')
                sql4 = "UPDATE c_d_0 SET rssi=%s,battery=%s WHERE device_id = %s"
                self.cursor.execute(sql4, (rssi, batt, deviceKey,))
                self.conn.commit()
                self.logger.info('<<<执行设备电量等信息写入c-d-0表的操作成功')
                sql = "INSERT INTO waitexecution (run_flag,repetitions,device_id,mode_id_id,`desc`) VALUES (1,0,%s,94,'配置wifi')"
                self.cursor.execute(sql, (deviceKey,))
                self.conn.commit()
                self.logger.info('现在他娘的插入了一条任务得到wifi名已经插入成功了')
            elif product_id == 2:
                sql_smo = "INSERT INTO conf_u_103(up_limit_smoke,smoke_offset,smoke_rt,up_limit_tem,up_limit_hum," \
                          "down_limit_tem,down_limit_hum,tem_offset,hum_offset,tem_rt,hum_rt,send_times,record_times," \
                          "syn_times,device_id) SELECT up_limit_smoke,smoke_offset,smoke_rt,up_limit_tem,up_limit_hum," \
                          "down_limit_tem,down_limit_hum,tem_offset,hum_offset,tem_rt,hum_rt,send_times,record_times," \
                          "syn_times,%s AS device_id FROM conf_d_103"
                self.cursor.execute(sql_smo, (deviceKey,))
                self.conn.commit()
                self.logger.info('<<<烟雾传感器执行设备初始化成功')
                ######  添加数据
                sql_h = "INSERT INTO h_d_0(rssi,battery,device_id) VALUES(%s,%s,%s)"
                self.cursor.execute(sql_h, (rssi, batt, deviceKey,))
                self.conn.commit()
                self.logger.info('<<<温湿度设备WiFi等信息添加表数据成功！！！')
                # 温湿度公共表写入
                sql3 = "INSERT INTO c_d_103 (smoke,temperature,humidity,device_id)VALUES(NULL,NULL,NULL,%s)"
                self.cursor.execute(sql3, (deviceKey,))
                self.conn.commit()
                self.logger.info('<<<温湿度公共表写入设备成功！！！！！！！！！！！！')
        except Exception as e:
            self.logger.error(traceback.format_exc() + '设备初始化各种表的时候报错，报错设备SN' + str(deviceKey))
            self.conn.rollback()
            self.logger.debug("设备首次激活初始化表出错：%s" % e)

    def alarm(self, deviceKey, batt, data):
        '''
        设备告警
        '''
        data = data[-1]
        t = data['a']
        h = data['b']
        batt = batt
        step = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        try:
            # 查询数据库中设备的温湿度上下限
            sql = "SELECT up_limit_tem,down_limit_tem,up_limit_hum,down_limit_hum FROM conf_u_101 WHERE device_id = %s"
            self.cursor.execute(sql, (deviceKey,))
            select_res = self.cursor.fetchone()
            up_limit_tem, down_limit_tem, up_limit_hum, down_limit_hum = select_res

            up_limit_tem = up_limit_tem
            down_limit_tem = down_limit_tem
            up_limit_hum = up_limit_hum
            down_limit_hum = down_limit_hum
            res = []

            if t > up_limit_tem:
                res.append(('温度过高', t, up_limit_tem, deviceKey, 1, step,))
                if batt < self.BATT:
                    res.append(('电量过低', batt, self.BATT, deviceKey, 1, step,))
                elif h > up_limit_hum:
                    res.append(('湿度过高', h, up_limit_hum, deviceKey, 1, step,))
                elif h < down_limit_hum:
                    res.append(('湿度过低', h, down_limit_hum, deviceKey, 1, step,))
                elif t < down_limit_tem:
                    res.append(('温度过低', t, down_limit_tem, deviceKey, 1, step,))

            elif t < down_limit_tem:
                res.append(('温度过低', t, down_limit_tem, deviceKey, 1, step,))
                if batt < self.BATT:
                    res.append(('电量过低', batt, self.BATT, deviceKey, 1, step,))
                elif h > up_limit_hum:
                    res.append(('湿度过高', h, up_limit_hum, deviceKey, 1, step,))
                elif h < down_limit_hum:
                    res.append(('湿度过低', h, down_limit_hum, deviceKey, 1, step,))
                elif t > up_limit_tem:
                    res.append(('温度过高', t, up_limit_tem, deviceKey, 1, step,))

            elif batt < self.BATT:
                res.append(('电量过低', batt, self.BATT, deviceKey, 1, step,))
                if h > up_limit_hum:
                    res.append(('湿度过高', h, up_limit_hum, deviceKey, 1, step,))
                elif h < down_limit_hum:
                    res.append(('湿度过低', h, down_limit_hum, deviceKey, 1, step,))
                elif t > up_limit_tem:
                    res.append(('温度过高', t, up_limit_tem, deviceKey, 1, step,))
                elif t < down_limit_tem:
                    res.append(('温度过低', t, down_limit_tem, deviceKey, 1, step,))
            elif h > up_limit_hum:
                res.append(('湿度过高', h, up_limit_hum, deviceKey, 1, step,))
                if h < down_limit_hum:
                    res.append(('湿度过低', h, down_limit_hum, deviceKey, 1, step,))
                elif t > up_limit_tem:
                    res.append(('温度过高', t, up_limit_tem, deviceKey, 1, step,))
                elif batt < self.BATT:
                    res.append(('电量过低', batt, self.BATT, deviceKey, 1, step,))
                elif t < down_limit_tem:
                    res.append(('温度过低', t, down_limit_tem, deviceKey, 1, step,))
            elif h < down_limit_hum:
                res.append(('湿度过低', h, down_limit_hum, deviceKey, 1, step,))
                if t > up_limit_tem:
                    res.append(('温度过高', t, up_limit_tem, deviceKey, 1, step,))
                elif t < down_limit_tem:
                    res.append(('温度过低', t, down_limit_tem, deviceKey, 1, step,))
                elif batt < self.BATT:
                    res.append(('电量过低', batt, self.BATT, deviceKey, 1, step,))
                elif h > up_limit_hum:
                    res.append(('湿度过高', h, up_limit_hum, deviceKey, 1, step,))

            if res:
                self.logger.info('向告警表中插入的res==={}'.format(res))
                sql = "INSERT INTO warninglog(`warning_type`,`current`,`threshold`,`device_id`,`processing_mark`,`create_time`) VALUES (%s,%s,%s,%s,%s,%s)"
                n = self.cursor.executemany(sql, res)
                self.conn.commit()
                if n:
                    self.logger.info('写入告警表完成!')
                else:
                    self.logger.info('对不起，没有写入告警表')

        except Exception as e:
            self.logger.error(traceback.format_exc() + '报错设备SN' + str(deviceKey))
            self.logger.debug('告警信息插入错误e={}'.format(e))

    def alarmRecovery(self, step, data, batt, deviceKey):
        '''
        告警恢复，修改告警表中该设备的告警恢复状态
        :return:
        '''
        stauts = False
        try:
            if len(data) < 2:
                data = data[-1]
            else:
                data = data[-2]
            self.logger.info('《》现在是告警恢复函数，data的值是：{}《》'.format(data))
            t = data['a']
            h = data['b']
            t = int(t)
            h = int(h)
            batt = batt
            # 将告警表中该设备告警数据中的阈值提取出来判断告警恢复的类型
            sql = "SELECT threshold,warning_type FROM warninglog WHERE device_id=%s AND processing_mark=1"
            self.cursor.execute(sql, (deviceKey,))
            res = self.cursor.fetchall()
            self.logger.info('告警恢复查询的告警信息是{}'.format(res))
            for i in res:
                threshold, warning_type = i
                threshold = int(threshold)
                self.logger.info('现在走到了for循环这里！！！！！！！！！！！！！！')
                if t < threshold:
                    # 表示温度告警恢复
                    sql = "UPDATE warninglog set processing_mark=0,process_time=%s WHERE device_id=%s AND warning_type=%s"
                    self.cursor.execute(sql, (step, deviceKey, warning_type,))
                    self.logger.info('温度过高告警恢复')
                    self.conn.commit()
                    stauts = True

                if t > threshold:
                    sql = "UPDATE warninglog set processing_mark=0,process_time=%s WHERE device_id=%s AND warning_type=%s"
                    self.cursor.execute(sql, (step, deviceKey, warning_type,))
                    self.logger.info('温度过低告警恢复')
                    self.conn.commit()
                    stauts = True

                self.logger.info('电量告警恢复进来了')
                if batt > threshold:
                    # 表示电量过低告警已恢复
                    sql = "UPDATE warninglog set processing_mark=0,process_time=%s WHERE device_id=%s AND warning_type=%s"
                    self.cursor.execute(sql, (step, deviceKey, warning_type,))
                    self.conn.commit()
                    self.logger.info('电量过低告警已恢复')
                    stauts = True

                if h > threshold:
                    # 表示湿度告警恢复
                    sql = "UPDATE warninglog set processing_mark=0,process_time=%s WHERE device_id=%s AND warning_type=%s"
                    self.cursor.execute(sql, (step, deviceKey, warning_type,))
                    self.logger.info('湿度过低告警恢复')
                    self.conn.commit()
                    stauts = True

                if h < threshold:
                    sql = "UPDATE warninglog set processing_mark=0,process_time=%s WHERE device_id=%s AND warning_type=%s"
                    self.cursor.execute(sql, (step, deviceKey, warning_type,))
                    self.logger.info('湿度过高告警恢复')
                    self.conn.commit()
                    stauts = True
        except Exception as e:
            self.logger.error(traceback.format_exc() + '报错设备SN' + str(deviceKey))
            self.logger.debug('告警恢复失败，e=={}'.format(e))

        finally:
            return stauts

    def activate_insert(self, body, SN):
        """
        接收传感器获取的数据,插入到数据库
        :param data:
        :param deviceKey:
        :return:
        """

        insert_flag = {}
        flag_insert = None
        self.logger.info(">>>开始进行数据插入1")
        deviceKey = SN
        data = body['datapoints']
        # 获取电量
        batt = body['batt']
        try:
            step = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
            for d in data:
                # 无指令
                if d['comm'] == 0:
                    self.logger.info('对不起，您的操作没有任何的指令')
                    insert_flag.update({'status': 406})

                # 温湿度的数据的同步
                if d['mod_id'] == 101 and d['comm'] == 1:
                    res = self.synchronousConfiguration(deviceKey)
                    if res:
                        insert_flag.update({'status': 200})
                        self.logger.info('<<<<同步数据命令提交成功,等待设备同步数据')
                    else:
                        self.logger.info('<<<<<<同步数据失败')

                # 温湿度的数据插入
                if d['mod_id'] == 101 and d['comm'] == 2:
                    self.logger.info('开始进入插入数据逻辑')
                    try:
                        sql_now = "INSERT INTO h_d_101 (temperature,humidity,device_id)VALUES(%s,%s,%s)"
                        self.logger.info('现在开始执行插入的sql语句{}'.format(sql_now))
                        self.cursor.execute(sql_now, (d['a'], d['b'], deviceKey,))
                        sql_new = "UPDATE c_d_101 SET temperature=%s,humidity=%s WHERE device_id = %s"
                        self.cursor.execute(sql_new, (d['a'], d['b'], deviceKey,))  # 更新最新数据记录
                        self.conn.commit()
                        insert_flag.update({'status': 200})
                        self.logger.info('<<<<数据提交成功,数据库完成写入')
                    except Exception as e:
                        insert_flag.update({'status': 406})
                        self.logger.error('《《《《数据插入失败，失败信息为:{}'.format(e))
                        self.logger.info('<<<<<数据插入失败')

                # config配网
                if d['mod_id'] == 101 and d['comm'] == 3:
                    self.logger.info('config配网操作')
                    insert_flag.update({'status': 406})

                # 传感器错误
                if d['comm'] == 4:
                    self.logger.info('传感器错误')
                    insert_flag.update({'status': 406})

                # rap操作
                if d['mod_id'] == 101 and d['comm'] == 5:
                    self.logger.info('rap操作')
                    insert_flag.update({'status': 406})

                # 发现告警
                if d['mod_id'] == 101 and d['comm'] == 6:
                    self.logger.info('发现告警')
                    self.alarm(deviceKey, batt, data)
                    insert_flag.update({'status': 200})

                # 告警恢复
                if d['mod_id'] == 101 and d['comm'] == 7:
                    self.logger.info('告警恢复，修改状态')
                    step = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                    ss = self.alarmRecovery(step, data, batt, deviceKey)
                    if ss:
                        insert_flag.update({'status': 200})
                    else:
                        insert_flag.update({'status': 404})

                # 既有告警又有告警恢复
                if d['mod_id'] == 101 and d['comm'] == 8:
                    self.logger.info('既有告警又有告警恢复')
                    self.alarm(data, deviceKey, batt)
                    self.alarmRecovery(step, data, batt, deviceKey)

                # 烟雾传感数据得插入
                if d['mod_id'] == 103 and d['comm'] == 2:
                    self.logger.info('烟雾传感器开始进入插入数据逻辑')
                    try:
                        sql_now = "INSERT INTO h_d_103 (temperature,humidity,smoke,device_id)VALUES(%s,%s,%s,%s)"
                        self.logger.info('现在开始执行插入的sql语句{}'.format(sql_now))
                        self.cursor.execute(sql_now, (d['a'], d['b'], d['c'], deviceKey,))
                        sql_new = "UPDATE c_d_103 SET temperature=%s,humidity=%s,smoke=%s WHERE device_id = %s"
                        self.cursor.execute(sql_new, (d['a'], d['b'], d['c'], deviceKey,))  # 更新最新数据记录
                        self.conn.commit()
                        insert_flag.update({'status': 200})
                        self.logger.info('<<<<数据提交成功,数据库完成写入')
                    except Exception as e:
                        insert_flag.update({'status': 406})
                        self.logger.info('<<<<<数据插入失败')

                # 烟雾的数据同步
                if d['mod_id'] == 103 and d['comm'] == 1:
                    res = self.synchronousConfigurations(deviceKey)
                    if res:
                        insert_flag.update({'status': 200})
                        self.logger.info('<<<<同步数据命令提交成功,等待设备同步数据')
                    else:
                        self.logger.info('<<<<<<同步数据失败')


        except Exception as e:
            self.logger.debug("数据插入出错：%s" % e)
            self.logger.error(traceback.format_exc() + '报错设备SN' + str(deviceKey))

        finally:
            return insert_flag

    def updateRssi(self, deviceKey, rssi):
        """+
        更新rssi值
        ,insertTime=unix_timestamp()
        """
        timestep = time.strftime('%Y-%m-%d %X')
        try:
            sql = "UPDATE currentthermograph SET rssi=%s,	`temperature` = NULL, `humidity` = NULL, `battery` = NULL,update_time= %s  WHERE device_id=%s;"
            # self.logger.info("执行:sql=：%s" % self.cursor.mogrify(sql, (rssi, timestep, deviceKey)))
            r = self.cursor.execute(sql, (rssi, timestep, deviceKey))
            self.conn.commit()
            self.logger.info('<<<更新设备rssi')
        except Exception as e:
            self.logger.error(traceback.format_exc() + '报错设备SN' + str(deviceKey))
            # self.logger.error('报错设备SN为 %s' % deviceKey)
            self.logger.debug("更新rssi值出错 % s" % e)
            return 0
        return r

    def Flicker_fre(self, deviceKey):
        try:
            sql = "SELECT flashing_frequency FROM user_device_config_led WHERE device_id = %s"
            sql_info = self.cursor.execute(sql, (deviceKey))
            if sql_info:
                flicker_fre_info = self.cursor.fetchone()
                self.logger.info('<<<<<设备的闪烁频率信息拿到了')
                return flicker_fre_info
            else:
                return 0

        except Exception as e:
            self.logger.debug(e)
            self.logger.error(traceback.format_exc() + '设备的闪烁频率信息报错' + str(deviceKey))

    def trigger_frequency(self, deviceKey):
        try:
            sql = "SELECT trigger_frequency,duration FROM user_device_config_infrared WHERE device_id = %s"
            sql_info = self.cursor.execute(sql, (deviceKey))
            if sql_info:
                trigger_frequency_info = self.cursor.fetchone()
                self.logger.info('<<<<<设备的触发频率信息拿到了')
                return trigger_frequency_info
            else:
                return 0
        except Exception as e:
            self.logger.debug(e)
            self.logger.error(traceback.format_exc() + '设备的触发频率信息报错' + str(deviceKey))

    def get_new_lcd_id_lcd(self, deviceKey):
        '''
        拿取现在lcd的数据返回
        '''
        try:
            sql = "SELECT lcd_id FROM conf_u_104  WHERE device_id=%s"
            tmp = self.cursor.execute(sql, deviceKey)
            if tmp:
                lcdId = self.cursor.fetchone()[0]
                self.logger.info("lcdId =：%s" % lcdId)

                sql = "UPDATE user_device_config_lcd SET lcd_id=%s WHERE device_id=%s"
                # self.logger.info("执行sql=：%s" % self.cursor.mogrify(sql, (version, deviceKey)))
                self.cursor.execute(sql, (lcdId, deviceKey))
                self.conn.commit()
                self.logger.info('<<<更新设备user_device_config_lcd表中的lcd_id的信息')
                return lcdId
            else:
                return 0
        except Exception as e:
            self.logger.debug(e)
            self.logger.error(traceback.format_exc() + '报错信息' + str(deviceKey))

    def get_lcd_id(self, deviceKey):
        """
        查询客户端lcd显示的id
        """
        try:
            sql = "SELECT lcd_id FROM conf_u_104  WHERE device_id=%s"
            tmp = self.cursor.execute(sql, deviceKey)
            if tmp:
                lcdId = self.cursor.fetchone()[0]
                self.logger.info("lcdId =：%s" % lcdId)
                return lcdId
            else:
                return 0
        except Exception as e:
            self.logger.debug(e)
            self.logger.error(traceback.format_exc() + '报错设备SN' + str(deviceKey))
            # self.logger.error('报错设备SN为 %s' % deviceKey)


    def get_device_count(self, deviceKey):
        """
        查询客户端的交互频率
        """
        try:
            sql = "SELECT syn_times FROM conf_d_101"
            tmp = self.cursor.execute(sql,)
            if tmp:
                returnRate = self.cursor.fetchone()
                self.logger.info("returnRate =：(%s)" % returnRate)
                return returnRate[0]
            else:
                return 0
        except Exception as e:
            self.logger.debug(e)
            self.logger.error(traceback.format_exc() + '报错设备SN' + str(deviceKey))

    def get_device_tranc_info(self, deviceKey):
        """
        查询客户端的数据传输设置
        """
        try:
            sql = "SELECT `record_times`, `send_times` FROM conf_u_0  WHERE device_id=%s"
            # self.logger.info("sql=：%s" % self.cursor.mogrify(sql, (deviceKey,)))
            tmp = self.cursor.execute(sql, (deviceKey,))
            if tmp:
                tranc_info = self.cursor.fetchone()
                self.logger.info("returnRate =：%s" % (tranc_info,))
                return tranc_info
            else:
                return 0
        except Exception as e:
            self.logger.debug(e)
            self.logger.error(traceback.format_exc() + '报错设备SN' + str(deviceKey))
            # self.logger.error('报错设备SN为 %s' % deviceKey)

    def ota_version(self, deviceKey):
        """
        查询客户端需要更新的版本
        """
        try:
            # 查询该类最新版本
            # sql = "SELECT version_nu FROM versionnow,d0evicehardwareinfo WHERE versionnow.product_id=devicehardwareinfo.product_id_id"
            sql = "SELECT version_nu from versionnow RIGHT JOIN devicehardwareinfo on versionnow.product_id = devicehardwareinfo.product_id_id WHERE devicehardwareinfo.device = %s;"
            self.cursor.execute(sql, (deviceKey,))
            newVersion = self.cursor.fetchone()[0]
        except Exception as e:
            self.logger.error(traceback.format_exc() + '报错设备SN' + str(deviceKey))
            # self.logger.error('报错设备SN为 %s' % deviceKey)
            self.logger.debug("查询客户端需要更新的版本出错 % s" % e)
            return 0

        self.logger.info("newVersion=%s" % newVersion)
        return newVersion

    def updateVersion(self, deviceKey, version):
        """
        更新版本数据
        """

        try:
            sql = "UPDATE deviceinfo SET version=%s WHERE device_id=%s"
            r = self.cursor.execute(sql, (version, deviceKey,))
            self.conn.commit()
            self.logger.info('<<<更新设备version信息')
        except Exception as e:
            self.logger.error(traceback.format_exc() + '报错设备SN' + str(deviceKey))
            # self.logger.error('报错设备SN为 %s' % deviceKey)
            self.logger.debug("更新版本数据出错 % s" % e)
            return 0
        return r

    def select_version(self, devicekey):
        try:
            sql = "SELECT version FROM devicehardwareinfo WHERE device=%s"
            self.cursor.execute(sql, (devicekey,))
            r = self.cursor.fetchone()[0]
        except Exception as e:
            self.logger.error(traceback.format_exc() + '报错设备SN' + str(devicekey))
            self.logger.debug('查找设备当前版本出错 %s' % e)
            return None
        return r

    def ota_version_update(self, devicekey, rom_version):
        '''
        OTA升级成功后更新数据库中设备的版本号version
        :param devicekey: 设备
        :param rom_version: 设备版本号
        :return: r
        '''
        try:
            sql = "UPDATE deviceinfo SET version=% WHERE device_id=%s"
            # self.logger.info('执行sql=:%s' % self.cursor.mogrify(sql, (rom_version, devicekey)))
            r = self.cursor.execute(sql, (rom_version, devicekey))
            self.conn.commit()
            self.logger.info('数据库设备版本号与设备更新同步')
        except Exception as e:
            self.logger.error(traceback.format_exc() + '报错设备SN' + str(devicekey))
            # self.logger.error('报错设备SN为 %s' % devicekey)
            self.logger.debug('OTA升级成功后更新数据库中设备的版本号version出错 %s' % e)
            return 0
        return r



    # 清除任务
    def clean_task_l(self, deviceKey, modeID):
        if int(modeID) == int(101):
            mname = '修改配置'
        elif int(modeID) == int(99):
            mname = 'ota升级'
        elif int(modeID) == int(95):
            mname = '配置wifi'
        else:
            mname = '啥命令啊'
        big_ru_fl = None
        try:
            sql_st = "SELECT run_flag FROM waitexecution WHERE device_id = %s AND mode_id_id = %s LIMIT 1"
            r_fl = self.cursor.execute(sql_st, (deviceKey, modeID,))
            if r_fl:
                ru_fl = self.cursor.fetchone()[0]
                big_ru_fl = ru_fl
            sql = "DELETE FROM waitexecution WHERE device_id = %s AND mode_id_id = %s AND run_flag = 2"
            self.cursor.execute(sql, (deviceKey, modeID,))
            sql_in = "INSERT INTO `waitexecution_his`(mode_id,`status`,devicekey_id,`desc`) VALUES (%s,%s,%s,%s)"
            self.cursor.execute(sql_in, (modeID, big_ru_fl, deviceKey, mname, ))
            self.conn.commit()
            self.logger.info('<><><带执行任务表中删除任务，历史数据表中添加了任务><><>')

        except Exception as e:
            self.logger.debug('>>>>>>>清除任务出错：{}'.format(e))
            self.logger.error('>>>>>>>清除任务出错：{}'.format(e))





    def insert_devicedata(self, devicekey):
        '''
        设备上线将设备相关数据查找到并插入到在线表
        :param devicekey:
        :return:
        '''
        try:
            # v7版本数据库
            sql = "SELECT send_times,record_times FROM conf_u_101 WHERE device_id=%s"
            self.cursor.execute(sql, (devicekey,))
            send_times, record_times = self.cursor.fetchone()
            self.logger.info('查找上线设备相关数据完成')
            sys_time = record_times * send_times
            step = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))

            # 将设备信息插入到在线表
            sql = "INSERT INTO connect_manage(device_id,sys_time,update_time)VALUES (%s,%s,%s)"
            self.cursor.execute(sql, (devicekey, sys_time, step,))
            # 修改deviceinfo表中设备的状态为在线
            sql = "UPDATE deviceinfo SET is_alive=0 WHERE device_id=%s"
            self.cursor.execute(sql, (devicekey,))
            self.conn.commit()
            self.logger.info('将设备插入到在线表以及修改deviceinfo表的设备的状态为在线成功')
        except Exception as e:
            self.logger.error(traceback.format_exc() + '报错设备SN' + str(devicekey))
            self.logger.debug('设备上线查找设备相关数据插入到在线表出错，e==={}'.format(e))

    def update_time(self, devicekey):
        '''
        验证设备已在线，更新设备最新在线时间
        :param devicekey:
        :return:
        '''
        try:
            # step = time.strftime('%Y-%m-%d %X')
            step = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
            sql = "UPDATE connect_manage SET update_time=%s WHERE device_id=%s"
            self.cursor.execute(sql, (step, devicekey))
            self.conn.commit()
            self.logger.info('设备在线的最新时间已经修改')
        except Exception as e:
            self.logger.error(traceback.format_exc() + '报错设备SN' + str(devicekey))
            self.logger.debug('更新设备最新交互时间出错，e={}'.format(e))

    def select_device(self, devidekey):
        '''
        检查在线表中是否有该设备，没有的话表示设备上线将设备相关信息插入在线表，有的话不需要什么操作
        :param devidekey:
        :return:
        '''
        try:
            sql = "SELECT * FROM connect_manage WHERE device_id=%s"
            res = self.cursor.execute(sql, (devidekey,))
            return res
        except Exception as e:
            self.logger.error(traceback.format_exc() + '报错设备SN' + str(devidekey))
            self.logger.debug('检查设备是否已经在线出错，error e==={}'.format(e))

    def select_data(self, start, stmp):
        '''
        查询在线表中的数据
        :return:
        '''
        try:
            sql = "SELECT sys_time,update_time ,device_id FROM connect_manage limit %s,%s"
            self.cursor.execute(sql, (start, stmp))
            res = self.cursor.fetchall()
            self.conn.commit()
        except Exception as e:
            self.logger.error(traceback.format_exc() + '检测设备是否在线查询在线表时出错')
            self.logger.debug('查询在线设备出错e={}'.format(e))
            return None
        return res

    def delDevice(self, devicekey):
        '''
        设备已经离线，将在线表中的设备数据删除
        :param devicekey:
        :return:
        '''
        try:
            step = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
            sql = "DELETE FROM connect_manage WHERE device_id=%s"
            self.cursor.execute(sql, (devicekey,))
            # 修改deviceinfo表中设备的状态为离线
            sql = "UPDATE deviceinfo SET is_alive=1 WHERE device_id=%s"
            self.cursor.execute(sql, (devicekey,))
            sql_del = "INSERT INTO warninglog (threshold,processing_mark,`current` ,warning_type,create_time,device_id) VALUES (NULL,1,NULL,'设备离线',%s,%s)"
            self.cursor.execute(sql_del, (step, devicekey,))
            self.conn.commit()
            self.logger.info('将在线表中的该离线设备删除并且已经把离线设备添加到告警表中')
        except Exception as e:
            self.logger.error(traceback.format_exc() + '报错设备SN' + str(devicekey))
            self.logger.debug('删除在线表中离线设备数据出错，e={}'.format(e))


    def clean_init(self, sn):
        clean_data = 'DELETE FROM c_d_0 WHERE device_id=%s'
        clean_data_conf = 'DELETE FROM conf_u_101 WHERE device_id=%s'
        self.cursor.execute(clean_data, (sn,))
        self.cursor.execute(clean_data_conf, (sn,))
        self.conn.commit()
        self.logger.info('情书初始化数据完成可以激活')
        print('清楚初始化数据')

if __name__ == "__main__":
    DB()
