#-*-coding:utf-8-*-
import zookeeper
import redis
import ConfigParser
import time
import datetime

#获取配置列表
def get_config_list(sel="master"):
	table  = []
	config = ConfigParser.SafeConfigParser()
	config.read("config.ini")
	options = config.options(sel)
	for node in options:
		table.append(config.get(sel,node))
	return table
#获取单个配置
def get_config_node(sel,node):
	if sel == 'master':
		ext = 'm'
	else:
		ext = 's'
	config = ConfigParser.SafeConfigParser()
	config.read("config.ini")
	value = config.get(sel,ext+str(node))
	return value

#修改配置列表
def update_config_node(sel,node,value):
	if sel == 'master':
		ext = 'm'
	else:
		ext = 's'
	write_log('node '+node+' update local config and value:'+value+' by section:'+sel)
	config = ConfigParser.SafeConfigParser()
	config.read("config.ini")
	config.set(sel, ext+str(node), value)
	fp = open(r'config.ini','w')
	config.write(fp)
	fp.close()


def node_replace(pos,val,strings):
	strings[pos] = val
	for i in range(0,len(strings)):
		strings[i] = str(strings[i])

	new_strings = ":".join(strings)
	return new_strings

#获取redis对象
def obj_redis(host,port,db = 0):
	try:
		r = redis.StrictRedis(host=host,port=port,db=db,password='quB1BY3njv0eq1N4e3d9Q570V4mYHldDeb7BFw92')
		return r
	except Exception,e:
		write_log('node '+host+':'+port+' connect redis service failed')
		return False

#检测redis的状态 
def is_alive(host,port):
	#3次检测机制
	status = False
	for i in range(0,3):
		r = obj_redis(host,port)
		try:
			status = r.ping()
			break
		except Exception, e:
			status = False
	return status

#检测存库
def make_slave(node):
	master_node = get_config_node('master',node)
	slave_node = get_config_node('slave',node)
	master_node_list= master_node.split(":")
	slave_node_list = slave_node.split(":")
	if int(slave_node_list[1]) == 1:#挂掉的旧master刚被拉起
		local_new_slave = node_replace(1,2,slave_node_list)
		update_config_node('slave',slave_node_list[0],local_new_slave)
		redis_slave  = obj_redis(slave_node_list[2],slave_node_list[3])
		redis_slave.slaveof(master_node_list[2],master_node_list[3])	#同步数据
		redis_slave.config_set('masterauth','123456')#同步权限
		#修改状态为数据同步
		write_log('node '+slave_node+' aberrant slave service Normal start sync data')

	elif int(slave_node_list[1]) == 2:#拉起之后数据同步中
		redis_master = obj_redis(master_node_list[2],master_node_list[3])
		redis_slave  = obj_redis(slave_node_list[2],slave_node_list[3])
		master_dbsize = redis_master.dbsize()
		slave_dbsize = redis_slave.dbsize()
		if(int(master_dbsize) - int(slave_dbsize) <=10):#数据大致相等
			zk_new_master = node_replace(1,0,slave_node_list)
			zk_new_slave  = master_node
			zk_old_master = master_node
			zk_old_slave  = node_replace(1,0,slave_node_list)

			#print '/redis_master/'+zk_old_master,'/redis_master/'+zk_new_master
			#print '/redis_slave/'+zk_old_slave,'/redis_slave/'+zk_new_slave
			#修改本地配置列表
			update_config_node('master',master_node_list[0],zk_new_master)
			update_config_node('slave',slave_node_list[0],zk_new_slave)
			#修改zookpeer
			set_node('/redis_master/'+zk_old_master,'/redis_master/'+zk_new_master)#修改主
			set_node('/redis_slave/'+zk_old_slave,'/redis_slave/'+zk_new_slave)#修改存
			redis_slave.slaveof()#设成可写
			redis_master.slaveof(slave_node_list[2],slave_node_list[3])#设置成存库
			write_log('node '+master_node+' and '+ slave_node+' make_slave dbsize consistency Switching master and slave')
		else:
			write_log('node '+master_node+' and '+ slave_node+' make_slave  dbsize inconsistencies continue to synchronize data')
	return 0

#重设主存
def reset_master(L):
	master = get_config_node('master',L[0])
	slave  = get_config_node('slave',L[0])
	masterlist = master.split(":")
	slavelist = slave.split(":")
	slave_status  = is_alive(slavelist[2],slavelist[3])
	zk_new_master = slave
	zk_new_slave = master
	if slave_status == True:
		local_new_master = slave
		local_new_slave = node_replace(1,1,masterlist)
		print local_new_master,local_new_slave
		print '/redis_master/'+master,'/redis_master/'+zk_new_master
		print '/redis_slave/'+slave,'/redis_slave/'+zk_new_slave
		#修改本地列表
		update_config_node('master',masterlist[0],local_new_master)
		update_config_node('slave',slavelist[0],local_new_slave)
		#修改zookpeer
		set_node('/redis_master/'+master,'/redis_master/'+zk_new_master)#修改主
		set_node('/redis_slave/'+slave,'/redis_slave/'+zk_new_slave)#修改存
		r = obj_redis(slavelist[2],slavelist[3])
		r.slaveof()#把存库设置为可写
	return 0



#设置zookeeper节点
def set_node(delurl,addurl):
	handler = zookeeper.init("127.0.0.1:2181")
	delnode = zookeeper.exists(handler,delurl)
	addnode = zookeeper.exists(handler,addurl)
	#找到旧的URL就删除
	if delnode:
		zookeeper.delete(handler,delurl)#删除旧的URL
	else:
		write_log(' zookeeper not found old url '+delurl)
	#如果新的URL不存在就创建
	if addnode == None:
		try:
			zookeeper.create(handler,addurl,'',[{"perms":0x1f,"scheme":"world","id":"anyone"}],0)#zookeeper重设URL
			write_log(' zookeeper url from '+delurl+' change to '+addurl+' success')
		except Exception, e:
			write_log(' zookeeper url from '+delurl+' change to '+addurl+' failed')
	else:
		write_log('zookeeper new url exists '+addurl)
	zookeeper.close(handler)

#记录日志
def write_log(data):
	date = time.strftime('%Y-%m-%d',time.localtime(time.time()))
	log = open('/tmp/redis_server_'+str(date)+'.log','a+')
	log.write('['+str(datetime.datetime.now())+']'+ data+"\r\n")
	log.close()

while True:
	time.sleep(1)#休眠1s
	master = get_config_list()#获取本地master列表
	for i in master:
		mlist = i.split(":")
		m_status = is_alive(mlist[2],mlist[3])#master状态
		if m_status == False:#master状态出问题
			write_log('node '+mlist[2]+':'+str(mlist[3])+' master ping failed')
			reset_master(mlist)

	slave  = get_config_list('slave')#获取本地slave列表
	for j in slave:
		slist = j.split(":")
		s_status = is_alive(slist[2],slist[3])#slave状态

		if s_status == True and (str(slist[1]) == '1' or str(slist[1])=='2'):#恢复机制
			make_slave(slist[0])
		elif s_status == False and str(slist[1]) == '1':
			write_log('Turned into a slave by the master server, or in an unusable state HOST:'+j)
		




		


