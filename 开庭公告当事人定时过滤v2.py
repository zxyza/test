# -*- coding: utf-8 -*-
# @Time    : 2022/3/3 15:54 
# @Author  : ZXYZA
# @Email   : 153909445@qq.com
# @File    : 开庭公告当事人定时过滤v2.py
# @Software: PyCharm


import regex as re
import os
import cx_Oracle
import traceback
import pymysql
import ahocorasick
import pickle

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'  # 防止出现编码错误
import datetime
import time


def connect_oracle_24():
    # 配置说明:用户名 密码 ip:端口号/数据库名
    connection = cx_Oracle.connect("zxy", "zxy", "192.168.0.25:1521/ORCL25")
    cur = connection.cursor()
    return connection, cur


def connect_oracle_89():
    # 配置说明:用户名 密码 ip:端口号/数据库名
    connection = cx_Oracle.connect("ZXY", "Zxy123456", "192.168.0.89:1521/orcl")
    cur = connection.cursor()
    return connection, cur


###这个是基于135oracle机器进行挖掘的
# def connect_oracle():
#     # 配置说明:用户名 密码 ip:端口号/数据库名
#
#     connection = cx_Oracle.connect("sys", "2016oracle2016", "192.168.0.135:1521/ORCL",cx_Oracle.SYSDBA)
#     cur = connection.cursor()
#     return connection,cur

def connect_mysql_getdata_89(dbname):
    conn = pymysql.connect(host="192.168.0.89", port=3306, charset='utf8', user='root', passwd='Smkj1303,', db=dbname)
    cur = conn.cursor(pymysql.cursors.SSCursor)
    cur.execute('set session net_write_timeout = 100000')
    return conn, cur


def connect_mysql_getdata_12(dbname):
    conn = pymysql.connect(host="192.168.0.12", port=3306, charset='utf8', user='root', passwd='Abc123456.', db=dbname)
    cur = conn.cursor(pymysql.cursors.SSCursor)
    cur.execute('set session net_write_timeout = 100000')
    return conn, cur


def connect_oracle_25():
    # 配置说明:用户名 密码 ip:端口号/数据库名
    connection = cx_Oracle.connect("zxy", "zxy", "192.168.0.25:1521/ORCL25")
    cur = connection.cursor()
    return connection, cur


def connect_oracle():
    # 配置说明:用户名 密码 ip:端口号/数据库名
    # connection = cx_Oracle.connect("zxy", "zxy", "218.61.32.88:1521/ORCL25")
    connection = cx_Oracle.connect("zxy", "zxy", "192.168.0.25:1521/ORCL25")
    # connection = cx_Oracle.connect("zxy", "zxy", "192.168.0.12:1521/ORCL")
    # connection = cx_Oracle.connect("sys", "Smkj123456", "192.168.0.12:1521/ORCL12",cx_Oracle.SYSDBA)
    cur = connection.cursor()
    return connection, cur


def connect_oracle135():
    # 配置说明:用户名 密码 ip:端口号/数据库名
    # connection = cx_Oracle.connect("zxy", "zxy", "218.61.32.88:1521/ORCL25")
    connection = cx_Oracle.connect("DATA_GSK_USER", "Smkj123456", "192.168.0.135:1521/ORCL")
    # connection = cx_Oracle.connect("zxy", "zxy", "192.168.0.12:1521/ORCL")
    # connection = cx_Oracle.connect("sys", "Smkj123456", "192.168.0.12:1521/ORCL12",cx_Oracle.SYSDBA)
    cur = connection.cursor()
    return connection, cur

def connect_mysql10(dbname):
    conn = pymysql.connect(host = "192.168.0.10", port = 3306, charset = 'utf8', user = 'zxy', passwd = 'Abc123456.', db = dbname)
    cur = conn.cursor(pymysql.cursors.SSCursor)
    cur.execute('set session net_write_timeout = 100000')
    return conn, cur

# 全角转半角
def char_transform(text):
    half = ''
    for u in text:
        num = ord(u)
        if num == 0x3000:
            num = 32
            # print('noting')
        elif 0xFF01 <= num <= 0xFF5E:
            num -= 0xfee0
        u = chr(num)
        half += u
    return half


# 获取到给律所付过律师费的公司
def pay_lawyerfee_company_dic():
    conn10 = connect_mysql10('Company')
    conn = conn10[0]
    cur = conn10[1]
    cmd = 'select * from b_company_律所_下游公司_付过律师费'

    cur.execute(cmd)

    all_company = []
    for info in cur:
        company = char_transform(info[0])
        all_company.append(company)
    print('付过律师费的公司字典生成完毕！')
    cur.close()
    conn.close()

    return set(all_company)

pay_lawyerfee_company = pay_lawyerfee_company_dic()


# 获取到上市公司
def shagnshi_company_dic():
    connect_25 = connect_oracle_25()  # 对应的是案件管理的表
    conn25 = connect_25[0]
    cur25 = connect_25[1]

    cmd = 'select 公司名称 from zxy.上市公司'
    cur25.execute(cmd)

    company_dic = set([i[0] for i in cur25])
    cur25.close()
    conn25.close()
    return company_dic


recur_count = 0


def KillNone(*parms):
    if len(parms) == 1 and (type(parms[0]) == type([]) or type(parms[0]) == type(())):
        ps = parms[0]
        ps = list(ps)
    else:
        ps = parms
        ps = list(ps)
    for i in range(len(ps)):
        if ps[i] is None or ps[i] == 'None' or ps[i] == 'none':
            ps[i] = ""
    if len(ps) == 1: return ps[0]
    return ps


def KillNone1(*parms):
    if len(parms) == 1 and (type(parms[0]) == type([]) or type(parms[0]) == type(())):
        ps = parms[0]
        ps = list(ps)
    else:
        ps = parms
        ps = list(ps)
    for i in range(len(ps)):
        if ps[i] is None or ps[i] == 'None' or ps[i] == 'none':
            ps[i] = 0
    if len(ps) == 1: return ps[0]
    return ps


# 获取当前当事人的开庭公告总数和待开庭个数，待开庭数是从当前时间开始的
def get_ktgg_num():
    connect_25 = connect_oracle_25()  # 对应的是案件管理的表
    conn25 = connect_25[0]
    cur25 = connect_25[1]
    cmd = """
    select x.当事人, x.总开庭数, y.待开庭数
    from (select t.当事人, count(distinct t.案号) as 总开庭数
          from zxy.开庭公告_结构化@DBLINKE_192_89 t
         where length(t.当事人) > 4
         group by t.当事人) x
    left join
    (select t1.当事人, count(distinct t1.案号) as 待开庭数
    from zxy.开庭公告_结构化@DBLINKE_192_89 t1
    where length(t1.当事人) > 4
     and t1.开庭日期 >=
         substr(regexp_replace(to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss'),'[^.0-9]',''),0, 8)
    group by t1.当事人) y
    on x.当事人 = y.当事人


    """
    cur25.execute(cmd)
    ktgg_num_dic = {}
    for info in cur25:
        company = info[0]
        count1 = info[1]
        count2 = KillNone1(info[2])
        ktgg_num_dic[company] = [count1, count2]

    cur25.close()
    conn25.close()
    return ktgg_num_dic


# 获取到当前执行个数信息
def get_execute_num():
    connect_25 = connect_oracle_25()  # 对应的是案件管理的表
    conn25 = connect_25[0]
    cur25 = connect_25[1]

    cmd = """
    select k1.被执行人姓名_名称,总失信个数,当前失信个数 from (
    select x.被执行人姓名_名称, count(distinct x.案号) as 总失信个数
    from (select a.案号, a.被执行人姓名_名称
          from ZWRZB.SHXX_SXGS_ALL a
         inner join oa_user.开庭公告_联系人@DBLINKE_192_89 b
            on a.被执行人姓名_名称 = b.当事人) x
    group by x.被执行人姓名_名称) k1
    left join (select x1.被执行人姓名_名称, count(distinct x1.案号) as 当前失信个数
    from (select a1.案号, a1.被执行人姓名_名称
          from ZWRZB.SHXX_SXGS_ALL a1
         inner join oa_user.开庭公告_联系人@DBLINKE_192_89 b1
            on a1.被执行人姓名_名称 = b1.当事人 where a1.delete_flag is null) x1
    group by x1.被执行人姓名_名称) k2 on k1.被执行人姓名_名称 = k2.被执行人姓名_名称
    """

    cur25.execute(cmd)

    execute_num_dic = {}
    for info in cur25:
        company = info[0]
        count1 = info[1]
        count2 = KillNone1(info[2])
        execute_num_dic[company] = [count1, count2]

    cur25.close()
    conn25.close()
    return execute_num_dic


ktgg_num_dic = get_ktgg_num()
execute_num_dic = get_execute_num()


# 批量插入oracel
def bulk_into_oracel(connect, cursor, sql_cmd, bulk_para):
    try:
        # cursor.prepare(sql_cmd)
        # cursor.executemany(None, bulk_para)
        cursor.executemany(sql_cmd, bulk_para)
        connect.commit()
    except Exception as e:
        print(e, sql_cmd)
        print(bulk_para)
        connect.rollback()


# oracel 批量插入数据库
def oracel_blob_bulk_insert(conn, cursor, para_list, field_name_list, tb_name):
    values_num = len(para_list[0])  # 获取字段的个数
    values_data = ''.join([':' + str(i) + ',' for i in range(1, values_num + 1)])  # 生成拼接字段结果
    values_data = values_data[:-1]  # 将最后一个逗号去掉
    insert_cmd = "INSERT INTO " + tb_name + field_name_list + " VALUES (" + values_data + ")"
    bulk_into_oracel(conn, cursor, insert_cmd, para_list)  # 多条批量插入


ori_search_company = ''


# 企业的实际控股人
def get_real_master(cur, search_company_list, insert_cur, insert_conn, field_name_list, out_put_tb_name):
    global recur_count
    global ori_search_company
    recur_count += 1

    # 根据公司名搜索出来股东信息
    next_search_company_list = []

    para_list = []
    for each_company in search_company_list:
        # 对公司进行清洗，防止一些特殊符号造成sql出错
        each_company = ''.join(re.findall('[一-龥0-9-()（）a-zA-z]', each_company))
        # search_cmd = """
        # select t.entname, t.inv, t.注册资本, t.出资比例,t1.*
        # from DATA_USER.E_INV_INVESTMENT t
        # inner join (select a.pripid,
        #             a.公司类型,
        #             a.所属行业,
        #             a.opscope,
        #             a.登记机关,
        #             a.dom
        #        from DATA_USER.ENTERPRISEBASEINFOCOLLECT_1 a
        #       WHERE a.entname = '%s'
        #         and rownum = 1) t1
        # on t.pripid = t1.pripid
        # """%(each_company)

        search_cmd = """
                SELECT
            t1.ent_name,
            t. NAME,
            t.sub_money,
            t.sub_rate,
            t1.unified_code,
            t1.ent_type,
            t1.industry,
            t1.scope,
            t1.authority,
            t1.reg_addr
            FROM
            qcc_b.td_gov_company_shares t
            INNER JOIN (
            SELECT
                a.ent_name,
                a.unified_code,
                a.ent_type,
                a.industry,
                a.scope,
                a.authority,
                a.reg_addr
            FROM
                qcc_b.td_gov_company_basic a
            WHERE
                a.ent_name = '%s'
            LIMIT 1
            ) t1 ON t.unified_code = t1.unified_code
        """ % (each_company)

        # print(search_cmd)
        cur.execute(search_cmd)

        each_level_info = []

        for i in cur:
            entname = char_transform(KillNone(i[0]))
            invest_entname = char_transform(KillNone(i[1])).replace("'", '')
            reg_capital = KillNone1(i[2].replace('-', ''))
            reg_percent = KillNone1(i[3].replace('-', '').replace('%', ''))
            if len(reg_percent) == 0:
                reg_percent = '0'
            pripid = i[4]
            company_type = KillNone(i[5])
            industry = KillNone(i[6])
            scope = KillNone(i[7])
            authority = KillNone(i[8])
            reg_addr = KillNone(i[9])

            each_level_info.append(
                [entname, invest_entname, reg_capital, reg_percent, pripid, company_type, industry, scope, authority,
                 reg_addr])

        if each_level_info:
            # 持股占比最大的股东信息
            max_master_info = sorted(each_level_info, key=lambda x: float(x[3]), reverse=True)[0]  # 按照年份从大到小排序
            max_master_company = [max_master_info[1]]

            # 有的股东持股比例没有写，用killnone转化为0了，这种没写持股比例的股东也要搜索一遍
            # invalid_master_info = [i[1] for i in each_level_info if i[-1]==0]

            # 下面这两行代码就包括了两种情况：股东没有持股比例，或者两大股东持股比例相同，则都查找一遍
            max_percent = max_master_info[3]
            invalid_master_info = [i[1] for i in each_level_info if i[3] == max_percent]

            next_search_company_list += list(set(max_master_company + invalid_master_info))

            each_level_info_final = []
            for i in each_level_info:
                if i[1] in next_search_company_list:  # 标记是否是实际控股人
                    i_new = [char_transform(ori_search_company)] + i + [recur_count, 1]
                else:
                    i_new = [char_transform(ori_search_company)] + i + [recur_count, 0]
                each_level_info_final.append(i_new)

                each_para = tuple(i_new)
                para_list += [each_para]
            # print('第%s层股东信息为%s' % (recur_count, each_level_info_final))
        # 20220218新增 如果没有股东信息，则直接加入到结果表中
        else:
            para_list += [(each_company, each_company, each_company, '0', '0', '0', '0', '0', '0', '0', '0', 1, 1)]
            oracel_blob_bulk_insert(insert_conn, insert_cur, para_list, field_name_list, out_put_tb_name)

    if next_search_company_list and recur_count <= 10:
        # print(para_list)
        oracel_blob_bulk_insert(insert_conn, insert_cur, para_list, field_name_list, out_put_tb_name)
        # print('下一次搜索公司列表:',next_search_company_list)
        next_search_company_list1 = [char_transform(i) for i in next_search_company_list]
        # 在转换为全角
        next_search_company_list2 = [char_transform1(i) for i in next_search_company_list]
        # 全角半角合并去重
        next_search_company_list_final = list(set(next_search_company_list1 + next_search_company_list2))

        # 下一次搜索的时候，需要将待搜索的公司全角半角都生成出来，然后进行搜索，否则可能搜索不全

        get_real_master(cur, next_search_company_list_final, insert_cur, insert_conn, field_name_list, out_put_tb_name)




def B2Q(uchar):
    """单个字符 半角转全角"""
    inside_code = ord(uchar)
    if inside_code < 0x0020 or inside_code > 0x7e:  # 不是半角字符就返回原来的字符
        return uchar
    if inside_code == 0x0020:  # 除了空格其他的全角半角的公式为: 半角 = 全角 - 0xfee0
        inside_code = 0x3000
    else:
        inside_code += 0xfee0
    return chr(inside_code)


# 半角转全角
def char_transform1(text):
    return "".join([B2Q(uchar) for uchar in text])


def connect_mysql_getdata(dbname):
    conn = pymysql.connect(host="192.168.0.12", port=3306, charset='utf8', user='root', passwd='Abc123456.', db=dbname)
    cur = conn.cursor(pymysql.cursors.SSCursor)
    cur.execute('set session net_write_timeout = 100000')
    return conn, cur


def connect_oracle_89():
    # 配置说明:用户名 密码 ip:端口号/数据库名
    connection = cx_Oracle.connect("ZXY", "Zxy123456", "192.168.0.89:1521/orcl")
    cur = connection.cursor()
    return connection, cur


# 要排除掉过滤掉的企业
def get_all_search_company(search_tb_name, master_info_output_tb_name):
    connect_25 = connect_oracle_25()  # 对应的是案件管理的表
    # connect_25 = connect_mysql_getdata('qcc_b')
    # connect_25 = connect_oracle_89()# 对应的是开庭公告的表
    conn25 = connect_25[0]
    cur25 = connect_25[1]
    # 案件管理表中的公司
    # get_search_company_cmd = """
    # select distinct b.债权人名称
    # from zwrzb_new.债务人线索_摘要表2 a
    # inner join zwrzb_new.债务人失信数据_债权人 b
    # on a.案号 = b.案号
    # where b.最新 = '1'
    # and length(b.债权人名称) > 3 and
    # not exists(select 1 from %s t where b.债权人名称 = t.entname_ori)
    # """ % (master_info_output_tb_name)

    get_search_company_cmd = """
    select distinct t.当事人 from %s t 
    where not exists 
     (select 1 from %s t1 where t.当事人 = t1.entname_ori) and not exists
     (select 1 from  ktgg已过滤当事人 t2 where t.当事人 = t2.当事人)
    """ % (search_tb_name, master_info_output_tb_name)

    # get_search_company_cmd = "select * from 开庭公告当事人"
    # 入口表中筛选的公司
    # get_search_company_cmd = """
    #    select distinct 公司名称
    # from zwrzb_new.债务人入口摘要表 t
    # where t.delete_flag = '0'
    #     """
    # get_search_company_cmd = """
    # select  t.公司名称 from 平台所有涉及的当事人 t
    # """
    # 开庭公告新增案件的被告
    # get_search_company_cmd = """
    #    select distinct 当事人 from OA_USER.开庭公告_当事人管理 t where t.状态 = '新增案件' and t.当事人类型 = '被告'
    #     """

    cur25.execute(get_search_company_cmd)

    # cur25 = [tuple(['神州数码（中国）有限公司'])]

    # 将公司先都转化为半角
    all_company = [char_transform(i[0]) for i in cur25]
    # 在转换为全角
    all_company1 = [char_transform1(i) for i in all_company]
    # 全角半角合并去重
    all_company_final = list(set(all_company + all_company1))
    cur25.close()
    conn25.close()

    return all_company_final


def get_all_master_info(all_search_company, out_put_tb_name):
    dbname = 'qcc_b'
    my_connect = connect_mysql_getdata_12(dbname)
    conn = my_connect[0]
    cur = my_connect[1]

    # insert_connect = connect_oracle_25()
    insert_connect = connect_oracle()  # 插入本地
    insert_conn = insert_connect[0]
    insert_cur = insert_connect[1]
    insert_cur.setinputsizes(None, None, cx_Oracle.CLOB)

    global ori_search_company
    global recur_count
    global each_company_all_master_info

    field_name_list = "(entname_ori,entname_tmp,entname_inv,reg_capital,reg_percent,pripid,company_type,industry,scope,authority,reg_addr,master_level,master_status)"  # 结果表字段

    # all_search_company = ['中国长城资产管理股份有限公司广东省分公司']

    count = 0
    for company in all_search_company:
        try:
            count += 1
            # print('正在查找第 %s 个公司...' % (count), company)
            ori_search_company = company
            recur_count = 0
            # ori_search_company = "重庆文化产业融资担保有限责任公司"

            search_company_list = [ori_search_company]

            get_real_master(cur, search_company_list, insert_cur, insert_conn, field_name_list, out_put_tb_name)
        except Exception as e:
            print(e)


# 数据库多次请求，后续会逐渐变慢，具体原因不知道怎么回事，先直接将所有数据都查询出来吧，然后做成字典，key是 entname_ori value是一个list，
# list中每个元素都是一个实际控股信息数据
def get_data_dict(input_tb_name, out_put_tb_name):
    conn_orcl = connect_oracle_24()
    conn = conn_orcl[0]
    cur = conn_orcl[1]
    # cmd = """
    #     select distinct k1.entname_ori,
    #                 k1.entname_tmp,
    #                 k1.entname_inv,
    #                 k1.company_type
    #     from (select distinct t.entname_ori
    #           from 企业实际控制人关联查找 t
    #          where not exists (select 1
    #                   from 平台公司国企标记 t1
    #                  where t.entname_ori = t1.company)) k
    #     inner join 企业实际控制人关联查找 k1
    #     on k.entname_ori = k1.entname_ori
    #     where k1.master_status = 1
    #
    #     """

    cmd = """
            select distinct k1.entname_ori,
                        k1.entname_tmp,
                        k1.entname_inv,
                        k1.company_type
            from (select distinct t.entname_ori
                  from %s t
                 where not exists (select 1
                          from %s t1
                         where t.entname_ori = t1.company)) k
            inner join %s k1
            on k.entname_ori = k1.entname_ori
            where k1.master_status = 1

            """ % (input_tb_name, out_put_tb_name, input_tb_name)

    cur.execute(cmd)
    data_dict = {}
    for info in cur:
        ent_ori = info[0]
        if ent_ori in data_dict:
            tmp_list = data_dict[ent_ori]
            tmp_list += [info[1:]]
            data_dict[ent_ori] = tmp_list
        else:
            data_dict[ent_ori] = [info[1:]]
    cur.close()
    conn.close()
    return data_dict


# 获取到已经解析过的公司名称
def get_reuslt_company(out_put_tb_name):
    conn_orcl = connect_oracle_24()
    conn = conn_orcl[0]
    cur = conn_orcl[1]
    cmd = "select distinct company from %s " % (out_put_tb_name)
    cur.execute(cmd)
    all_company = set([i[0] for i in cur])
    cur.close()
    conn.close()
    return all_company


def guoqi_recongize_fix(input_tb_name, out_put_tb_name, all_search_company):
    conn_orcl = connect_oracle_24()
    conn = conn_orcl[0]
    cur = conn_orcl[1]

    conn_orcl_insert = connect_oracle_24()
    conn_insert = conn_orcl_insert[0]
    cur_insert = conn_orcl_insert[1]

    have_dealed_company = get_reuslt_company(out_put_tb_name)
    # all_company = ['北洋国家精馏技术工程发展有限公司']
    # out_put_tb_name = '平台公司国企标记'

    field_name_list = '(company,label,match_text)'

    country_regex = '国有|政府|人民|改革|省投资|省建设|区投资|区建设|市投资|市建设|省金融|市金融|区金融|省资产|市资产|区资产|开发投资|能源投资|区管委会|区管理委员会|国家|国资|国企|国务院|医院|学校|大学|学院|中学|\\(上市|中石油|中石化' \
                    '|中铁|国网|法院|华融资产|东方资产|信达资产|长城资产|市信用|市融资|信用合作|银行|分行|支行|信用社|储蓄所|律师|律所|保险|村民委员会|居民委员会|保障服务中心|国土|小学|监狱|中国联合网络通信|中移铁通|登记中心|街道办事处|公积金|妇幼保健院|城市规划'

    regex1 = '^(?:中国|中建|中交)|(?:委|局)$'

    vaild_regex = '培训学校'

    bulk_insert_num = 100
    para_list = []
    count = 0
    data_dict = get_data_dict(input_tb_name, out_put_tb_name)

    shangshi_dic = shagnshi_company_dic()  # 获取到上市公司

    # data_dict = {'郑州黑铁商贸有限公司':[('郑州黑铁商贸有限公司','王政委','有限责任公司(自然人独资)')]}

    # 20211210新增 需要将无法查询到工商信息的公司加入到待识别国企标签的标记结果中
    # 需要将原始的公司都转换为半角，防止重复添加

    all_search_company_new = set([char_transform(i) for i in all_search_company])
    count1 = 0
    for i in all_search_company_new:
        if i not in data_dict:
            if i not in have_dealed_company:
                tmp_value = [(i, '', '')]
                data_dict[i] = tmp_value
                count1 += 1
    print('无工商信息公司名添加完毕，共 %s 家' % (count1))

    # 每次只提取没有进行标记过的公司，然后进行标记
    for each_company, master_info in data_dict.items():
        each_company = ''.join(re.findall('[一-龥()]+', each_company))[:30]
        if each_company not in have_dealed_company:
            # if each_company=='重庆市南岸区筑成泽远职业技能培训学校有限公司':
            country_label = '未知'
            match_text = ''
            count += 1
            print(count, each_company)
            # print(master_info)

            if master_info:
                for info in master_info:
                    tmp_list = [i for i in info if len(KillNone(i)) > 3]
                    tmp_str = ','.join(tmp_list)
                    match_str = '_'.join(re.findall(country_regex, tmp_str))
                    # 因为可能匹配的是人名,所以进行一下判断
                    if len(match_str) > 0:
                        if not any(re.findall(vaild_regex, tmp_str)):
                            country_label = '国企'
                            # print('0'+tmp_str)
                            match_text = tmp_str
                    if country_label == '国企':
                        break
                    tmp_list1 = [info[0]]
                    tmp_list1 = [i for i in tmp_list1 if len(KillNone(i)) > 3]
                    tmp_list1_str = ','.join(tmp_list1)
                    match_str = '_'.join(re.findall(regex1, tmp_list1_str))
                    if len(match_str) > 0:
                        country_label = '国企'
                        # print('1'+tmp_list1_str)
                        match_text = info[0]
                    if country_label == '国企':
                        break

                    tmp_list2 = [info[1]]
                    tmp_list2 = [i for i in tmp_list2 if len(KillNone(i)) > 3]
                    tmp_list2_str = ','.join(tmp_list2)
                    match_str = '_'.join(re.findall(regex1, tmp_list2_str))
                    if len(match_str) > 0:
                        country_label = '国企'
                        match_text = info[1]
                        # print('2:'+tmp_list2_str)
                    if country_label == '国企':
                        break

                    # 20220128新增 如果控股股东为上市公司，直接认定为国企

                    shangshi_info = shangshi_dic & set(tmp_list)
                    if len(shangshi_info) > 0:
                        country_label = '国企'
                        match_text = ','.join(list(shangshi_info)) + '_控股股东为上市公司'
                    if country_label == '国企':
                        break

            each_para = tuple([each_company, country_label, match_text])
            para_list += [each_para]
            if len(para_list) == bulk_insert_num:
                oracel_blob_bulk_insert(conn_insert, cur_insert, para_list, field_name_list, out_put_tb_name)
                para_list.clear()

    if len(para_list) > 0:
        oracel_blob_bulk_insert(conn_insert, cur_insert, para_list, field_name_list, out_put_tb_name)


def key_word_judge(company):
    country_regex = '国有|政府|人民|改革|省投资|省建设|区投资|区建设|市投资|市建设|省金融|市金融|区金融|省资产|市资产|区资产|开发投资|能源投资|区管委会|区管理委员会|国家|国资|国企|国务院|医院|学校|大学|学院|中学|\\(上市|中石油|中石化' \
                    '|中铁|国网|法院|华融资产|东方资产|信达资产|长城资产|市信用|市融资|信用合作|银行|分行|支行|信用社|储蓄所|律师|律所|保险|村民委员会|居民委员会|保障服务中心|国土|小学|监狱|中国联合网络通信|中移铁通|登记中心|街道办事处|公积金|妇幼保健院|城市规划'

    regex1 = '^(?:中国|中建|中交)|(?:委|局)$'

    vaild_regex = '培训学校'

    country_label = '未知'
    match_text = ''

    match_str = '_'.join(re.findall(country_regex, company))
    # 因为可能匹配的是人名,所以进行一下判断
    if len(match_str) > 0:
        if not any(re.findall(vaild_regex, company)):
            country_label = '国企'
            match_text = company
            return country_label, match_text

    match_str1 = '_'.join(re.findall(regex1, company))
    if len(match_str1) > 0:
        country_label = '国企'
        match_text = company
        return country_label, match_text

    return country_label, match_text


# 国企检测漏掉的公司再检测一遍，直接用关键字检测
def guoqi_recongize_fix1(search_tb_name, company_sign_output_tb_name):
    conn_orcl = connect_oracle_24()
    conn = conn_orcl[0]
    cur = conn_orcl[1]

    conn_orcl_insert = connect_oracle_24()
    conn_insert = conn_orcl_insert[0]
    cur_insert = conn_orcl_insert[1]
    cmd = "select distinct t.当事人 from %s t where not exists(select 1 from %s t1 where t.当事人 = t1.company ) " % (
        search_tb_name, company_sign_output_tb_name)
    cur.execute(cmd)

    bulk_insert_num = 100
    count = 0
    para_list = []

    field_name_list = '(company,label,match_text)'

    for i in cur:
        count += 1
        company = i[0]
        country_label, match_text = key_word_judge(company)

        each_para = tuple([company, country_label, match_text])
        para_list += [each_para]
        if len(para_list) == bulk_insert_num:
            oracel_blob_bulk_insert(conn_insert, cur_insert, para_list, field_name_list, company_sign_output_tb_name)
            para_list.clear()

    if len(para_list) > 0:
        oracel_blob_bulk_insert(conn_insert, cur_insert, para_list, field_name_list, company_sign_output_tb_name)


# 给定公司标记国企结果
def master_company_sign(search_tb_name):
    # 首先获取到要跑实际控股的公司名list
    # all_search_company = ['凉山州通行公路机械化养护有限公司']

    master_info_output_tb_name = '企业实际控制人关联查找_ktgg'
    all_search_company = get_all_search_company(search_tb_name, master_info_output_tb_name)
    # # 实际控股公司结果表
    #

    print('正在关联企业实际控制信息。。。')
    print('需要关联企业个数共 %s 家' % (len(all_search_company)))
    get_all_master_info(all_search_company, master_info_output_tb_name)
    print('正在对企业进行实际控制人标记。。。')
    # 国企标记表

    company_sign_output_tb_name = 'ktgg被告国企标记'
    # company_sign_output_tb_name = '平台公司国企标记'
    guoqi_recongize_fix(master_info_output_tb_name, company_sign_output_tb_name, all_search_company)

    # 20220211新增 为了防止遗漏，最后再对所有不在结果表中的公司用关键字检测一遍
    guoqi_recongize_fix1(search_tb_name, company_sign_output_tb_name)
    print('国企关联完毕！')


def case_status_update():
    cmd = """
    insert into zxy.平台公司国企标记_log
    select x.uuid,
         'zxy',
         'com.ill.service.zwr.CaseService',
         '程序更改为无需联系_债权人识别为国企',
         '',
         sysdate,
         x.状态,
         sysdate,
         x.案号,
         x.债权人名称,
         y.match_text
    from (select distinct a.uuid, a.案号, a.债务人, a.状态, b.债权人名称
            from zwrzb_new.债务人线索_摘要表2 a
           inner join zwrzb_new.债务人失信数据_债权人 b
              on a.案号 = b.案号
           where b.最新 = '1') x
    inner join zxy.平台公司国企标记 y
      on x.债权人名称 = y.company
    where y.label = '国企'
     and x.状态 != '执行结案'
     and 状态 != '优先联系'
     and 状态 != '优先跟进'
     and 状态 != '无需联系'
    """
    update_connect = connect_oracle_24()
    update_conn = update_connect[0]
    update_cur = update_connect[1]
    update_cur.execute(cmd)
    update_conn.commit()

    cmd1 = """
    insert into zwrzb_new.CASE_MANAGE_LOG
    select t.uuid,
         t.user_name,
         t.request_mapping,
         t.operation_type,
         t.param_text,
         t.request_date
    from 平台公司国企标记_LOG t
    where not exists (select 1
            from zwrzb_new.CASE_MANAGE_LOG t1
           where t.uuid = t1.uuid
             and t.user_name = t1.user_name
             and t.operation_type = t1.operation_type)
    """
    update_cur.execute(cmd1)
    update_conn.commit()

    cmd2 = """
    update zwrzb_new.债务人线索_摘要表2 t
    set t.状态 = '无需联系',
       t.最新更新时间 =
       (select max(x.更新时间) from 平台公司国企标记_LOG x where t.uuid = x.uuid)
    where exists (select 1 from 平台公司国企标记_LOG t1 where t.uuid = t1.uuid)
    and t.状态 != '无需联系'
    """
    update_cur.execute(cmd2)
    update_conn.commit()


# 获取到该当事人是否过去找过律师代理(这种查找方式非常慢)
def lawyer_info(company, cur):
    # 先不用近几年这种进行过滤，但是后期可能会根据年份过滤，所以要把年份关联出来
    cmd1 = """
    select x.*, y.案号
    from (select *
          from data_court_user.cpws_case_person_info a
         where a.uuid in (select t.uuid
                            from data_court_user.cpws_case_person_info t
                           where t.当事人 = '%s' and t.当事人类型 = '原告')
           and a.当事人类型 like '%%原告%%'
           and a.当事人 != '0') x
    inner join data_court_user.cpws_basic_info y
    on x.uuid = y.uuid
    """ % (company)

    cur.execute(cmd1)
    info1 = cur.fetchall()
    year_regex = '[0-9\\-]+'
    # 包含代理律师、代理年份、代理案号
    info1_clean = [[i[1], ''.join(re.findall(year_regex, i[-1])[:1])[:4], i[-1]] for i in info1 if i[2] == '原告代理人']

    cmd2 = """

        select x.*, y.案号
        from (select *
              from data_court_user.cpws_case_person_info a
             where a.uuid in (select t.uuid
                                from data_court_user.cpws_case_person_info t
                               where t.当事人 = '%s' and t.当事人类型 = '被告')
               and a.当事人类型 like '%%被告%%'
               and a.当事人 != '0') x
        inner join data_court_user.cpws_basic_info y
        on x.uuid = y.uuid
        """ % (company)
    cur.execute(cmd2)
    info2 = cur.fetchall()
    info2_clean = [[i[1], ''.join(re.findall(year_regex, i[-1])[:1])[:4], i[-1]] for i in info2 if i[2] == '被告代理人']

    final_info = info1_clean + info2_clean

    # 有律师代理的所有案件数量
    final_info_case_no_num = len(set([i[-1] for i in final_info]))
    # 有律师代理的近三年的案件数量
    final_info_case_no_filter_num = len(
        set([i[-1] for i in final_info if i[1] == '2021' or i[1] == '2020' or i[1] == '2019']))

    return final_info_case_no_num, final_info_case_no_filter_num


# 直接从事先关联好的表中来找到对应的代理信息
def lawyer_info_fast(company, cur):
    cmd = "select t.律师,t.案号 from 当事人信息表_律师_案件 t where t.当事人 = '%s' " % (company)
    cur.execute(cmd)
    info = cur.fetchall()
    year_regex = '[0-9\\-]+'
    # 包含代理律师、代理年份、代理案号
    final_info = [[i[0], ''.join(re.findall(year_regex, i[1])[:1])[:4], i[1]] for i in info]
    # 有律师代理的所有案件数量
    final_info_case_no_num = len(set([i[-1] for i in final_info]))
    # 有律师代理的近三年的案件数量
    final_info_case_no_filter_num = len(
        set([i[-1] for i in final_info if i[1] == '2021' or i[1] == '2020' or i[1] == '2019']))
    return final_info_case_no_num, final_info_case_no_filter_num


# 获取到庞帅采集的百度失信 执行公开网失信 裁判文书涉案数量
def get_shixin_new():
    conn_getdata = connect_mysql_getdata_89('HLB_5')
    conn = conn_getdata[0]
    cur = conn_getdata[1]
    cmd = """
    SELECT
	company,
	BaiDuShiXinRen_ResultCount,
	ShiXinRen_ResultCount,
	CaiPanWenShu_ResultCount
    FROM
	`KTGG_DCSXGS` t
    WHERE
	t.BaiDuShiXinRen_ResultCount > 0
    OR t.CaiPanWenShu_ResultCount > 0
    OR t.ShiXinRen_ResultCount > 0;
    """
    cur.execute(cmd)

    shixin_dic = {}
    for info in cur:
        company = info[0]
        baidu_sx_num = int(KillNone1(info[1]))
        ori_sx_num = int(KillNone1(info[2]))
        wenshu_num = int(KillNone1(info[3]))
        shixin_dic[company] = [baidu_sx_num, ori_sx_num, wenshu_num]
    cur.close()
    conn.close()
    return shixin_dic


# 获取企业大股东的关联公司
def inv_relate_company_dic():
    conn_25 = connect_oracle()  # 插入本地
    conn = conn_25[0]
    cur = conn_25[1]

    conn_135 = connect_oracle135()
    conn1 = conn_135[0]
    cur1 = conn_135[1]

    cmd = """
    insert into data_gsk_user.待开庭公司@DBLINKE_192_135
    select distinct t.当事人
    from zxy.开庭公告_结构化@DBLINKE_192_89 t
    where length(t.当事人) > 4
     and (t.当事人类型 = '被告' or  t.当事人类型 = '被申请人' or t.当事人类型 = '被申诉人' or t.当事人类型 = '被上诉人'  or t.当事人类型 = '被执行人')
     and  t.开庭日期 >=substr(regexp_replace(to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss'),'[^.0-9]',''),0, 8)
   and not exists(select 1 from data_gsk_user.待开庭公司@DBLINKE_192_135 t1 where t.当事人 = t1.公司名称)
   and not exists(select 1 from ktgg已过滤当事人 t2 where t.当事人 = t2.当事人)
                    and not exists (select 1
                  from ktgg被告国企标记 t3
                 where t.当事人 = t3.company
                   and t3.label = '国企')
                    and not exists
                (select 1 from 上市公司 t4 where t.当事人 = t4.公司名称)
    and not exists
    (select 1 from zxy.开庭公告排除案由_汇总 t5 where t.案由 = t5.案由)        
    and t.案由 not like '%%物业%%'
     and t.案由 not like '%%殡%%'
     and t.案由 not like '%%资本认购%%'
     and t.案由 not like '%%罪%%'
     and t.案由 not like '%%涉嫌%%'
     and t.当事人 not like '%%培训%%'
     and t.当事人 not like '%%健身%%'            
            and t.当事人 not like '%%国有%%'
            and t.当事人 not like '%%政府%%'
            and t.当事人 not like '%%人民%%'
            and t.当事人 not like '%%改革%%'
            and t.当事人 not like '%%省投资%%'
            and t.当事人 not like '%%省建设%%'
            and t.当事人 not like '%%区投资%%'
            and t.当事人 not like '%%区建设%%'
            and t.当事人 not like '%%市投资%%'
            and t.当事人 not like '%%市建设%%'
            and t.当事人 not like '%%省金融%%'
            and t.当事人 not like '%%市金融%%'
            and t.当事人 not like '%%区金融%%'
            and t.当事人 not like '%%省资产%%'
            and t.当事人 not like '%%市资产%%'
            and t.当事人 not like '%%区资产%%'
            and t.当事人 not like '%%开发投资%%'
            and t.当事人 not like '%%能源投资%%'
            and t.当事人 not like '%%区管委会%%'
            and t.当事人 not like '%%区管理委员会%%'
            and t.当事人 not like '%%全国%%'
            and t.当事人 not like '%%国家%%'
            and t.当事人 not like '%%国资%%'
            and t.当事人 not like '%%集体%%'
            and t.当事人 not like '%%国营%%'
            and t.当事人 not like '%%国企%%'
            and t.当事人 not like '%%国务院%%'
            and t.当事人 not like '%%医院%%'
            and t.当事人 not like '%%大学%%'
            and t.当事人 not like '%%学院%%'
            and t.当事人 not like '%%中学%%'
            and t.当事人 not like '%%小学%%'
            and t.当事人 not like '%%中石油%%'
            and t.当事人 not like '%%中石化%%'
            and t.当事人 not like '%%中铁%%'
            and t.当事人 not like '%%国网%%'
            and t.当事人 not like '%%法院%%'
            and t.当事人 not like '%%华融资产%%'
            and t.当事人 not like '%%东方资产%%'
            and t.当事人 not like '%%信达资产%%'
            and t.当事人 not like '%%长城资产%%'
            and t.当事人 not like '%%市信用%%'
            and t.当事人 not like '%%市融资%%'
            and t.当事人 not like '%%信用合作%%'
            and t.当事人 not like '%%银行%%'
            and t.当事人 not like '%%分行%%'
            and t.当事人 not like '%%支行%%'
            and t.当事人 not like '%%信用社%%'
            and t.当事人 not like '%%储蓄所%%'
            and t.当事人 not like '%%律师%%'
            and t.当事人 not like '%%律所%%'
            and t.当事人 not like '%%保险%%'
            and t.当事人 not like '%%村民委员会%%'
            and t.当事人 not like '%%居民委员会%%'
            and t.当事人 not like '%%居委会%%'
            and t.当事人 not like '%%村委会%%'
            and t.当事人 not like '%%保障服务中心%%'
            and t.当事人 not like '%%国土%%'
            and t.当事人 not like '%%监狱%%'
            and t.当事人 not like '%%中国联合网络通信%%'
            and t.当事人 not like '%%中移铁通%%'
            and t.当事人 not like '%%登记中心%%'
            and t.当事人 not like '%%街道办事处%%'
            and t.当事人 not like '%%公积金%%'
            and t.当事人 not like '%%妇幼保健院%%'
            and t.当事人 not like '%%城市规划%%'
            and t.当事人 not like '%%药房%%'
            and t.当事人 not  like '%%事务所%%'
            and t.当事人 not  like '%%部队%%'
            and t.当事人 not  like '%%公证处%%'
            and t.当事人 not  like '%%淘宝%%'
            and t.当事人 not  like '%%法律服务所%%'
            and t.当事人 not like'%%集团%%'
            and t.当事人 not like '%%置业%%'
            and t.当事人 not like '%%拍卖%%'
            and t.当事人 not like '%%担保%%'
            and t.当事人 not like '%%资产管理%%'
            and t.当事人 not like '%%私募%%'
            and t.当事人 not like '%%公募%%'
            and t.当事人 not like '%%期货%%'
            and t.当事人 not like '%%商业管理%%'
            and t.当事人 not like '%%房产开发%%'
            and t.当事人 not like '%%房地产开发%%'
            and t.当事人 not like '%%物业%%'
            and t.当事人 not like '%%殡%%'
            and t.当事人 not like '%%保障服务%%'
            and t.当事人 not like '%%人力资源和社会保障%%'
            and t.当事人 not like '%%恒大%%'
            and t.当事人 not like '%%万科%%'
            and t.当事人 not like '%%执法%%'
            and t.当事人 not like '%%司法%%'
            and t.当事人 not like '%%财政%%'
            and t.当事人 not like '%%房地产管理%%'
            and t.当事人 not like '中国%%'
            and t.当事人 not like '中建%%'
            and t.当事人 not like '中交%%'
            and t.当事人 not like '%%局'
            and t.当事人 not like '%%委'

    """
    cur.execute(cmd)
    conn.commit()

    cmd1 = """

    insert into  公司大股东联其他公司 
    select distinct c1.entname as 公司名, c1.inv as 公司大股东,c.cerno_18 as 公司大股东ID, c.entname as 公司大股东名下企业
    from e_inv_person_md c
    inner join (

             select a.entname, a.inv, b.cerno_18
               from (

                      select x.entname, x.inv
                        from (select tt.entname,
                                      tt.inv,
                                      row_number() over(partition by entname order by 出资比例 desc) rn
                                 from data_gsk_user.e_inv_investment tt
                                inner join data_gsk_user.待开庭公司 kk
                                   on tt.entname = kk.公司名称) x
                       where x.rn = 1
                         and length(x.inv) < 5) a
              inner join e_inv_person_md b
                 on a.entname = b.entname
                and a.inv = b.name) c1
    on c.cerno_18 = c1.cerno_18
    where not exists(select 1 from 公司大股东联其他公司 gg where c1.entname = gg.公司名 )

    """
    print('正在进行企业股东实际控制人名下企业关联！')
    cur1.execute(cmd1)
    conn1.commit()

    cmd2 = """
    select distinct a.公司名, b.大股东关联企业个数
    from 公司大股东联其他公司 a
    inner join (select t.公司大股东id,
                    count(distinct t.公司大股东名下企业) as 大股东关联企业个数
               from 公司大股东联其他公司 t
              where t.公司大股东名下企业 is not null
              group by t.公司大股东id) b
    on a.公司大股东id = b.公司大股东id

    """
    cur1.execute(cmd2)
    relate_ent_dic = {}

    for i in cur1:
        relate_ent_dic[i[0]] = i[1]

    print('企业股东实际控制人名下企业关联完毕！')

    cur.close()
    cur1.close()
    conn.close()
    conn1.close()

    return relate_ent_dic


def get_AC_auto_model():  # 生成AC自动机查询结构
    A = ahocorasick.Automaton()
    file_path1 = "C:\\Users\Administrator\Desktop\\知名企业相关\\五百强前缀.txt"
    # file_path1 = "/home/zxy/ktgg_filter/五百强前缀.txt"
    with open(file_path1, 'r', encoding='utf8') as f:
        for line in f:
            key_word = line.strip()
            A.add_word(key_word, key_word)
    A.make_automaton()

    # # 保存字典树
    # with open('dict_tree_model_100w.dat', 'wb') as f:
    #     pickle.dump(A, f)
    # # 加载字典树
    # with open('/home/mnt/html多进程提取腾讯词/tencent_dict_tree_del_onechar.dat', 'rb') as g:
    #     dict_tree = pickle.load(g)

    print('前缀字典树建立完毕！')
    return A


ac_tree = get_AC_auto_model()


# 查找公司名的前几个字是不是前缀字典中的
def ac_find(company):
    match_text = ''
    # 提取到的位置信息是匹配结果字符串的最后一个字符的index值
    # [(26, '延边众星建筑有限公司第八分公司')]  这种形式
    match_result = [i for i in ac_tree.iter(company)]
    # print(match_result)
    # status = 0
    if match_result:
        first_matct_index = match_result[0][0]
        first_matct_key_word = match_result[0][1]
        if first_matct_index == len(first_matct_key_word) - 1:
            # status =1
            match_text = first_matct_key_word
    return match_text





def get_relate_data(search_tb_name):
    dbname = 'qcc_b'
    my_connect = connect_mysql_getdata_12(dbname)
    conn = my_connect[0]
    cur = my_connect[1]

    # insert_connect = connect_oracle_25()
    insert_connect = connect_oracle()  # 插入本地
    insert_conn = insert_connect[0]
    insert_cur = insert_connect[1]
    insert_cur.setinputsizes(None, None, cx_Oracle.CLOB)

    conn_orcl = connect_oracle_24()
    conn_or = conn_orcl[0]
    cur_or = conn_orcl[1]

    conn_orcl1 = connect_oracle_24()
    conn_or1 = conn_orcl1[0]
    cur_or1 = conn_orcl1[1]

    conn_orcl2 = connect_oracle_24()
    conn_or2 = conn_orcl2[0]
    cur_or2 = conn_orcl2[1]

    # 进行当事人经营信息关联的时候，需要过滤掉国企 上市公司等，这样会加速关联，因为大型企业关联速度很慢
    out_put_tb_name = '债权人企业基本信息_ktgg'

    cmd = """select distinct t.当事人 from %s t 
                     where not exists((select 1 from  ktgg已过滤当事人 t0 where t.当事人 = t0.当事人))
                    and not exists (select 1
                  from ktgg被告国企标记 t2
                 where t.当事人 = t2.company
                   and t2.label = '国企')
                    and not exists
                (select 1 from 上市公司 t3 where t.当事人 = t3.公司名称)
                and t.当事人 not like '%%培训%%'
                and t.当事人 not like '%%健身%%' 
             and t.当事人 not like '%%国有%%'
            and t.当事人 not like '%%政府%%'
            and t.当事人 not like '%%人民%%'
            and t.当事人 not like '%%改革%%'
            and t.当事人 not like '%%省投资%%'
            and t.当事人 not like '%%省建设%%'
            and t.当事人 not like '%%区投资%%'
            and t.当事人 not like '%%区建设%%'
            and t.当事人 not like '%%市投资%%'
            and t.当事人 not like '%%市建设%%'
            and t.当事人 not like '%%省金融%%'
            and t.当事人 not like '%%市金融%%'
            and t.当事人 not like '%%区金融%%'
            and t.当事人 not like '%%省资产%%'
            and t.当事人 not like '%%市资产%%'
            and t.当事人 not like '%%区资产%%'
            and t.当事人 not like '%%开发投资%%'
            and t.当事人 not like '%%能源投资%%'
            and t.当事人 not like '%%区管委会%%'
            and t.当事人 not like '%%区管理委员会%%'
            and t.当事人 not like '%%全国%%'
            and t.当事人 not like '%%国家%%'
            and t.当事人 not like '%%国资%%'
            and t.当事人 not like '%%集体%%'
            and t.当事人 not like '%%国营%%'
            and t.当事人 not like '%%国企%%'
            and t.当事人 not like '%%国务院%%'
            and t.当事人 not like '%%医院%%'
            and t.当事人 not like '%%大学%%'
            and t.当事人 not like '%%学院%%'
            and t.当事人 not like '%%中学%%'
            and t.当事人 not like '%%小学%%'
            and t.当事人 not like '%%中石油%%'
            and t.当事人 not like '%%中石化%%'
            and t.当事人 not like '%%中铁%%'
            and t.当事人 not like '%%国网%%'
            and t.当事人 not like '%%法院%%'
            and t.当事人 not like '%%华融资产%%'
            and t.当事人 not like '%%东方资产%%'
            and t.当事人 not like '%%信达资产%%'
            and t.当事人 not like '%%长城资产%%'
            and t.当事人 not like '%%市信用%%'
            and t.当事人 not like '%%市融资%%'
            and t.当事人 not like '%%信用合作%%'
            and t.当事人 not like '%%银行%%'
            and t.当事人 not like '%%分行%%'
            and t.当事人 not like '%%支行%%'
            and t.当事人 not like '%%信用社%%'
            and t.当事人 not like '%%储蓄所%%'
            and t.当事人 not like '%%律师%%'
            and t.当事人 not like '%%律所%%'
            and t.当事人 not like '%%保险%%'
            and t.当事人 not like '%%村民委员会%%'
            and t.当事人 not like '%%居民委员会%%'
            and t.当事人 not like '%%居委会%%'
            and t.当事人 not like '%%村委会%%'
            and t.当事人 not like '%%保障服务中心%%'
            and t.当事人 not like '%%国土%%'
            and t.当事人 not like '%%监狱%%'
            and t.当事人 not like '%%中国联合网络通信%%'
            and t.当事人 not like '%%中移铁通%%'
            and t.当事人 not like '%%登记中心%%'
            and t.当事人 not like '%%街道办事处%%'
            and t.当事人 not like '%%公积金%%'
            and t.当事人 not like '%%妇幼保健院%%'
            and t.当事人 not like '%%城市规划%%'
            and t.当事人 not like '%%药房%%'
            and t.当事人 not  like '%%事务所%%'
            and t.当事人 not  like '%%部队%%'
            and t.当事人 not  like '%%公证处%%'
            and t.当事人 not  like '%%淘宝%%'
            and t.当事人 not  like '%%法律服务所%%'
            and t.当事人 not like'%%集团%%'
            and t.当事人 not like '%%置业%%'
            and t.当事人 not like '%%拍卖%%'
            and t.当事人 not like '%%担保%%'
            and t.当事人 not like '%%资产管理%%'
            and t.当事人 not like '%%私募%%'
            and t.当事人 not like '%%公募%%'
            and t.当事人 not like '%%期货%%'
            and t.当事人 not like '%%商业管理%%'
            and t.当事人 not like '%%房产开发%%'
            and t.当事人 not like '%%房地产开发%%'
            and t.当事人 not like '%%物业%%'
            and t.当事人 not like '%%殡%%'
            and t.当事人 not like '%%保障服务%%'
            and t.当事人 not like '%%人力资源和社会保障%%'
            and t.当事人 not like '%%恒大%%'
            and t.当事人 not like '%%万科%%'
            and t.当事人 not like '%%执法%%'
            and t.当事人 not like '%%司法%%'
            and t.当事人 not like '%%财政%%'
            and t.当事人 not like '%%房地产管理%%'
            and t.当事人 not like '中国%%'
            and t.当事人 not like '中建%%'
            and t.当事人 not like '中交%%'
            and t.当事人 not like '%%局'
            and t.当事人 not like '%%委' """ % (
        search_tb_name)
    cur_or.execute(cmd)
    all_company_info = [i for i in cur_or]

    para_list = []

    bulk_insert_num = 50
    field_name_list = '(公司名称,经营状态,行业,注册资本,注册资本类型,企业类型,总营收,社保人数,代理案件总量,代理案件总量_近三年,开庭总个数,待开庭个数,失信总个数,正在失信个数,案件地域,insert_time,上市标记,国企标记,百度失信个数,执行公开失信个数,裁判文书个数,知名企业,股东名下公司个数,裁判文书当事人,付过律师费)'

    insert_time = datetime.datetime.now()

    # 获取到庞帅采集的百度失信 执行公开网失信 裁判文书涉案数量
    shixin_dic = get_shixin_new()

    baidu_sx_num = -1
    ori_sx_num = -1
    wenshu_num = -1

    ent_master_relate_company_dic = inv_relate_company_dic()  # 获取到企业实控人的关联企业个数
    master_ent_num = -1

    count = 0
    print('需要跑企业基本信息公司共 %s 个' % (len(all_company_info)))

    for each_company_info in all_company_info:
        each_company = each_company_info[0]
        case_district = ''
        each_company = ''.join(re.findall('[一-龥()（）]+', each_company))[:50]
        count += 1
        print(count, each_company)
        each_company1 = char_transform1(each_company)
        cmd1 = "select max(t.vendinc) from 18all.a_assetsinfo_清洗 t where t.entname = '%s'" % (each_company1)
        cur.execute(cmd1)
        vendinc = 0
        info1 = cur.fetchall()
        if info1:
            vendinc = float(format(float(KillNone1(info1[0][0])), '.2f'))

        cmd2 = "select t.reg_capital,t.open_status,t.industry,t.ent_type from qcc_b.td_gov_company_basic t where t.ent_name = '%s' limit 1 " % (
            each_company)
        cur.execute(cmd2)

        uint_regex = '美元|欧元|港币|日元|韩元'
        open_status = ''
        reg_capital = 0
        reg_capital_unit = ''  # 注册资本单位
        industry = ''
        ent_type = ''

        info2 = cur.fetchall()
        if info2:
            reg_capital_info = KillNone1(info2[0][0])
            reg_capital_str = ''.join(
                re.findall('[0-9\\.]+', KillNone(reg_capital_info.replace(',', '').replace('-', ''))))
            if not reg_capital_str:
                reg_capital_str = '0'
            reg_capital = float(format(float(reg_capital_str), '.2f'))

            reg_capital_unit = ''.join(re.findall(uint_regex, reg_capital_info)[:1])
            if len(reg_capital_unit) < 2:
                reg_capital_unit = '人民币'

            open_status = KillNone(info2[0][1])
            industry = KillNone(info2[0][2])
            ent_type = KillNone(info2[0][3])

        social_fee_num = 0
        cmd2 = "select max(t.so3) from 18all.a_socialfee t where t.entname = '%s' " % (each_company)
        cur.execute(cmd2)
        info3 = cur.fetchall()
        if info3:
            social_fee_num = int(KillNone1(info3[0][0]))

        # 获取该公司是否存在于裁判文书的当事人中
        exist_wenshu = 0
        cmd3 = "select * from data_court_user.cpws_case_person_info t where t.当事人 = '%s' and rownum=1"%(each_company)

        cur_or2.execute(cmd3)
        info4 = cur_or2.fetchall()
        if info4:
            exist_wenshu = 1

        # 获取该公司是否存在于付过律师费的公司中

        pay_lawyerfee_staus = 0
        company_new = char_transform(each_company)
        if company_new in pay_lawyerfee_company:
            pay_lawyerfee_staus = 1


        # 获取律师代理信息  律师代理的所有案件 律师代理的最近三年的案件
        agent_case_num, agent_case_num_filter = lawyer_info_fast(each_company, cur_or1)

        # 开庭公告总数
        ktgg_all_num = -1
        # 待开庭个数
        ktgg_future_num = -1
        if each_company in ktgg_num_dic:
            ktgg_num_info = ktgg_num_dic[each_company]
            ktgg_all_num = ktgg_num_info[0]
            ktgg_future_num = ktgg_num_info[1]

        sx_all_num = -1
        sx_current_num = -1

        if each_company in execute_num_dic:
            sx_num_info = execute_num_dic[each_company]
            sx_all_num = sx_num_info[0]
            sx_current_num = sx_num_info[1]

        if each_company in shixin_dic:
            new_sx_info = shixin_dic[each_company]
            baidu_sx_num = new_sx_info[0]
            ori_sx_num = new_sx_info[1]
            wenshu_num = new_sx_info[2]

        if each_company in ent_master_relate_company_dic:
            master_ent_num = ent_master_relate_company_dic[each_company]

        # 知名品牌查找
        famous_brand = ac_find(each_company)

        each_para = tuple(
            [each_company, open_status, industry, reg_capital, reg_capital_unit, ent_type, vendinc, social_fee_num,
             agent_case_num, agent_case_num_filter, ktgg_all_num, ktgg_future_num, sx_all_num, sx_current_num,
             case_district, insert_time, '', '', baidu_sx_num, ori_sx_num, wenshu_num, famous_brand, master_ent_num,exist_wenshu,pay_lawyerfee_staus])
        para_list += [each_para]
        if len(para_list) == bulk_insert_num:
            oracel_blob_bulk_insert(insert_conn, insert_cur, para_list, field_name_list, out_put_tb_name)
            para_list.clear()

    if len(para_list) > 0:
        oracel_blob_bulk_insert(insert_conn, insert_cur, para_list, field_name_list, out_put_tb_name)

    cur.close()
    conn.close()
    insert_cur.close()
    insert_conn.close()
    cur_or.close()
    conn_or.close()


# 黑名单定时检测，修改案件
def case_status_update3():
    update_connect = connect_oracle_24()
    update_conn = update_connect[0]
    update_cur = update_connect[1]

    cmd1 = """
    update zwrzb_new.债务人线索_摘要表2 t
    set t.状态 = '无需联系', t.最新更新时间 = sysdate
    where t.状态 != '无需联系'
    and exists (select 1
          from ZWRZB_NEW.债务人平台公司黑名单 t1
         where t.债务人 = t1.公司名称
           and t1.公司类型 = '被执行人')
    """

    update_cur.execute(cmd1)
    update_conn.commit()

    time.sleep(10)

    cmd2 = """

    update zwrzb_new.债务人线索_摘要表2 t
    set t.状态 = '无需联系', t.最新更新时间 = sysdate
    where t.状态 != '无需联系'
    and exists(select 1 from (select distinct a.案号
    from zwrzb_new.债务人失信数据_债权人 a
    inner join ZWRZB_NEW.债务人平台公司黑名单 b
    on a.债权人名称 = b.公司名称
    where b.公司类型 = '债权人') k where k.案号 = t.案号)
    """
    update_cur.execute(cmd2)
    update_conn.commit()
    print('案件管理黑名单企业关联无需联系完毕！')


# 获取到排除已经识别出来的公司的入口公司，重新再识别找出是否存在过代理律师
def get_company1():
    conn_orcl = connect_oracle_24()
    conn = conn_orcl[0]
    cur = conn_orcl[1]
    cmd = "select distinct t.当事人 from zxy.开庭公告当事人 t where not exists(select 1 from ktgg已过滤当事人 t1 where t.当事人=t1.当事人)"
    cur.execute(cmd)
    all_company = [i[0] for i in cur]
    cur.close()
    conn.close()
    return all_company


# 获取到增量的公司，这里获取很简单，直接从开庭公告结构化中获取当日以及以后开庭的结果
def get_increased_company(search_tb_name):
    # 先建立一个表，这个表里面存的是目前需要跑指标的公司
    """
    create table zxy.开庭公告当事人增量表 as
    select distinct t.当事人
    from zxy.开庭公告_结构化@DBLINKE_192_89 t
    where length(t.当事人) > 4
   and t.当事人类型 = '原告'
   and t.开庭日期 > '20220201'
    :return:
    """

    conn_orcl = connect_oracle_24()
    conn = conn_orcl[0]
    cur = conn_orcl[1]

    # 然后隔一段时间获取一次增量的需要跑国企等指标的当事人，这里要剔除掉已经被筛掉的当事人，重复跑没有意义

    # 每次插入增量表中的开庭公告被告为   开庭日期为当日及以后的所有被告公司 排除掉 目前已经识别的国企 上市公司 案由 关键字 并且不能在增量表中，不能在已经过滤的表中

    get_data_cmd = """
    insert into %s
    select distinct t.当事人
    from zxy.开庭公告_结构化@DBLINKE_192_89 t
    where length(t.当事人) > 4
     and (t.当事人类型 = '被告' or  t.当事人类型 = '被申请人' or t.当事人类型 = '被申诉人' or t.当事人类型 = '被上诉人'  or t.当事人类型 = '被执行人')
     and  t.开庭日期 >=substr(regexp_replace(to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss'),'[^.0-9]',''),0, 8)
     and not exists
   (select 1 from %s t1 where t.当事人 = t1.当事人)
   and not exists(select 1 from ktgg已过滤当事人 t2 where t.当事人 = t2.当事人)
                    and not exists (select 1
                  from ktgg被告国企标记 t3
                 where t.当事人 = t3.company
                   and t3.label = '国企')
                    and not exists
                (select 1 from 上市公司 t4 where t.当事人 = t4.公司名称)
    and not exists
    (select 1 from zxy.开庭公告排除案由_汇总 t5 where t.案由 = t5.案由)        
    and t.案由 not like '%%物业%%'
     and t.案由 not like '%%殡%%'
     and t.案由 not like '%%资本认购%%'
     and t.案由 not like '%%罪%%'
     and t.案由 not like '%%涉嫌%%'  
     and t.当事人 not like '%%培训%%'
     and t.当事人 not like '%%健身%%'            
            and t.当事人 not like '%%国有%%'
            and t.当事人 not like '%%政府%%'
            and t.当事人 not like '%%人民%%'
            and t.当事人 not like '%%改革%%'
            and t.当事人 not like '%%省投资%%'
            and t.当事人 not like '%%省建设%%'
            and t.当事人 not like '%%区投资%%'
            and t.当事人 not like '%%区建设%%'
            and t.当事人 not like '%%市投资%%'
            and t.当事人 not like '%%市建设%%'
            and t.当事人 not like '%%省金融%%'
            and t.当事人 not like '%%市金融%%'
            and t.当事人 not like '%%区金融%%'
            and t.当事人 not like '%%省资产%%'
            and t.当事人 not like '%%市资产%%'
            and t.当事人 not like '%%区资产%%'
            and t.当事人 not like '%%开发投资%%'
            and t.当事人 not like '%%能源投资%%'
            and t.当事人 not like '%%区管委会%%'
            and t.当事人 not like '%%区管理委员会%%'
            and t.当事人 not like '%%全国%%'
            and t.当事人 not like '%%国家%%'
            and t.当事人 not like '%%国资%%'
            and t.当事人 not like '%%集体%%'
            and t.当事人 not like '%%国营%%'
            and t.当事人 not like '%%国企%%'
            and t.当事人 not like '%%国务院%%'
            and t.当事人 not like '%%医院%%'
            and t.当事人 not like '%%大学%%'
            and t.当事人 not like '%%学院%%'
            and t.当事人 not like '%%中学%%'
            and t.当事人 not like '%%小学%%'
            and t.当事人 not like '%%中石油%%'
            and t.当事人 not like '%%中石化%%'
            and t.当事人 not like '%%中铁%%'
            and t.当事人 not like '%%国网%%'
            and t.当事人 not like '%%法院%%'
            and t.当事人 not like '%%华融资产%%'
            and t.当事人 not like '%%东方资产%%'
            and t.当事人 not like '%%信达资产%%'
            and t.当事人 not like '%%长城资产%%'
            and t.当事人 not like '%%市信用%%'
            and t.当事人 not like '%%市融资%%'
            and t.当事人 not like '%%信用合作%%'
            and t.当事人 not like '%%银行%%'
            and t.当事人 not like '%%分行%%'
            and t.当事人 not like '%%支行%%'
            and t.当事人 not like '%%信用社%%'
            and t.当事人 not like '%%储蓄所%%'
            and t.当事人 not like '%%律师%%'
            and t.当事人 not like '%%律所%%'
            and t.当事人 not like '%%保险%%'
            and t.当事人 not like '%%村民委员会%%'
            and t.当事人 not like '%%居民委员会%%'
            and t.当事人 not like '%%居委会%%'
            and t.当事人 not like '%%村委会%%'
            and t.当事人 not like '%%保障服务中心%%'
            and t.当事人 not like '%%国土%%'
            and t.当事人 not like '%%监狱%%'
            and t.当事人 not like '%%中国联合网络通信%%'
            and t.当事人 not like '%%中移铁通%%'
            and t.当事人 not like '%%登记中心%%'
            and t.当事人 not like '%%街道办事处%%'
            and t.当事人 not like '%%公积金%%'
            and t.当事人 not like '%%妇幼保健院%%'
            and t.当事人 not like '%%城市规划%%'
            and t.当事人 not like '%%药房%%'
            and t.当事人 not  like '%%事务所%%'
            and t.当事人 not  like '%%部队%%'
            and t.当事人 not  like '%%公证处%%'
            and t.当事人 not  like '%%淘宝%%'
            and t.当事人 not  like '%%法律服务所%%'
            and t.当事人 not like'%%集团%%'
            and t.当事人 not like '%%置业%%'
            and t.当事人 not like '%%拍卖%%'
            and t.当事人 not like '%%担保%%'
            and t.当事人 not like '%%资产管理%%'
            and t.当事人 not like '%%私募%%'
            and t.当事人 not like '%%公募%%'
            and t.当事人 not like '%%期货%%'
            and t.当事人 not like '%%商业管理%%'
            and t.当事人 not like '%%房产开发%%'
            and t.当事人 not like '%%房地产开发%%'
            and t.当事人 not like '%%物业%%'
            and t.当事人 not like '%%殡%%'
            and t.当事人 not like '%%保障服务%%'
            and t.当事人 not like '%%人力资源和社会保障%%'
            and t.当事人 not like '%%恒大%%'
            and t.当事人 not like '%%万科%%'
            and t.当事人 not like '%%执法%%'
            and t.当事人 not like '%%司法%%'
            and t.当事人 not like '%%财政%%'
            and t.当事人 not like '%%房地产管理%%'
            and t.当事人 not like '中国%%'
            and t.当事人 not like '中建%%'
            and t.当事人 not like '中交%%'
            and t.当事人 not like '%%局'
            and t.当事人 not like '%%委'


    """ % (search_tb_name, search_tb_name)

    cur.execute(get_data_cmd)
    conn.commit()
    print('获取待识别公司增量数据完毕！')


    #===============================
    # 20220304 准备追加一批公司的法人控股的其他公司，也做目标检测，后续如果一个公司 法人控股其他公司被剔除了，那么这个公司也会被剔除
    # 先生成一个当事人增量表，即当天需要检测的当事人
    # 然后插入进去，然后将这个表里面的公司的法人控制的其他公司插入到跑指标的任务表里面进行指标计算
    # 然后再生成一个表，这个表是 原始公司+法人控股公司  两列  为了是后续关联哪些原始公司他的法人名下其他公司在过滤的结果表中

    cmd0 = """
    truncate table 开庭公告当事人增量表_tmp
    """
    cur.execute(cmd0)
    conn.commit()
    print('开庭公告增量临时表清空完毕！')

    cmd1 = """
    insert into 开庭公告当事人增量表_tmp
    select distinct t.当事人
    from zxy.开庭公告_结构化@DBLINKE_192_89 t
    where length(t.当事人) > 4
     and (t.当事人类型 = '被告' or  t.当事人类型 = '被申请人' or t.当事人类型 = '被申诉人' or t.当事人类型 = '被上诉人'  or t.当事人类型 = '被执行人')
     and  t.开庭日期 >=substr(regexp_replace(to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss'),'[^.0-9]',''),0, 8)
     and not exists
   (select 1 from %s t1 where t.当事人 = t1.当事人)
   and not exists(select 1 from ktgg已过滤当事人 t2 where t.当事人 = t2.当事人)
                    and not exists (select 1
                  from ktgg被告国企标记 t3
                 where t.当事人 = t3.company
                   and t3.label = '国企')
                    and not exists
                (select 1 from 上市公司 t4 where t.当事人 = t4.公司名称)
    and not exists
    (select 1 from zxy.开庭公告排除案由_汇总 t5 where t.案由 = t5.案由)        
    and t.案由 not like '%%物业%%'
     and t.案由 not like '%%殡%%'
     and t.案由 not like '%%资本认购%%'
     and t.案由 not like '%%罪%%'
     and t.案由 not like '%%涉嫌%%'  
     and t.当事人 not like '%%培训%%'
     and t.当事人 not like '%%健身%%'            
            and t.当事人 not like '%%国有%%'
            and t.当事人 not like '%%政府%%'
            and t.当事人 not like '%%人民%%'
            and t.当事人 not like '%%改革%%'
            and t.当事人 not like '%%省投资%%'
            and t.当事人 not like '%%省建设%%'
            and t.当事人 not like '%%区投资%%'
            and t.当事人 not like '%%区建设%%'
            and t.当事人 not like '%%市投资%%'
            and t.当事人 not like '%%市建设%%'
            and t.当事人 not like '%%省金融%%'
            and t.当事人 not like '%%市金融%%'
            and t.当事人 not like '%%区金融%%'
            and t.当事人 not like '%%省资产%%'
            and t.当事人 not like '%%市资产%%'
            and t.当事人 not like '%%区资产%%'
            and t.当事人 not like '%%开发投资%%'
            and t.当事人 not like '%%能源投资%%'
            and t.当事人 not like '%%区管委会%%'
            and t.当事人 not like '%%区管理委员会%%'
            and t.当事人 not like '%%全国%%'
            and t.当事人 not like '%%国家%%'
            and t.当事人 not like '%%国资%%'
            and t.当事人 not like '%%集体%%'
            and t.当事人 not like '%%国营%%'
            and t.当事人 not like '%%国企%%'
            and t.当事人 not like '%%国务院%%'
            and t.当事人 not like '%%医院%%'
            and t.当事人 not like '%%大学%%'
            and t.当事人 not like '%%学院%%'
            and t.当事人 not like '%%中学%%'
            and t.当事人 not like '%%小学%%'
            and t.当事人 not like '%%中石油%%'
            and t.当事人 not like '%%中石化%%'
            and t.当事人 not like '%%中铁%%'
            and t.当事人 not like '%%国网%%'
            and t.当事人 not like '%%法院%%'
            and t.当事人 not like '%%华融资产%%'
            and t.当事人 not like '%%东方资产%%'
            and t.当事人 not like '%%信达资产%%'
            and t.当事人 not like '%%长城资产%%'
            and t.当事人 not like '%%市信用%%'
            and t.当事人 not like '%%市融资%%'
            and t.当事人 not like '%%信用合作%%'
            and t.当事人 not like '%%银行%%'
            and t.当事人 not like '%%分行%%'
            and t.当事人 not like '%%支行%%'
            and t.当事人 not like '%%信用社%%'
            and t.当事人 not like '%%储蓄所%%'
            and t.当事人 not like '%%律师%%'
            and t.当事人 not like '%%律所%%'
            and t.当事人 not like '%%保险%%'
            and t.当事人 not like '%%村民委员会%%'
            and t.当事人 not like '%%居民委员会%%'
            and t.当事人 not like '%%居委会%%'
            and t.当事人 not like '%%村委会%%'
            and t.当事人 not like '%%保障服务中心%%'
            and t.当事人 not like '%%国土%%'
            and t.当事人 not like '%%监狱%%'
            and t.当事人 not like '%%中国联合网络通信%%'
            and t.当事人 not like '%%中移铁通%%'
            and t.当事人 not like '%%登记中心%%'
            and t.当事人 not like '%%街道办事处%%'
            and t.当事人 not like '%%公积金%%'
            and t.当事人 not like '%%妇幼保健院%%'
            and t.当事人 not like '%%城市规划%%'
            and t.当事人 not like '%%药房%%'
            and t.当事人 not  like '%%事务所%%'
            and t.当事人 not  like '%%部队%%'
            and t.当事人 not  like '%%公证处%%'
            and t.当事人 not  like '%%淘宝%%'
            and t.当事人 not  like '%%法律服务所%%'
            and t.当事人 not like'%%集团%%'
            and t.当事人 not like '%%置业%%'
            and t.当事人 not like '%%拍卖%%'
            and t.当事人 not like '%%担保%%'
            and t.当事人 not like '%%资产管理%%'
            and t.当事人 not like '%%私募%%'
            and t.当事人 not like '%%公募%%'
            and t.当事人 not like '%%期货%%'
            and t.当事人 not like '%%商业管理%%'
            and t.当事人 not like '%%房产开发%%'
            and t.当事人 not like '%%房地产开发%%'
            and t.当事人 not like '%%物业%%'
            and t.当事人 not like '%%殡%%'
            and t.当事人 not like '%%保障服务%%'
            and t.当事人 not like '%%人力资源和社会保障%%'
            and t.当事人 not like '%%恒大%%'
            and t.当事人 not like '%%万科%%'
            and t.当事人 not like '%%执法%%'
            and t.当事人 not like '%%司法%%'
            and t.当事人 not like '%%财政%%'
            and t.当事人 not like '%%房地产管理%%'
            and t.当事人 not like '中国%%'
            and t.当事人 not like '中建%%'
            and t.当事人 not like '中交%%'
            and t.当事人 not like '%%局'
            and t.当事人 not like '%%委'
    
    """% (search_tb_name)
    cur.execute(cmd1)
    conn.commit()

    print('开庭公告临时表生成完毕！')

    # 选取一些公司，然后插入待跑指标的表中，然后准备跑指标，顺便跑一下公司的法人名下控股人
    cmd2 = """
    insert into %s select distinct  y.entname as 法人控股公司
    from (select b.cerno_18, b.entname
          from 开庭公告当事人增量表_tmp a
         inner join data_gsk_user.e_pri_person_md@dblinke_192_135 b
            on a.当事人 = b.entname
         where (b.法人代表 = '法人代表' or b.position_fy = '总经理')） x
    inner join data_gsk_user.e_inv_person_md@dblinke_192_135 y
    on x.cerno_18 = y.cerno_18
    where y.entname is not null
    and not exists (select 1 from %s t1 where y.entname = t1.当事人)
    """ %(search_tb_name, search_tb_name)

    cur.execute(cmd2)
    conn.commit()


    # 这个主要是为了后期回溯的，一旦指标表中有该名下公司，则根据与这个表进行关联，然后找出那些法人控股公司所在的起始关联公司，然后将该公司标记为过滤公司
    cmd3 = """
    insert into 法人名下公司 select distinct x.entname as 原始公司, y.entname as 法人控股公司
    from (select b.cerno_18, b.entname
          from 开庭公告当事人增量表_tmp a
         inner join data_gsk_user.e_pri_person_md@dblinke_192_135 b
            on a.当事人 = b.entname
         where (b.法人代表 = '法人代表' or b.position_fy = '总经理')) x
    inner join data_gsk_user.e_inv_person_md@dblinke_192_135 y
    on x.cerno_18 = y.cerno_18
    where y.entname is not null and not exists(select 1 from 法人名下公司 k where y.entname = k.原始公司)
    """
    cur.execute(cmd3)
    conn.commit()

    cur.close()
    conn.close()


# 根据国企识别结果以及上市公司结果对公司进行 国企和上市标记
def guoqi_shangshi_status_update():
    conn_orcl = connect_oracle_24()
    conn = conn_orcl[0]
    cur = conn_orcl[1]

    cmd1 = """
    update 债权人企业基本信息_ktgg t
    set t.国企标记 = '国企'
    where exists (select 1
          from ktgg被告国企标记 t1
         where t.公司名称 = t1.company
           and t1.label = '国企')
    """
    cur.execute(cmd1)
    conn.commit()
    print('国企标记完毕！')
    cmd2 = """
    update 债权人企业基本信息_ktgg t
    set t.上市标记 = '上市'
    where exists (select 1
          from zxy.上市公司 t1
         where t.公司名称 = t1.公司名称)
    """
    cur.execute(cmd2)
    conn.commit()
    print('上市公司标记完毕！')

    cur.close()
    conn.close()


# 最后进行过滤，并给出每个过滤的原因
def final_filter_result(final_out_put_company):
    conn_orcl = connect_oracle_24()
    conn = conn_orcl[0]
    cur = conn_orcl[1]

    # 将国企类的公司都写入结果表
    cmd1 = "insert into %s(当事人,reason,国企label,insert_date) select t.company,'国企',match_text,sysdate from  ktgg被告国企标记 t  " \
           "where t.label = '国企' and  not exists(select 1 from %s t1 where t.company = t1.当事人)" % (
               final_out_put_company, final_out_put_company)

    cur.execute(cmd1)
    conn.commit()
    print('国企过滤完毕！')

    # 将上市类公司写入结果表
    cmd2 = "insert into %s(当事人,reason,insert_date) select distinct t.当事人,'上市',sysdate from  %s t  " \
           "inner join zxy.上市公司 t1 on t.当事人 = t1.公司名称 where not exists(select 1 from %s t2 where t.当事人 = t2.当事人)" % (
               final_out_put_company, search_tb_name, final_out_put_company)

    cur.execute(cmd2)
    conn.commit()
    print('上市公司过滤完毕！')

    # 将经营指标异常写入结果表

    cmd3 = """
        insert into %s(当事人,reason,总营收,注册资本,社保人数,insert_date)
        select distinct t.公司名称, '经营指标过滤',t.总营收,t.注册资本,t.社保人数, sysdate
        from 债权人企业基本信息_ktgg t
        where t.总营收 >= 3000 or(t.注册资本 >= 5000 and (t.总营收 <= 10 or t.总营收>=1000) )or t.社保人数 >= 100
        and not exists
        (select 1 from %s t1 where t.公司名称 = t1.当事人)
        """ % (final_out_put_company, final_out_put_company)

    cur.execute(cmd3)
    conn.commit()
    print('企业经营指标过滤完毕！')

    cmd4 = """
            insert into %s(当事人,reason,经营状态,insert_date)
            select distinct t.公司名称, '已注销',t.经营状态, sysdate
            from 债权人企业基本信息_ktgg t
            where t.经营状态 like '%%销%%' and not exists
            (select 1 from %s t1 where t.公司名称 = t1.当事人)
            """ % (final_out_put_company, final_out_put_company)

    cur.execute(cmd4)
    conn.commit()
    print('注销企业过滤完毕！')

    cmd5 = """
            insert into %s(当事人,reason,开庭总个数,insert_date)
            select distinct t.公司名称, '开庭个数过滤',t.开庭总个数, sysdate
            from 债权人企业基本信息_ktgg t
            where t.开庭总个数 >= 5
            and not exists
            (select 1 from %s t1 where t.公司名称 = t1.当事人)
            """ % (final_out_put_company, final_out_put_company)

    cur.execute(cmd5)
    conn.commit()
    print('开庭个数过滤完毕！')

    cmd6 = """
                insert into %s(当事人,reason,失信总个数,insert_date)
                select distinct t.公司名称, '失信个数过滤',t.失信总个数, sysdate
                from 债权人企业基本信息_ktgg t
                where t.失信总个数 >= 0
                and not exists
                (select 1 from %s t1 where t.公司名称 = t1.当事人)
                """ % (final_out_put_company, final_out_put_company)

    cur.execute(cmd6)
    conn.commit()
    print('失信个数过滤完毕！')

    cmd7 = """
            insert into %s(当事人,reason,代理案件总量_近三年,insert_date)
            select distinct t.公司名称, '近三年律师代理过滤',t.代理案件总量_近三年, sysdate
            from 债权人企业基本信息_ktgg t
            where t.代理案件总量_近三年 >= 30
            and not exists
            (select 1 from %s t1 where t.公司名称 = t1.当事人)
            """ % (final_out_put_company, final_out_put_company)

    cur.execute(cmd7)
    conn.commit()
    print('近三年律师代理个数过滤完毕！')

    cmd8 = """
    insert into %s(当事人,reason,insert_date)
    select distinct t.当事人,'案由过滤',sysdate
    from zxy.开庭公告_结构化@DBLINKE_192_89 t
    where length(t.当事人) > 4 and (t.当事人类型 = '被告' or  t.当事人类型 = '被申请人' or t.当事人类型 = '被申诉人' or t.当事人类型 = '被上诉人'  or t.当事人类型 = '被执行人')
     and t.开庭日期 >=substr(regexp_replace(to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss'),'[^.0-9]',''),0, 8)
     and (t.案由 like '%%物业%%'
     or t.案由 like '%%殡%%'
     or t.案由 like '%%资本认购%%'
     or t.案由 like '%%罪%%'
     or t.案由 like '%%涉嫌%%')
     and  exists
    (select 1 from zxy.开庭公告排除案由_汇总 t1 where t.案由 = t1.案由)
    and not exists (select 1 from %s t2 where t.当事人 = t2.当事人)
    """ % (final_out_put_company, final_out_put_company)
    cur.execute(cmd8)
    conn.commit()
    print('案由过滤完毕！')

    cmd9 = """
    insert into %s(当事人,reason,insert_date)
    select distinct t.当事人,'关键字过滤',sysdate
    from zxy.开庭公告_结构化@DBLINKE_192_89 t
    where length(t.当事人) > 4 and (t.当事人类型 = '被告' or  t.当事人类型 = '被申请人' or t.当事人类型 = '被申诉人' or t.当事人类型 = '被上诉人'  or t.当事人类型 = '被执行人')
     and t.开庭日期 >=substr(regexp_replace(to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss'),'[^.0-9]',''),0, 8)
     and (t.当事人 like '%%物业%%'
     or t.当事人  like '%%培训%%'
     or t.当事人  like '%%健身%%'
     or t.当事人 like '%%国有%%'
        or t.当事人 like '%%政府%%'
        or t.当事人 like '%%人民%%'
        or t.当事人 like '%%改革%%'
        or t.当事人 like '%%省投资%%'
        or t.当事人 like '%%省建设%%'
        or t.当事人 like '%%区投资%%'
        or t.当事人 like '%%区建设%%'
        or t.当事人 like '%%市投资%%'
        or t.当事人 like '%%市建设%%'
        or t.当事人 like '%%省金融%%'
        or t.当事人 like '%%市金融%%'
        or t.当事人 like '%%区金融%%'
        or t.当事人 like '%%省资产%%'
        or t.当事人 like '%%市资产%%'
        or t.当事人 like '%%区资产%%'
        or t.当事人 like '%%开发投资%%'
        or t.当事人 like '%%能源投资%%'
        or t.当事人 like '%%区管委会%%'
        or t.当事人 like '%%区管理委员会%%'
        or t.当事人 like '%%全国%%'
        or t.当事人 like '%%国家%%'
        or t.当事人 like '%%国资%%'
        or t.当事人 like '%%集体%%'
        or t.当事人 like '%%国营%%'
        or t.当事人 like '%%国企%%'
        or t.当事人 like '%%国务院%%'
        or t.当事人 like '%%医院%%'
        or t.当事人 like '%%大学%%'
        or t.当事人 like '%%学院%%'
        or t.当事人 like '%%中学%%'
        or t.当事人 like '%%小学%%'
        or t.当事人 like '%%中石油%%'
        or t.当事人 like '%%中石化%%'
        or t.当事人 like '%%中铁%%'
        or t.当事人 like '%%国网%%'
        or t.当事人 like '%%法院%%'
        or t.当事人 like '%%华融资产%%'
        or t.当事人 like '%%东方资产%%'
        or t.当事人 like '%%信达资产%%'
        or t.当事人 like '%%长城资产%%'
        or t.当事人 like '%%市信用%%'
        or t.当事人 like '%%市融资%%'
        or t.当事人 like '%%信用合作%%'
        or t.当事人 like '%%银行%%'
        or t.当事人 like '%%分行%%'
        or t.当事人 like '%%支行%%'
        or t.当事人 like '%%信用社%%'
        or t.当事人 like '%%储蓄所%%'
        or t.当事人 like '%%律师%%'
        or t.当事人 like '%%律所%%'
        or t.当事人 like '%%保险%%'
        or t.当事人 like '%%村民委员会%%'
        or t.当事人 like '%%居民委员会%%'
        or t.当事人 like '%%居委会%%'
        or t.当事人 like '%%村委会%%'
        or t.当事人 like '%%保障服务中心%%'
        or t.当事人 like '%%国土%%'
        or t.当事人 like '%%监狱%%'
        or t.当事人 like '%%中国联合网络通信%%'
        or t.当事人 like '%%中移铁通%%'
        or t.当事人 like '%%登记中心%%'
        or t.当事人 like '%%街道办事处%%'
        or t.当事人 like '%%公积金%%'
        or t.当事人 like '%%妇幼保健院%%'
        or t.当事人 like '%%城市规划%%'
        or t.当事人 like '%%药房%%'
        or t.当事人 like '%%事务所%%'
        or t.当事人 like '%%部队%%'
        or t.当事人 like '%%公证处%%'
        or t.当事人 like '%%淘宝%%'
        or t.当事人 like '%%法律服务所%%'
        or t.当事人 like '%%集团%%'
        or t.当事人 like '%%置业%%'
        or t.当事人 like '%%拍卖%%'
        or t.当事人 like '%%担保%%'
        or t.当事人 like '%%资产管理%%'
        or t.当事人 like '%%私募%%'
        or t.当事人 like '%%公募%%'
        or t.当事人 like '%%期货%%'
        or t.当事人 like '%%商业管理%%'
        or t.当事人 like '%%房产开发%%'
        or t.当事人 like '%%房地产开发%%'
        or t.当事人 like '%%物业%%'
        or t.当事人 like '%%殡%%'
        or t.当事人 like '%%保障服务%%'
        or t.当事人 like '%%人力资源和社会保障%%'
        or t.当事人 like '%%恒大%%'
        or t.当事人 like '%%万科%%'
        or t.当事人 like '%%执法%%'
        or t.当事人 like '%%司法%%'
        or t.当事人 like '%%财政%%'
        or t.当事人 like '%%房地产管理%%'
        or t.当事人 like '中国%%'
        or t.当事人 like '中建%%'
        or t.当事人 like '中交%%'
        or t.当事人 like '%%局'
        or t.当事人 like '%%委' )
     and not exists (select 1 from %s t2 where t.当事人 = t2.当事人)

    """ % (final_out_put_company, final_out_put_company)
    cur.execute(cmd9)
    conn.commit()
    print('关键字过滤完毕！')

    cmd10 = """
    insert into %s(当事人,reason,insert_date)
    select distinct t.当事人,'高院过滤',sysdate
    from zxy.开庭公告_结构化@DBLINKE_192_89 t
    where length(t.当事人) > 4 and (t.当事人类型 = '被告' or  t.当事人类型 = '被申请人' or t.当事人类型 = '被申诉人' or t.当事人类型 = '被上诉人'  or t.当事人类型 = '被执行人')
     and t.开庭日期 >=substr(regexp_replace(to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss'),'[^.0-9]',''),0, 8) and t.审理法院 like '%%高级%%'
     and not exists (select 1 from %s t2 where t.当事人 = t2.当事人)
    """ % (final_out_put_company, final_out_put_company)
    cur.execute(cmd10)
    conn.commit()
    print('高院审理过滤完毕！')

    cmd11 = """
        insert into %s(当事人,reason,insert_date)
        select distinct t.当事人,'中院过滤',sysdate
        from zxy.开庭公告_结构化@DBLINKE_192_89 t
        where length(t.当事人) > 4 and (t.当事人类型 = '被告' or  t.当事人类型 = '被申请人' or t.当事人类型 = '被申诉人' or t.当事人类型 = '被上诉人'  or t.当事人类型 = '被执行人')
         and t.开庭日期 >=substr(regexp_replace(to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss'),'[^.0-9]',''),0, 8) and t.审理法院 like '%%中级%%' and t.案由 not like '%%知识产权%%'
         and t.案由 not like '%%著作权%%'
         and not exists (select 1 from %s t2 where t.当事人 = t2.当事人)
        """ % (final_out_put_company, final_out_put_company)
    cur.execute(cmd11)
    conn.commit()
    print('中院审理过滤完毕！')

    # 还差公司类型  百度失信 文书个数过滤

    cmd12 = """
                insert into %s(当事人,reason,insert_date)
                select distinct t.公司名称, '公司类型过滤', sysdate
                from 债权人企业基本信息_ktgg t
                where (t.企业类型 like '%%国%%'
                or t.企业类型 like '%%(上市%%'
                or t.企业类型 like '%%集体%%'
                or t.企业类型 like '%%港%%'
                or t.企业类型 like '%%澳%%'
                or t.企业类型 like '%%台%%'
                or t.企业类型 like '%%合资%%'
                or t.企业类型 like '%%全民%%'
                or t.企业类型 like '%%事业%%')
                and not exists
                (select 1 from %s t1 where t.公司名称 = t1.当事人)
                """ % (final_out_put_company, final_out_put_company)

    cur.execute(cmd12)
    conn.commit()
    print('公司类型过滤完毕！')

    cmd13 = """
                insert into %s(当事人,reason,insert_date)
                select distinct t.公司名称, '失信和涉案过滤', sysdate
                from 债权人企业基本信息_ktgg t
                where (t.百度失信个数>0 or t.执行公开失信个数>0 or t.裁判文书个数>0)
                and not exists
                (select 1 from %s t1 where t.公司名称 = t1.当事人)
                """ % (final_out_put_company, final_out_put_company)

    cur.execute(cmd13)
    conn.commit()
    print('百度失信+涉案数量过滤完毕！')

    cmd14 = """
                    insert into %s(当事人,reason,insert_date)
                    select distinct t.公司名称, '知名企业', sysdate
                    from 债权人企业基本信息_ktgg t
                    where length(t.知名企业)>0
                    and not exists
                    (select 1 from %s t1 where t.公司名称 = t1.当事人)
                    """ % (final_out_put_company, final_out_put_company)

    cur.execute(cmd14)
    conn.commit()
    print('知名企业过滤完毕！')

    cmd15 = """
                    insert into %s(当事人,reason,insert_date)
                    select distinct t.公司名称, '名下公司过多', sysdate
                    from 债权人企业基本信息_ktgg t
                    where t.股东名下公司个数>=10
                    and not exists
                    (select 1 from %s t1 where t.公司名称 = t1.当事人)
                    """ % (final_out_put_company, final_out_put_company)

    cur.execute(cmd15)
    conn.commit()
    print('实控人名下企业过多过滤完毕！')


    cmd16 = """
            insert into %s(当事人,reason,insert_date)
            select distinct t.公司名称, '公司为当事人', sysdate
            from 债权人企业基本信息_ktgg t
            where t.裁判文书当事人=1
            and not exists
            (select 1 from %s t1 where t.公司名称 = t1.当事人)
            """ % (final_out_put_company, final_out_put_company)

    cur.execute(cmd16)
    conn.commit()
    print('公司作为当事人出现在文书中过滤完毕！')

    cmd17 = """
                insert into %s(当事人,reason,insert_date)
                select distinct t.公司名称, '发票显示公司付过律师费', sysdate
                from 债权人企业基本信息_ktgg t
                where t.付过律师费=1
                and not exists
                (select 1 from %s t1 where t.公司名称 = t1.当事人)
                """ % (final_out_put_company, final_out_put_company)

    cur.execute(cmd17)
    conn.commit()
    print('公司付过律师费过滤完毕！')


    cmd18 = """
    insert into %s(当事人,reason,insert_date)
                select distinct t.法人控股公司, '法人控股公司关键字过滤', sysdate
                from 法人名下公司 t
                where (t.法人控股公司 like '%%物业%%'
     or t.法人控股公司  like '%%培训%%'
     or t.法人控股公司  like '%%健身%%' 
     or t.法人控股公司 like '%%国有%%'
        or t.法人控股公司 like '%%政府%%'
        or t.法人控股公司 like '%%人民%%'
        or t.法人控股公司 like '%%改革%%'
        or t.法人控股公司 like '%%省投资%%'
        or t.法人控股公司 like '%%省建设%%'
        or t.法人控股公司 like '%%区投资%%'
        or t.法人控股公司 like '%%区建设%%'
        or t.法人控股公司 like '%%市投资%%'
        or t.法人控股公司 like '%%市建设%%'
        or t.法人控股公司 like '%%省金融%%'
        or t.法人控股公司 like '%%市金融%%'
        or t.法人控股公司 like '%%区金融%%'
        or t.法人控股公司 like '%%省资产%%'
        or t.法人控股公司 like '%%市资产%%'
        or t.法人控股公司 like '%%区资产%%'
        or t.法人控股公司 like '%%开发投资%%'
        or t.法人控股公司 like '%%能源投资%%'
        or t.法人控股公司 like '%%区管委会%%'
        or t.法人控股公司 like '%%区管理委员会%%'
        or t.法人控股公司 like '%%全国%%'
        or t.法人控股公司 like '%%国家%%'
        or t.法人控股公司 like '%%国资%%'
        or t.法人控股公司 like '%%集体%%'
        or t.法人控股公司 like '%%国营%%'
        or t.法人控股公司 like '%%国企%%'
        or t.法人控股公司 like '%%国务院%%'
        or t.法人控股公司 like '%%医院%%'
        or t.法人控股公司 like '%%大学%%'
        or t.法人控股公司 like '%%学院%%'
        or t.法人控股公司 like '%%中学%%'
        or t.法人控股公司 like '%%小学%%'
        or t.法人控股公司 like '%%中石油%%'
        or t.法人控股公司 like '%%中石化%%'
        or t.法人控股公司 like '%%中铁%%'
        or t.法人控股公司 like '%%国网%%'
        or t.法人控股公司 like '%%法院%%'
        or t.法人控股公司 like '%%华融资产%%'
        or t.法人控股公司 like '%%东方资产%%'
        or t.法人控股公司 like '%%信达资产%%'
        or t.法人控股公司 like '%%长城资产%%'
        or t.法人控股公司 like '%%市信用%%'
        or t.法人控股公司 like '%%市融资%%'
        or t.法人控股公司 like '%%信用合作%%'
        or t.法人控股公司 like '%%银行%%'
        or t.法人控股公司 like '%%分行%%'
        or t.法人控股公司 like '%%支行%%'
        or t.法人控股公司 like '%%信用社%%'
        or t.法人控股公司 like '%%储蓄所%%'
        or t.法人控股公司 like '%%律师%%'
        or t.法人控股公司 like '%%律所%%'
        or t.法人控股公司 like '%%保险%%'
        or t.法人控股公司 like '%%村民委员会%%'
        or t.法人控股公司 like '%%居民委员会%%'
        or t.法人控股公司 like '%%居委会%%'
        or t.法人控股公司 like '%%村委会%%'
        or t.法人控股公司 like '%%保障服务中心%%'
        or t.法人控股公司 like '%%国土%%'
        or t.法人控股公司 like '%%监狱%%'
        or t.法人控股公司 like '%%中国联合网络通信%%'
        or t.法人控股公司 like '%%中移铁通%%'
        or t.法人控股公司 like '%%登记中心%%'
        or t.法人控股公司 like '%%街道办事处%%'
        or t.法人控股公司 like '%%公积金%%'
        or t.法人控股公司 like '%%妇幼保健院%%'
        or t.法人控股公司 like '%%城市规划%%'
        or t.法人控股公司 like '%%药房%%'
        or t.法人控股公司 like '%%事务所%%'
        or t.法人控股公司 like '%%部队%%'
        or t.法人控股公司 like '%%公证处%%'
        or t.法人控股公司 like '%%淘宝%%'
        or t.法人控股公司 like '%%法律服务所%%'
        or t.法人控股公司 like '%%集团%%'
        or t.法人控股公司 like '%%置业%%'
        or t.法人控股公司 like '%%拍卖%%'
        or t.法人控股公司 like '%%担保%%'
        or t.法人控股公司 like '%%资产管理%%'
        or t.法人控股公司 like '%%私募%%'
        or t.法人控股公司 like '%%公募%%'
        or t.法人控股公司 like '%%期货%%'
        or t.法人控股公司 like '%%商业管理%%'
        or t.法人控股公司 like '%%房产开发%%'
        or t.法人控股公司 like '%%房地产开发%%'
        or t.法人控股公司 like '%%物业%%'
        or t.法人控股公司 like '%%殡%%'
        or t.法人控股公司 like '%%保障服务%%'
        or t.法人控股公司 like '%%人力资源和社会保障%%'
        or t.法人控股公司 like '%%恒大%%'
        or t.法人控股公司 like '%%万科%%'
        or t.法人控股公司 like '%%执法%%'
        or t.法人控股公司 like '%%司法%%'
        or t.法人控股公司 like '%%财政%%'
        or t.法人控股公司 like '%%房地产管理%%'
        or t.法人控股公司 like '中国%%'
        or t.法人控股公司 like '中建%%'
        or t.法人控股公司 like '中交%%'
        or t.法人控股公司 like '%%局'
        or t.法人控股公司 like '%%委')
                and not exists
                (select 1 from %s t1 where t.法人控股公司 = t1.当事人)
    """ % (final_out_put_company, final_out_put_company)

    cur.execute(cmd18)
    conn.commit()
    print('法人控股公司关键字过滤完毕！')


    cmd19 = """
    insert into %s(当事人,reason,insert_date)
                select distinct t.原始公司, '法人控股公司指标异常', sysdate
                from 法人名下公司 t inner join %s t1 on t.法人控股公司 = t1.当事人
             and not exists
                (select 1 from %s t2 where t.原始公司 = t2.当事人)   
    
    """%(final_out_put_company, final_out_put_company,final_out_put_company)
    cur.execute(cmd19)
    conn.commit()
    print('法人控股公司指标异常过滤完毕！')

    cur.close()
    conn.close()


def bulk_into_mysql(connect, cursor, sql_cmd, para):
    try:
        cursor.executemany(sql_cmd, para)
        connect.commit()
    except Exception as e:
        print(e, sql_cmd)
        connect.rollback()


# 这里将公司中有分公司的拆开 分为总公司和分公司
def company_clean(company):
    line = ''.join(re.findall('[a-zA-Z()（）一-龥]+', company)).replace('（', '(').replace('）', ')')
    company_list = []
    if '分公司' in line:
        cut_index = line.find('公司')
        new_line = line[:cut_index + 2]
        if len(line) > 4:
            if ('(' in line and ')' not in line):
                return []
            if (')' in line and '(' not in line):
                return []
            company_list.append(line)
            company_list.append(new_line)
    else:

        if len(line) > 4:
            if ('(' in line and ')' not in line):
                return []
            if (')' in line and '(' not in line):
                return []
            company_list.append(line)
    return company_list


# 新增的公司送到爬虫任务表中
def increased_company_to_spider():
    conn_getdata = connect_mysql_getdata_89('HLB_5')
    conn = conn_getdata[0]
    cur = conn_getdata[1]
    cmd = 'select company from KTGG_DCSXGS'
    cur.execute(cmd)
    exist_company = set([i[0] for i in cur])

    connect_25 = connect_oracle_25()  # 对应的是开庭公告的表
    conn25 = connect_25[0]
    cur25 = connect_25[1]

    cmd1 = """
    select distinct t.当事人
    from zxy.开庭公告_结构化@DBLINKE_192_89 t
    where length(t.当事人) > 4
     and (t.当事人类型 = '被告' or  t.当事人类型 = '被申请人' or t.当事人类型 = '被申诉人' or t.当事人类型 = '被上诉人'  or t.当事人类型 = '被执行人')
     and  t.开庭日期 >=substr(regexp_replace(to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss'),'[^.0-9]',''),0, 8)
   and not exists(select 1 from ktgg已过滤当事人 t2 where t.当事人 = t2.当事人)

                    and not exists (select 1
                  from ktgg被告国企标记 t3
                 where t.当事人 = t3.company
                   and t3.label = '国企')
                    and not exists
                (select 1 from 上市公司 t4 where t.当事人 = t4.公司名称)
    and not exists
    (select 1 from zxy.开庭公告排除案由_汇总 t5 where t.案由 = t5.案由)        
    and t.案由 not like '%%物业%%'
     and t.案由 not like '%%殡%%'
     and t.案由 not like '%%资本认购%%'
     and t.案由 not like '%%罪%%'
     and t.案由 not like '%%涉嫌%%' 
     and t.当事人 not like '%%培训%%'
     and t.当事人 not like '%%健身%%'             
            and t.当事人 not like '%%国有%%'
            and t.当事人 not like '%%政府%%'
            and t.当事人 not like '%%人民%%'
            and t.当事人 not like '%%改革%%'
            and t.当事人 not like '%%省投资%%'
            and t.当事人 not like '%%省建设%%'
            and t.当事人 not like '%%区投资%%'
            and t.当事人 not like '%%区建设%%'
            and t.当事人 not like '%%市投资%%'
            and t.当事人 not like '%%市建设%%'
            and t.当事人 not like '%%省金融%%'
            and t.当事人 not like '%%市金融%%'
            and t.当事人 not like '%%区金融%%'
            and t.当事人 not like '%%省资产%%'
            and t.当事人 not like '%%市资产%%'
            and t.当事人 not like '%%区资产%%'
            and t.当事人 not like '%%开发投资%%'
            and t.当事人 not like '%%能源投资%%'
            and t.当事人 not like '%%区管委会%%'
            and t.当事人 not like '%%区管理委员会%%'
            and t.当事人 not like '%%全国%%'
            and t.当事人 not like '%%国家%%'
            and t.当事人 not like '%%国资%%'
            and t.当事人 not like '%%集体%%'
            and t.当事人 not like '%%国营%%'
            and t.当事人 not like '%%国企%%'
            and t.当事人 not like '%%国务院%%'
            and t.当事人 not like '%%医院%%'
            and t.当事人 not like '%%大学%%'
            and t.当事人 not like '%%学院%%'
            and t.当事人 not like '%%中学%%'
            and t.当事人 not like '%%小学%%'
            and t.当事人 not like '%%中石油%%'
            and t.当事人 not like '%%中石化%%'
            and t.当事人 not like '%%中铁%%'
            and t.当事人 not like '%%国网%%'
            and t.当事人 not like '%%法院%%'
            and t.当事人 not like '%%华融资产%%'
            and t.当事人 not like '%%东方资产%%'
            and t.当事人 not like '%%信达资产%%'
            and t.当事人 not like '%%长城资产%%'
            and t.当事人 not like '%%市信用%%'
            and t.当事人 not like '%%市融资%%'
            and t.当事人 not like '%%信用合作%%'
            and t.当事人 not like '%%银行%%'
            and t.当事人 not like '%%分行%%'
            and t.当事人 not like '%%支行%%'
            and t.当事人 not like '%%信用社%%'
            and t.当事人 not like '%%储蓄所%%'
            and t.当事人 not like '%%律师%%'
            and t.当事人 not like '%%律所%%'
            and t.当事人 not like '%%保险%%'
            and t.当事人 not like '%%村民委员会%%'
            and t.当事人 not like '%%居民委员会%%'
            and t.当事人 not like '%%居委会%%'
            and t.当事人 not like '%%村委会%%'
            and t.当事人 not like '%%保障服务中心%%'
            and t.当事人 not like '%%国土%%'
            and t.当事人 not like '%%监狱%%'
            and t.当事人 not like '%%中国联合网络通信%%'
            and t.当事人 not like '%%中移铁通%%'
            and t.当事人 not like '%%登记中心%%'
            and t.当事人 not like '%%街道办事处%%'
            and t.当事人 not like '%%公积金%%'
            and t.当事人 not like '%%妇幼保健院%%'
            and t.当事人 not like '%%城市规划%%'
            and t.当事人 not like '%%药房%%'
            and t.当事人 not  like '%%事务所%%'
            and t.当事人 not  like '%%部队%%'
            and t.当事人 not  like '%%公证处%%'
            and t.当事人 not  like '%%淘宝%%'
            and t.当事人 not  like '%%法律服务所%%'
            and t.当事人 not like'%%集团%%'
            and t.当事人 not like '%%置业%%'
            and t.当事人 not like '%%拍卖%%'
            and t.当事人 not like '%%担保%%'
            and t.当事人 not like '%%资产管理%%'
            and t.当事人 not like '%%私募%%'
            and t.当事人 not like '%%公募%%'
            and t.当事人 not like '%%期货%%'
            and t.当事人 not like '%%商业管理%%'
            and t.当事人 not like '%%房产开发%%'
            and t.当事人 not like '%%房地产开发%%'
            and t.当事人 not like '%%物业%%'
            and t.当事人 not like '%%殡%%'
            and t.当事人 not like '%%保障服务%%'
            and t.当事人 not like '%%人力资源和社会保障%%'
            and t.当事人 not like '%%恒大%%'
            and t.当事人 not like '%%万科%%'
            and t.当事人 not like '%%执法%%'
            and t.当事人 not like '%%司法%%'
            and t.当事人 not like '%%财政%%'
            and t.当事人 not like '%%房地产管理%%'
            and t.当事人 not like '中国%%'
            and t.当事人 not like '中建%%'
            and t.当事人 not like '中交%%'
            and t.当事人 not like '%%局'
            and t.当事人 not like '%%委'


    """
    cur25.execute(cmd1)

    for info in cur25:
        company = info[0]
        company_list = company_clean(company)
        for company_new in company_list:
            if company not in exist_company:  # 如果给定的公司不在爬虫列表中
                para = (company_new)
                para_list = []
                para_list.append(para)
                sql_cmd = 'INSERT INTO' + ' ' + 'HLB_5.`KTGG_DCSXGS`(company)' + 'VALUES(%s)'

                # 每一条都采用bulk_insert的方式插入，如果采用单纯插入的方式有可能格式不正确导致插入失败
                bulk_into_mysql(conn, cur, sql_cmd, para_list)
    print('开庭公告新增被告插入百度失信爬虫完毕！')
    cur.close()
    conn.close()
    cur25.close()
    conn25.close()


# 新增的公司法人名下公司全部送到爬虫任务表中
def increased_company_to_spider1():
    conn_getdata = connect_mysql_getdata_89('HLB_5')
    conn = conn_getdata[0]
    cur = conn_getdata[1]
    cmd = 'select company from KTGG_DCSXGS'
    cur.execute(cmd)
    exist_company = set([i[0] for i in cur])

    connect_25 = connect_oracle_25()  # 对应的是开庭公告的表
    conn25 = connect_25[0]
    cur25 = connect_25[1]

    cmd = """
        select distinct  y.entname as 法人控股公司
        from (select b.cerno_18, b.entname
              from 开庭公告当事人增量表_tmp a
             inner join data_gsk_user.e_pri_person_md@dblinke_192_135 b
                on a.当事人 = b.entname
             where (b.法人代表 = '法人代表' or b.position_fy = '总经理')） x
        inner join data_gsk_user.e_inv_person_md@dblinke_192_135 y
        on x.cerno_18 = y.cerno_18
        where y.entname is not null
        """

    cur25.execute(cmd)
    for info in cur25:
        company = info[0]
        company_list = company_clean(company)
        for company_new in company_list:
            if company not in exist_company:  # 如果给定的公司不在爬虫列表中
                para = (company_new)
                para_list = []
                para_list.append(para)
                sql_cmd = 'INSERT INTO' + ' ' + 'HLB_5.`KTGG_DCSXGS`(company)' + 'VALUES(%s)'

                # 每一条都采用bulk_insert的方式插入，如果采用单纯插入的方式有可能格式不正确导致插入失败
                bulk_into_mysql(conn, cur, sql_cmd, para_list)
    print('公司法人名下公司插入百度失信爬虫完毕！')
    cur.close()
    conn.close()
    cur25.close()
    conn25.close()





if __name__ == '__main__':
    start = datetime.datetime.now()
    # search_tb_name = '开庭公告当事人增量表'
    # get_increased_company(search_tb_name)  # 获取到增量的开庭公告当事人
    filtered_company_name = 'ktgg已过滤当事人'
    # # 国企关联 结果表为 ktgg被告国企标记
    # master_company_sign(search_tb_name)
    # # 将新增公司添加到失信和涉案爬虫中（新增公司的股东名下其他公司 是不会加入的）
    # increased_company_to_spider()
    # # 将新增公司的股东名下其他公司加入到失信涉案爬虫中
    # increased_company_to_spider1()
    # # 获取相关数据指标
    # get_relate_data(search_tb_name)
    # 将最后过滤结果都写上相关过滤原因
    final_filter_result(filtered_company_name)
    end = datetime.datetime.now()
    print('耗时:', end - start)

    # while True:
    #     if str(datetime.datetime.now().hour)==4:
    #         start = datetime.datetime.now()
    #         search_tb_name = '开庭公告当事人增量表'
    #         get_increased_company(search_tb_name)  # 获取到增量的开庭公告当事人
    #         filtered_company_name = 'ktgg已过滤当事人'
    #         # 国企关联 结果表为 ktgg被告国企标记
    #         master_company_sign(search_tb_name)
    #         # 将新增公司添加到失信和涉案爬虫中
    #         increased_company_to_spider()
    #         # 获取相关数据指标
    #         get_relate_data(search_tb_name)
    #         # 将最后过滤结果都写上相关过滤原因
    #         final_filter_result(filtered_company_name)
    #         end = datetime.datetime.now()
    #         print('耗时:', end - start)
    #     time.sleep(60)
    #     print('正在检测任务开始时间！')




