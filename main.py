import requests
import re
import json
import pymysql
from bs4 import BeautifulSoup
import time


# get请求
def get_request(url):
    headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
               'Accept-Encoding': 'gzip, deflate',
               'Accept-Language': 'zh-cn',
               'Cache-Control': 'max-age=0',
               'Connection': 'keep-alive',
               'User-Agent': 'xxxxx',
               'Content-Type': 'application/x-www-form-urlencoded',
               'Cookie': 'xxxxx'
               }
    wx_session = requests.Session()
    return wx_session.get(url, headers=headers, verify=False).content.decode()


# json请求
def json_request(url):
    headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
               'Accept-Encoding': 'gzip, deflate',
               'Accept-Language': 'zh-cn',
               'Cache-Control': 'max-age=0',
               'Connection': 'keep-alive',
               'User-Agent': 'xxxxx',
               'Content-Type': 'application/x-www-form-urlencoded',
               'Cookie': 'xxxxx'
               }
    wx_session = requests.Session()
    return wx_session.get(url, headers=headers, verify=False).content.decode()


# 获取消息列表数据
def MsgList(url):
    f = get_request(url)

    search_obj = re.search(r"var msgList(.*)';", f, re.M | re.I)
    if search_obj:
        msg_list = search_obj.group()
        msg_list = msg_list[(msg_list.index("'") + 1):-2]
        msg_list = msg_list.replace("&#39;", "'").replace("&quot;", '"').replace("&nbsp;", " ").replace("&gt;",
                                                                                                        ">").replace(
            "&lt;", "<").replace("&amp;", "&").replace("&yen;", "¥")
        msg_list = json.loads(json.dumps(json.loads(msg_list), indent=4, sort_keys=False, ensure_ascii=False))
        return msg_list["list"]
    else:
        return None


# 存入数据库-- book_list
def store_book_list(list):
    # 打开数据库连接
    db = pymysql.connect("localhost", "username", "password", "table_name", use_unicode=True, charset="utf8")
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    # SQL插入语句
    init_sql = "INSERT INTO `sd_book_list` (`title`, `content_url`, `source_url`, `status`, `datetime`, `fileid`) VALUES"
    for item in list:
        if "app_msg_ext_info" in item:
            app_msg_ext_info = item["app_msg_ext_info"]
            if "copyright_stat" in app_msg_ext_info and app_msg_ext_info["copyright_stat"] == 11:
                params = [
                    "'" + app_msg_ext_info["title"] + "'",
                    "'" + app_msg_ext_info["content_url"] + "'",
                    "'" + app_msg_ext_info["source_url"] + "'",
                    1,
                    item["comm_msg_info"]["datetime"],
                    app_msg_ext_info["fileid"]
                ]
                value = ",".join('%s' % index for index in params)
                sql = init_sql + '(' + value + ');'
                try:
                    # 执行sql语句
                    cursor.execute(sql)
                    db.commit()
                except Exception as err:
                    # 发生错误时回滚
                    db.rollback()
                    print(err)
                    # 关闭数据库连接
        else:
            print(item)
    cursor.close()
    db.close()


# 获取所有没有获取过详情的书单
def get_book_list():
    # 打开数据库连接
    db = pymysql.connect("localhost", "username", "password", "table_name", use_unicode=True, charset="utf8")
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    # SQL查询语句
    cursor.execute("SELECT id, content_url FROM `sd_book_list` WHERE status=1")
    # 获取所有结果行
    rows = cursor.fetchall()
    db.commit()
    cursor.close()
    db.close()
    return rows


# 获取书单详情
def list_detail(content_url):
    f = get_request(content_url)
    bs = BeautifulSoup(f, 'html.parser')
    all_p = bs.select('#js_content')[0].find_all('p')
    contents = []
    for p in all_p:
        content = p.get_text().strip()
        if content is not None and content != '\n' and content != '':
            contents.append(content)
    content_dict = []
    for i in range(len(contents)):
        item = contents[i]
        if item.startswith('豆瓣评分：'):
            print(item)
            dict_value = {
                'name': contents[i - 2],
                'author': contents[i - 1],
                'score': item[item.index('：') + 1:item.index('(')] if '(' in item else item[item.index('：') + 1:item.index('（')],
                'comment': (item[item.index('(') + 1:item.index('人')] if '人' in item else item[item.index('(') + 1:item.index(')')]) if '(' in item else item[item.index('（') + 1:item.index('人')]
            }
            content_dict.append(dict_value)
    return content_dict


# 保存书单详情
def store_detail(lid, content_dict):
    # 打开数据库连接
    db = pymysql.connect("localhost", "username", "password", "table_name", use_unicode=True, charset="utf8")
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    if content_dict is not None and len(content_dict) > 0:
        # SQL插入语句
        sql = "INSERT INTO `sd_book_detail` (`lid`, `name`, `author`, `score`, `comment`) VALUES "
        for item in content_dict:
            params = [
                lid,
                "'" + item["name"] + "'",
                "'" + item["author"] + "'",
                float(item["score"]) if item["score"] != '' else 0,
                int(item["comment"]) if item["score"] != '' else 0
            ]
            sql += '(' + ",".join('%s' % index for index in params) + '),'
        try:
            # 执行sql语句
            cursor.execute(sql[0:-1] + ';')
            db.commit()
            update_sql = "UPDATE `sd_book_list` SET `status`='2' WHERE `id`=" + str(lid) + ";"
            cursor.execute(update_sql)
            db.commit()
        except Exception as err:
            # 发生错误时回滚
            db.rollback()
            print(err)
            # 关闭数据库连接
    else:
        update_sql = "UPDATE `sd_book_list` SET `status`='2' WHERE `id`=" + str(lid) + ";"
        cursor.execute(update_sql)
        db.commit()

    cursor.close()
    db.close()


# 继续加载
def json_list():
    offset = 10
    condition = True
    i = 1
    while condition:
        json_url = "往下拉ajax加载数据的网址，每次都是offset变化而已"
        f = json_request(json_url)
        res = json.loads(f)
        store_book_list(json.loads(res['general_msg_list'])['list'])
        print(i)
        i += 1
        if res['can_msg_continue'] <= 0:
            condition = False
        else:
            offset += 10
            time.sleep(1)


if __name__ == '__main__':
    wx_url = "fiddler抓取到的打开历史消息的网址"
    content_list = MsgList(wx_url)
    if content_list is not None:
        store_book_list(content_list)
    json_list()
    list_rows = get_book_list()
    if list_rows is not None and len(list_rows) > 0:
        for item in list_rows:
            content_url = item[1].replace("&amp;", "&")
            result_dict = list_detail(content_url=content_url)
            store_detail(item[0], result_dict)
