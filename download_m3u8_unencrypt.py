

import requests
import os
import threading
from tqdm import tqdm
import time
import random
from Crypto.Cipher import AES

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:84.0) Gecko/20100101 Firefox/84.0'}

def download(url,file_path):

    while True:
        try:
            r = requests.get(url, headers=headers)
            with open(file_path, "wb") as f:
                f.write(r.content)
            return
        except Exception as e:
            print(f"{url}获取数据失败:{e}")
            time.sleep(random.random())


def get_time_str():
    now = time.time()
    timeArray = time.localtime(now)
    return time.strftime("%Y%m%d_%H%M%S", timeArray)

def main(m3u8_url, file_name, root_path):
    target_file = os.path.join(root_path, file_name)
    print(f"\n{target_file}开始下载...")
    if os.path.exists(target_file):
        os.system(f"rm -rf {target_file}")
    # 获取m3u8文件信息
    r = requests.get(m3u8_url, headers=headers).content
    m3u8_info = str(r, encoding="gbk").split("\n")
    # ts文件列表
    ts_url_list = [m3u8_url.replace("index.m3u8", i) for i in m3u8_info if i.endswith("ts")]
    # 临时文件保存
    time_str = get_time_str()
    tmp_dir = os.path.join(root_path,time_str)
    if os.path.exists(tmp_dir):
        os.system(f"rm -rf {tmp_dir}/*")
    else:
        os.makedirs(tmp_dir)
    file_list = [os.path.join(tmp_dir, f"{i}.ts") for i in range(len(ts_url_list))]

    t_list = []
    for url, file_path in zip(ts_url_list, file_list):

        t = threading.Thread(target=download, args=(url, file_path))
        t_list.append(t)
        t.start()
    for t in t_list:
        t.join()

    # 文件合并
    with open(target_file, "ab") as f:
        for file_path in file_list:
            with open(file_path, "rb") as tmp:
                data = tmp.read()
            f.write(data)

    os.system(f"rm -rf {tmp_dir}")
    print(f"{target_file}下载完成!")





if __name__ == '__main__':

    # m3u8_url = "https://cdn-bo3.mangguo-youku.com:5278/20211017/c37UglSs/10000kb/hls/key.key"
    #
    # # m3u8_url = input("m3u8 url:")
    # # file_name = input("file name:")
    # file_name = "一撸向西EP1.AV篇.赵一曼.诺米.男女通吃的3P四手按摩.mp4"
    # root_path = "/Users/tangyu/Desktop/"
    # main(m3u8_url, file_name, root_path)

    test()
