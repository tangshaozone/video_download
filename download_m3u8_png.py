"""
m3u8加密破解:https://blog.csdn.net/baidu_41902768/article/details/85011094
index.m3n8中文件为png破解方法:https://blog.csdn.net/feiyu361/article/details/121196667
dd if=input.png of=output.ts bs=4 skip=30
"""
import sys
import requests
import os
import threading
import time
import random
from Crypto.Cipher import AES
from tqdm import tqdm
from UtilsLib import *

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36'}

def download_ts(args):
    url,file_path,cryptor,i = args
    try_n = 5
    n = 0
    while True:
        try:
            data = requests.get(url, headers=headers,verify=False).content
            mem_mb = sys.getsizeof(data)
            if mem_mb < 500:
                time.sleep(2 + random.random())
                continue
            with open(file_path, "wb") as f:
                if cryptor:
                    f.write(cryptor.decrypt(data))
                else:
                    f.write(data)
            return
        except Exception as e:
            if (str(e) != "Data must be padded to 16 byte boundary in CBC mode") and ("bad handshake" not in str(e)) and ("Too many open" not in str(e)):
                print(f"{url}获取数据失败:{e}")
            time.sleep(2 + random.random())
            if n <= try_n:
                n += 1
            else:
                return


def get_time_str():
    now = time.time()
    timeArray = time.localtime(now)
    return time.strftime("%Y%m%d_%H%M%S", timeArray)

def download(args):
    m3u8_url, file_name, root_path = args

    target_file = os.path.join(root_path, file_name.replace(" ",""))
    log_info(f"{target_file}开始下载...")
    if os.path.exists(target_file):
        os.system(f"rm -rf {target_file}")
    # 获取m3u8文件信息
    r = requests.get(m3u8_url, headers=headers,verify=False).content
    m3u8_info = str(r, encoding="gbk").split("\n")

    # 文件是否加密
    cryptor = None
    for i in m3u8_info:
        if "key.key" in i:
            key_url = m3u8_url.replace("index.m3u8","key.key")
            key = requests.get(key_url, headers=headers,verify=False).text.encode("utf-8")
            cryptor = AES.new(key, AES.MODE_CBC)
            break

    # ts文件列表 可能是jpg/png结尾
    # ts_url_list = [m3u8_url.replace("index.m3u8", i.split("/")[-1]) for i in m3u8_info if not i.startswith("#")]
    ts_url_list = [i for i in m3u8_info if not i.startswith("#")]
    # 临时文件保存
    time_str = get_time_str()
    tmp_dir = os.path.join(root_path,time_str)
    if os.path.exists(tmp_dir):
        os.system(f"rm -rf {tmp_dir}/*")
    else:
        os.makedirs(tmp_dir)
    file_list = [os.path.join(tmp_dir, f"{i}.ts") for i in range(len(ts_url_list))]

    # for i,(url, file_path) in tqdm(enumerate(zip(ts_url_list, file_list)),total=len(file_list)):
    #     download_ts((url, file_path, cryptor,i))

    t_list = []
    for i,(url, file_path) in enumerate(zip(ts_url_list, file_list)):
        t = threading.Thread(target=download_ts, args=((url, file_path, cryptor,i),))
        t_list.append(t)
        t.start()
    for t in t_list:
        t.join()

    # 文件合并
    log_info(f"{target_file}开始合成...")
    with open(target_file, "ab") as f:
        for file_path in file_list:
            try:
                with open(file_path, "rb") as tmp:
                    tmp.read(120)
                    data = tmp.read()
                f.write(data)
            except Exception as error:
                continue

    os.system(f"rm -rf {tmp_dir}")
    log_info(f"{target_file}下载完成!")


def test():
    # key_url = "https://cdn-bo3.mangguo-youku.com:5278/20211017/c37UglSs/10000kb/hls/key.key"
    # key = requests.get(key_url, headers=headers).text.encode("utf-8")
    # cryptor = AES.new(key,AES.MODE_CBC)
    # with open("/Users/tangyu/Desktop/test.ts","wb") as f:
    #     r = requests.get("https://cdn-bo3.mangguo-youku.com:5278/20211017/c37UglSs/10000kb/hls/KtNZJTCT.ts", headers=headers).content
    #     f.write(cryptor.decrypt(r))

    # with open("/Users/tangyu/Desktop/test.mp4","wb") as f:
    #     r = requests.get("https://ae04.alicdn.com/kf/Ub44c3f703e3c4520a6024c9fdcbe0957k.png",
    #                      headers=headers
    #                      ,verify=False).content
    #     f.write(r)

    # m3u8_url = "https://c2.monidai.com/20220128/9XrPbh8c/index.m3u8"
    # r = requests.get(m3u8_url, headers=headers,verify=False).content
    # m3u8_info = str(r, encoding="gbk").split("\n")
    # print(m3u8_info)

    file_list = [f"/Users/tangyu/Desktop/tonyzone/movies/20220303_202538/{i}.ts" for i in range(100)]
    with open("/Users/tangyu/Desktop/a.ts", "ab") as f:
        for file_path in file_list:
            with open(file_path, "rb") as tmp:
                tmp.read(120)
                data = tmp.read()
            f.write(data)

def main():
    m3u8_url = "https://cdn-bo3.mangguo-youku.com:5278/20211017/rX3EjZdB/10000kb/hls/index.m3u8"
    file_name = "换妻性爱治疗之旅 EP1.mp4"
    root_path = "/Users/tangyu/Desktop/movies"
    download((m3u8_url, file_name, root_path))

def batch_download():
    with open("params.txt", "r") as f:
        info = f.read()

    info_list = info.strip().split("\n")
    root_path = info_list[0].split("=")[1]
    args = []
    for i in info_list[1:]:
        if 1 - i.startswith("#"):
            i_0,i_1 = i.split(",")
            args.append((i_0,i_1 + ".mp4",root_path))

    for arg in tqdm(args,total=len(args)):
        # try:
            download(arg)
        # except Exception as e:
        #     log_error(f"{arg[1]}下载失败:{e}")

if __name__ == '__main__':
    # try:
        batch_download()
        # main()

    # except Exception as e:
    #     tonychat.log_error(f"视频下载失败:{e}")
    #     raise Exception(str(e))

    # test()

