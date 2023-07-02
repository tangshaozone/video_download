"""
m3u8加密破解:https://blog.csdn.net/baidu_41902768/article/details/85011094
index.m3n8中文件为png破解方法:https://blog.csdn.net/feiyu361/article/details/121196667
dd if=input.png of=output.ts bs=4 skip=30

f12 debug解除:https://www.jianshu.com/p/e6757f4919a6

https://www.mdrccbig.info/index.php/vodplay/436709-1-1.html
mp4文件合并
"""

"""
jable
https://jable.tv/videos/sdde-607/
https://amuse-lefty.mushroomtrack.com/hls/A5owugOgMGFkdGGtBCbLOQ/1675275658/30000/30165/30165.m3u8
key: https://amuse-lefty.mushroomtrack.com/hls/A5owugOgMGFkdGGtBCbLOQ/1675275658/30000/30165/5a15bfa177b646bc.ts
ts: https://amuse-lefty.mushroomtrack.com/hls/A5owugOgMGFkdGGtBCbLOQ/1675275658/30000/30165/301654.ts

openssl aes-128-cbc -d -nosalt -iv 00000000000000000000000000000000 -K 41F2DB330A7BB4A264F32894E313C8DE -in D:\miku\media-****_DVR_0.ts -out D:\miku_decode\media-****_DVR_0_decode.ts
"""

import requests
import random
from Crypto.Cipher import AES
from tqdm import tqdm
import time
from multiprocessing.dummy import Pool as ThreadPool
from moviepy.editor import *
import toml
import shutil
import logging
import urllib3
import traceback
import binascii


urllib3.disable_warnings()

logger = logging.getLogger(__name__)

LOG_FORMAT = "%(asctime)s[%(levelname)s]%(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36'}

# 配置文件参数
file_path = os.path.dirname(os.path.abspath(__file__))
param_file_path = os.path.join(file_path, "params.toml")
params = toml.load(param_file_path)


def ffmpeg_ts2mp4(ts_file_name: str, mp4_file_name: str):
    os.system(
        f"ffmpeg -i {ts_file_name} -acodec copy -vcodec copy -f mp4 {mp4_file_name}"
    )


def del_special_char(str_: str) -> str:
    """删除特殊字符."""
    for char_ in params["replace_char_list"]:
        str_ = str_.replace(char_, "")
    return str_


def download_ts(args):
    """单个ts文件下载."""
    url, file_path, cryptor, i = args
    try_n = 5
    n = 0
    while True:
        try:
            data = requests.get(url, headers=headers, verify=False).content
            mem_mb = sys.getsizeof(data)
            if mem_mb < 1024:
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
            time.sleep(3 + random.random())
            if n <= try_n:
                n += 1
            else:
                print(f"{url}下载失败!")
                return


class DownloatTsFilm:

    def __init__(self) -> None:
        """init."""

        self._root_path = params["root_path"]
        self._m3u8_url_file_name_list = params["downloat_ts_film"]["m3u8_url_file_name_list"]

    @staticmethod
    def _get_time_str():
        now = time.time()
        timeArray = time.localtime(now)
        return time.strftime("%Y%m%d_%H%M%S", timeArray)

    def _download(self, mp4_file_name: str, m3u8_url: str = "", m3u8_file_path: str = None):
        """下载ts文件转mp4.

        Args:
            m3u8_url: m3u8_url地址
            mp4_file_name: mp4文件名,mp4后缀可有可无.
            m3u8_file_name: m3u8文件绝对路径. Defaults to None.
        """
        mp4_file_name = del_special_char(mp4_file_name)
        if mp4_file_name.endswith(".mp4") is False:
            mp4_file_name = f"{mp4_file_name}.mp4"
        logger.info(f"{mp4_file_name} 开始下载...")

        target_file = os.path.join(self._root_path, mp4_file_name)
        if os.path.exists(target_file):
            shutil.rmtree(target_file)

        # 获取m3u8文件信息
        if m3u8_file_path == None:
            r = requests.get(m3u8_url, headers=headers, verify=False).content
            m3u8_info = str(r, encoding="gbk").split("\n")
        else:
            with open(m3u8_file_path, "r") as f:
                data = f.readlines()
            m3u8_info = [i.replace("\n", "") for i in data]

        # m3u8url后缀名
        postfix_m3u8 = m3u8_url.split("/")[-1]
        # m3u8前缀
        # prefix_m3u8 = "/".join(m3u8_url.split("m3u8")[0].split("/")[:-1])

        # 文件是否加密
        cryptor = None
        for i in m3u8_info:
            if "EXT-X-KEY" in i:
                # 获取key_url
                if "http" in i:
                    key_url = i.split('"')[1]
                else:
                    key_name = i.split('"')[1].split("/")[-1]
                    key_url = m3u8_url.replace(postfix_m3u8, key_name)

                if "IV=" in i: 
                    iv = i.split("IV=")[1]
                    iv = iv.replace("0x", "")[:16].encode() #去掉前面的标志符, 并切片取前16位
                else:
                    iv = ""
                
                if key_url.endswith("key"):
                    key = requests.get(key_url, headers=headers, verify=False).text.encode("utf-8")
                else:
                    key = requests.get(key_url, headers=headers,verify=False).content

                if iv != "":
                    cryptor = AES.new(key, AES.MODE_CBC, iv)
                else:
                    cryptor = AES.new(key, AES.MODE_CBC)
                break

        ts_url_list = [i.strip() for i in m3u8_info if not i.startswith("#") and i != '']
        for i in range(len(ts_url_list)):
            if not ts_url_list[i].startswith("http"):
                ts_url_list[i] = m3u8_url.replace(postfix_m3u8, ts_url_list[i].split("/")[-1])
                # ts_url_list[i] = "/".join([url, ts_url_list[i]])

        if ts_url_list[0].split(".")[-1] in ["jpg", "png"]:
            delete_byte = 120
        else:
            delete_byte = 0

        # 临时文件保存
        time_str = self._get_time_str() + "_" + str(random.randint(0, 1000))
        tmp_dir = os.path.join(self._root_path, time_str)
        if os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir)
        os.makedirs(tmp_dir)

        file_list = [os.path.join(tmp_dir, f"{i}.ts")
                     for i in range(len(ts_url_list))]

        """单线程"""
        # for i,(url, file_path) in tqdm(enumerate(zip(ts_url_list, file_list)),total=len(file_list),desc="download ts"):
        #     download_ts((url, file_path, cryptor,i))
        #     if i > 10:
        #         exit()
        """线程池"""
        t_num = params["tread_num"]
        pool = ThreadPool(t_num)
        args = [(url, file_path, cryptor, i)
                for i, (url, file_path) in enumerate(zip(ts_url_list, file_list))]
        for _ in tqdm(pool.map(download_ts, args), desc="download ts", total=len(args)):
            pass

        # 文件合并
        logger.info(f"{target_file} 开始合成...")
        ts_file_name = target_file.replace(".mp4", ".ts")
        with open(ts_file_name, "ab") as f:
            for file_path in file_list:
                try:
                    with open(file_path, "rb") as tmp:
                        if delete_byte:
                            tmp.read(delete_byte)
                        data = tmp.read()
                    f.write(data)
                except Exception:
                    continue
        # 格式转换
        logger.info(f"{target_file} 开始格式转换...")
        ffmpeg_ts2mp4(ts_file_name, target_file)

        os.remove(ts_file_name)
        shutil.rmtree(tmp_dir)
        logger.info(f"{target_file} 下载完成!")

    def batch_download(self):
        """批量下载."""

        file_name = ""
        for m3u8_url_file_name in tqdm(self._m3u8_url_file_name_list):
            if m3u8_url_file_name != '':
                m3u8_url, file_name = m3u8_url_file_name.split("|")
                mp4_file_name = f"{file_name}.mp4"

                try:
                    self._download(
                        mp4_file_name=mp4_file_name,
                        m3u8_url=m3u8_url
                    )
                except Exception as e:
                    logger.error(f"{file_name}下载失败:{e}")
                    traceback.print_exc()

    def download_by_m3u8_file(self) -> None:
        """通过m3u8文件下载."""
        func_params = params["download_by_m3u8_file"]
        mp4_file_name = func_params["mp4_file_name"]
        m3u8_file_path = func_params["m3u8_file_path"]
        m3u8_url = func_params["m3u8_url"]
        self._download(mp4_file_name=mp4_file_name,
                       m3u8_file_path=m3u8_file_path,
                       m3u8_url=m3u8_url)


def merge_ts_files2mp4() -> None:
    """
    ts文件集转mp4.
    参数见merge_ts_files2mp4."""
    func_params = params["merge_ts_files2mp4"]
    ts_files_dir = func_params["ts_files_dir"]
    mp4_file_name = func_params["mp4_file_name"]
    delete_byte = func_params["delete_byte"]

    if mp4_file_name.endswith(".mp4") is False:
        mp4_file_name = del_special_char(mp4_file_name) + ".mp4"

    ids = sorted([int(i.split(".")[0]) for i in os.listdir(ts_files_dir)])
    ts_files_path_list = [os.path.join(ts_files_dir, f"{i}.ts") for i in ids]

    logger.info(f"{mp4_file_name} 开始合并...")
    mp4_file_path = ts_files_dir.replace(
        ts_files_dir.split("/")[-1], mp4_file_name)
    merge_ts_file = mp4_file_path.replace(".mp4", ".ts")
    with open(merge_ts_file, "ab") as f:
        for file_path in ts_files_path_list:
            try:
                with open(file_path, "rb") as tmp:
                    if delete_byte:
                        tmp.read(delete_byte)
                    data = tmp.read()
                f.write(data)
            except Exception:
                continue
    ffmpeg_ts2mp4(merge_ts_file, mp4_file_path)

    # os.remove(merge_ts_file)
    shutil.rmtree(ts_files_dir)
    logger.info(f"{mp4_file_path} 合并完成!")


def merge_mp4(files_dir, mp4_file_name):
    """mp4文件合并"""
    path = "/Users/tangyu/Desktop/帝都金领"
    mp4_list = []
    for f in os.listdir(path):
        file_path = os.path.join(path, f)
        mp4_list.append(VideoFileClip(file_path))
    final_clip = concatenate_videoclips(mp4_list)

    # 生成目标视频文件
    final_clip.to_videofile(
        "/Users/tangyu/Desktop/帝都金领合集.mp4", fps=24, remove_temp=False)


def test():
    # data = requests.get(url, headers=headers, verify=False).content
    # mem_mb = sys.getsizeof(data)
    # with open("~/Desktop/test.m4s", "wb") as f:
    #     f.write(data)
    # with open("/Users/tangyu/Desktop/tonyzone/movies/index.m3u8", "r") as f:
    #     data = f.readlines()
    # print([i.replace("\n", "")for i in data])
    # key_url = "https://d2hjaxnycbnwqa.cloudfront.net/20200305/enZ54Cuk/1000kb/hls/index.m3u8"
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

    m3u8_url = "https://cdn52.akamai-content-network.com/bcdn_token=n_rmFWWCKIfO43BNTdLjnylte0O4U47fD4n-TC017is&expires=1680938645&token_path=/3f8da90e-b9bd-4481-8fda-099e4cd8490c//3f8da90e-b9bd-4481-8fda-099e4cd8490c/1280x720/video.m3u8"
    r = requests.get(m3u8_url, headers=headers, verify=False).content

    m3u8_info = str(r, encoding="gbk").split("\n")
    # print(m3u8_info)
    with open("1.ts", "wb") as f:
        f.write(r)

    file_list = [
        f"/Users/tangyu/Desktop/tonyzone/movies/雙胞胎姐妹花20220302_224017/{i}.ts" for i in range(10000)]
    with open("/Users/tangyu/Desktop/雙胞胎姐妹花.mp4", "ab") as f:
        for file_path in file_list:
            try:
                with open(file_path, "rb") as tmp:
                    # tmp.read(120)
                    data = tmp.read()
                f.write(data)
            except Exception as e:
                pass


if __name__ == '__main__':

    dtf = DownloatTsFilm()
    # 通过utl地址批下载
    dtf.batch_download()

    # 聚合ts文件转mp4
    # merge_ts_files2mp4()

    # 通过m3u8文件下载
    # dtf.download_by_m3u8_file()

"""
python video_download/download_m3u8.py
"""
