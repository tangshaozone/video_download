import re
import requests
import json
import os

headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36',
    'cookie': ""  # YOUR COOKIES
    }


def download_single_video(url, name):
    res = requests.get(url, headers=headers)
    video_pattern = '__playinfo__=(.*?)</script><script>'
    playlist_info = json.loads(re.findall(video_pattern, res.text)[0])
    video_url = playlist_info['data']['dash']['video'][0]['baseUrl']
    audio_url = playlist_info['data']['dash']['audio'][0]['baseUrl']
    save_file(video_url, 'video')
    save_file(audio_url, 'audio')
    merge(name)
    print('{} 下载完毕'.format(name))


def save_file(url, type):
    download_content = requests.get(url, headers=headers).content
    with open('{}.mp4'.format(type), 'wb') as output:
        output.write(download_content)


def merge(name):
    command = 'ffmpeg -i video.mp4 -i audio.mp4 -c copy "{}".mp4'.format(name)
    os.system(command)
    os.remove('video.mp4')
    os.remove('audio.mp4')


def get_list_info(url):
    aid_pattern = 'window.__INITIAL_STATE__={"aid":(\d*?),'
    res = requests.get(url, headers=headers)
    aid = re.findall(aid_pattern, res.text)[0]
    playlist_json_url = 'https://api.bilibili.com/x/player/pagelist?aid={}'.format(aid)
    json_info = json.loads(requests.get(playlist_json_url, headers=headers).content.decode('utf-8'))['data']
    return json_info


if __name__ == '__main__':
    base_url = 'https://www.bilibili.com/video/BV14J4114768'
    json_info = get_list_info(base_url)
    for i in json_info:
        p = i['page']
        name = 'P{} - {}'.format(p, i['part'])
        url = base_url + '?p={}'.format(p)
        download_single_video(url, name)
