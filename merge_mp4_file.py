from moviepy.editor import *
import os

root_path = "/Users/tangyu/Desktop/tonyzone/movies/帝都金领"
video_lists = []
for file in os.listdir(os.path.join(root_path))[:30]:
    if file.endswith("mp4"):
        file_path = os.path.join(root_path, file)
        video = VideoFileClip(file_path)
        video_lists.append(video)
final_clip = concatenate_videoclips(video_lists)
final_clip.write_videofile(os.path.join(root_path,"res1.mp4"))