from crawler import *
from threading import Thread
import json
import jsonstreams
import requests
import multiprocessing

month_range = [5, 10]


def pull_video(video_id):
    try:

        v_data = pull_video_data(video_id)
        c_data = pull_chat_data(video_id)
        v_data["chat_analyzed"] = c_data["chat_analyzed"]

        # f = open("data/video_data.json", "wb")
        # f.write(json.dumps(v_data, ensure_ascii=False, indent=4).encode('UTF-8'))
    except requests.RequestException as e:
        print(e)
    return v_data


# 爬取ID为streamer_id的直播主month_range内的视频数据
def pull_streamer_data(name, streamer_id, month_range):
    print("pulling " + name)
    with jsonstreams.Stream(jsonstreams.Type.array, "crawled/" + name + ".json", indent=2, pretty=True) as s:
        # 爬取ID为streamer_id的直播主month_range内的视频数据
        video_id_list = pull_video_id(streamer_id, month_range)
        for v in video_id_list:
            v_data = pull_video(v)
            s.iterwrite(v_data.items())
    print(name + " saved")


def pull_streamer(streamer_id):
    streamer_list = json.load(open("data/VTuber.json"))['result']
    for streamer in streamer_list:
        if streamer_id == streamer["streamer_id"]:
            pull_streamer_data(streamer["name"], streamer_id, month_range)


def get_streamers():
    for page in range(5):
        g = requests.post(group_url, headers=headers, data=json.dumps(
            {
                'filter_state': '''{
    "text": "",
    "inc_old_group": false,
    "retired": "all",
    "following": false,
    "notifications": false
}''',
                'page': page,
                'sort_by': 'subscriber_count'
            }))
        streamer_list = json.loads(g.text)
        for streamer in streamer_list["result"]:
            yield (streamer["name"], streamer["streamer_id"])


if __name__ == "__main__":
    def pull_streamer_data_wrapper(pair):
        pull_streamer_data(pair[0], pair[1], month_range)
    with multiprocessing.Pool(4) as p:
        p.map(pull_streamer_data_wrapper, get_streamers(), 1)

# streamer_list= json.load(open("data/VTuber.json"))['result']
# for streamer in streamer_list:
#     streamer_name = streamer["name"]
#     streamer_id = streamer["streamer_id"]
#     Thread(target=pull_streamer_data, args=(streamer_name, streamer_id, month_range)).start()
# pull_video('y7_QWshqsw0')
