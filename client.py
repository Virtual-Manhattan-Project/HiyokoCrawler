from crawler import *
from threading import Thread
import json

    
video_id = 'xNxgQ3ZxUoU'
month_range = [10, 10]

def pull_video(video_id):    
    try:
        
        v_data = pull_video_data(video_id)
        c_data = pull_chat_data(video_id)
        v_data["chat_analyzed"]= c_data["chat_analyzed"]

        f = open("data/video_data.json", "wb")
        f.write(json.dumps(v_data, ensure_ascii=False, indent=4).encode('UTF-8'))
    except requests.RequestException as e:
        print(e)
    return v_data

def pull_streamer_data(name, streamer_id, month_range):#爬取ID为streamer_id的直播主month_range内的视频数据
    print("pulling " + name)
    
    main_list = []#主数据表
    f = open("data/" + name + ".json", "wb")
    
    video_id_list = pull_video_id(streamer_id, month_range)#爬取ID为streamer_id的直播主month_range内的视频数据
    for v in video_id_list:
        v_data = pull_video(v)
        main_list.append(v_data)
     
    f.write(json.dumps(v_data + c_data, ensure_ascii=False, indent=4).encode('UTF-8'))
    print(name + " saved")

# g = requests.post(group_url, headers=headers, data=json.dumps({'filter_state': '{"text": "", "inc_old_group": false, "retired" : "all", "following":false, "notifications": false}', 'page': 0, 'sort_by': 'subscriber_count'}))
# streamer_list = json.loads(g.text)
# data2 = json.dumps(streamer_list, sort_keys=True, indent=4, separators=(',', ': '))
# open("VTuber.json", "w").write(data2)

pull_video(y7_QWshqsw0)