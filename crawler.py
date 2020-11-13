import requests
import json
from datetime import datetime

month_range = [10, 10]
year = 2020

group_url = "https://hiyoko.sonoj.net/f-slow/avtapi/search/streamer/fetchv3"
list_url = "https://hiyoko.sonoj.net/f-slow/avtapi/strm/fetch_history_v2"
data_url = "https://hiyoko.sonoj.net/f-slow/avtapi/video/fetch_summary"
chatdata_url = "https://hiyoko.sonoj.net/f-slow/avtapi/video/fetch_summary_chatdata"
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.80 Safari/537.36', "Content-Type": "application/json"}

############
def pull_video_id(streamer_id, month_range):#从新到旧,爬取从from_month月到to_month月的视频ID
    video_id_list = []
    month_range.sort()
    for month in range(month_range[0],month_range[1] + 1):
        r = requests.post(list_url, headers=headers, data=json.dumps({"streamer_id": streamer_id, "year": year, "month": month}))

        #open("fetch_history.json","w").write(r.text)
        video_list = json.loads(r.text)
        for z in video_list["history_array"]:
            video_id_list.append(z["video_id"])
        
    return video_id_list

######

def pull_video_data(video_id):#获得ID为video_id的视频的数据
    l = requests.post(data_url, headers=headers, data=json.dumps({"video_id":video_id}))
    v_data = json.loads(l.text)
    
    v_data.pop("ch_id")
    v_data.pop("ch_type")
    v_data.pop("channel_misc")
    v_data.pop("channel_name")
    v_data.pop("desc")
    v_data.pop("event_type")
    v_data.pop("groups")
    v_data.pop("last_fetched")
    v_data.pop("like_count")
    v_data.pop("streamer_id")
    v_data.pop("streamer_name")
    v_data.pop("streamer_name_en")
    v_data.pop("streamer_thumbnail_url")
    v_data.pop("thumbnail_url")
    v_data.pop("view_count")
    v_data["video_date"] = datetime.utcfromtimestamp(v_data["video_date"] / 1000 + 32400).strftime('%Y-%m-%d %H:%M:%S')#时间戳转换为日本时间
    try:#获得直播结束时的赞数以及赞/人数比
        v_data["like_count_live_end"] = v_data["viewer_chart"]["like_count_list"][-1]
       # v_data.pop("viewer_chart")
    except TypeError:
        pass
    print("pulled video " + video_id)
    return v_data

def pull_chat_data(video_id):#获得ID为video_id的聊天的数据
    l = requests.post(chatdata_url, headers=headers, data=json.dumps({"video_id":video_id}))
    c_data = json.loads(l.text)
    print("pulled chat " + video_id)
    return c_data

##################

#获取指定Vtuver信息
# g = requests.post(group_url, headers=headers, data=json.dumps({'filter_state': '{"selectedGroups": "cover", "text": "", "inc_old_group": false, "retired" : "all", "following":false, "notifications": false}', 'page': 0, 'sort_by': 'subscriber_count'}))
# streamer_list = json.loads(g.text)
