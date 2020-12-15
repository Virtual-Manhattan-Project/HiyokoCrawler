# 洗数据时各种写下的代码，乱的一P，现在我都快忘了干嘛的
# 有时间有心情再整理成函数吧/(ㄒoㄒ)/~~
# 虽然估计也就用一次就再也用不到也没人看了。。。。(╯‵□′)╯︵┻━┻

import json
import glob
import pandas as pd
from datetime import datetime
from datetime import timedelta

# df = pandas.read_csv('hiyoko.csv')
# df = df[df[['max_viewers','view_count_live_end','like_count_live_end','chat_author_count','chat_count']].notnull().all(1)]

# df_cover = df.loc[((df['groups'] == "cover") | (df['groups'] == "hololiveid") | (df['groups'] == "hololiveen"))]
# df_njsj = df.loc[(df['groups'] == "nijisanjip") | (df['groups'] == "nijisanjiid")]
# df_others = df.loc[(df['groups'] != "nijisanjip") & (df['groups'] != "nijisanjiid") & (df['groups'] != "cover") & (df['groups'] != "hololiveid") & (df['groups'] != "hololiveen")]

# df_group = df_others
# df_group =  df_group.drop(columns=['name','groups','video_date','chat_author_count','chat_count'])
# df_group = df_group.rename(columns={'view_count_live_end':'view','like_count_live_end':'like'})
# df_group.to_csv('others.csv',index=False)


# df = pd.read_csv('Vtuber.csv')
# df= df[:50]
#
# df_cover = pd.read_csv('others.csv')
# df_cover = df_cover.loc[df_cover['channel_id'].isin(df['ch_ids'])]
# df_cover = df_cover.loc[df_cover['view'] > 50000]
# df_cover = df_cover['channel_id'].drop_duplicates()
# df = df.loc[df['ch_ids'].isin(df_cover)]
# print(df.count())


# df = pd.DataFrame(columns=['name', 'groups', 'subscriber_count', 'ch_ids', 'streamer_id'])
# with open('Vtuber.json') as openfileobject:
#     for line in openfileobject:
#         if not line[0].isdigit():
#             j = json.loads(line)
#             for v in j['result']:
#                 df.loc[v['rank']] = [v['name'], v['groups'], v['subscriber_count'], v['ch_ids'], v['streamer_id']]
#         else:
#             print(line)

# df.to_csv('Vtuber.csv', index=False)


def data_string_to_np(s: str):
    return np.fromstring(s[1:len(s) - 1], sep=',')


day = timedelta(days=1)


def labels_firstday(labels: list):
    firstday = next(x for x in labels if len(x) != 0)
    date = datetime.strptime(firstday, '%m/%d')
    return date - day * labels.index(firstday)


def fill_labels(timespan):
    return [datetime.strftime(timespan[0] + x * day, '%m/%d') for x in range(timespan[1])]


with open('hiyoko_fetch_summary_long_history_v2.data') as openfileobject:
    df = data = pd.DataFrame()
    data_timespan = 0, 0  # firstday, length
    Vtuber = pd.read_csv("Vtuber.csv")
    status = 'no-data'

    for line in openfileobject:
        # read data
        if len(line) == 33:
            streamer_id = line[:-1]
            name = Vtuber.loc[Vtuber['streamer_id'] == streamer_id].iat[0, 0]

        elif line[0] == '{':
            j = json.loads(line)['long_history']
            status = j['status']
            continue

        # deal data
        elif status == 'ok':
            labels = j['labels']
            timespan = labels_firstday(labels), len(labels)
            subscriber_count = j['subscriber_count']

            if data_timespan != timespan:
                df = df.append(data)
                data = pd.DataFrame(columns=['name', 'streamer_id']+fill_labels(timespan))
                data_timespan = timespan
            try:
                data.loc[len(data)] = [name, streamer_id] + subscriber_count
            except ValueError:
                print(name, streamer_id, subscriber_count)

            status = 'no-data'

df = df.append(data)
df.to_csv('subscriber_count.csv', index=False)


files = glob.glob('hiyoko-1/*.json')
name_slice = slice(9, -5)
df = pd.DataFrame(columns=['name', 'ch_id', 'video_id', 'max_viewers', 'title', 'video_date', 'video_duration',
                           'view_count_live_end', 'like_count_live_end', 'labels', 'like_count_list',
                           'viewers', 'author_count', 'chat_count', 'superchat_pops'], dtype=object)

Vtuber = pd.read_csv('Vtuber.csv', index_col='name')
ch_ids = []
for ch_id in Vtuber['ch_ids']:
    if not pd.isna(ch_id):
        ch_id = ch_id[ch_id.find('UC'):]
    ch_ids.append(ch_id)
Vtuber['ch_ids'] = ch_ids
Vtuber = Vtuber['ch_ids']

i = 0
for file in files:
    with open(file) as f:
        name = f.name[name_slice]
        ch_id = Vtuber[name]
        videos = json.load(f)

        for j in videos:
            data = [name, ch_id, j['video_id'], j['max_viewers'], j['title'],
                    j['video_date'], j['video_duration'], j['view_count_live_end']]

            viewer_chart = j['viewer_chart']
            # 奇葩hiyoko会缺少like_count_live_end
            data.append(pd.NA if viewer_chart['like_count_list'] is None else j['like_count_live_end'])
            data += [pd.NA, pd.NA, pd.NA] if len(viewer_chart['labels']) == 0 else list(viewer_chart.values())

            try:
                chat_analyzed = j['chat_analyzed']
                if chat_analyzed['status'] == 'ok':
                    data[1] = chat_analyzed['aggr_video_freq']['ch_id']
                    data += list(chat_analyzed['aggr_video'].values())
                else:
                    data += [pd.NA, pd.NA, pd.NA]
                df.loc[i] = data
                i += 1
            except KeyError:
                print(f.name + j['video_id'] + ' BUG_DATA')
    print(i.__str__() + '.' + name)

df.to_csv('hiyoko-1.csv', index=False)
#
# df1 = pd.read_csv('hiyoko-1.csv')
# df2 = pd.read_csv('hiyoko-2.csv')
# df = df1.append(df2)
# df = df.sort_values('name')
# df.to_csv('hiyoko.csv', index=False)
