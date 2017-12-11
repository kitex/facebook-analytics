import sqlite3
import pandas as pd
import numpy as np

connection = sqlite3.connect('E:\\python\\notebook\\course3_downloads\\feed.db')
cursor = connection.cursor()
test_done = False
limit = 5000
cur_length = limit


df = pd.read_sql("""select m.status_message quest,c.status_message cmt from comment c
                left join main_post m
                on 
                c.post_id = m.status_id
                where m.status_message <> ""
                order by c.post_id limit {}
                """.format(limit), connection)


msk = np.random.rand(len(df)) < 0.8
df = df.replace('\n',' ', regex=True).replace('\r',' ',regex=True).replace('"',"'")
train = df[msk]

test = df[~msk]
pd.set_option('display.max_colwidth', -1)
print(len(test), len(train))
np.set_printoptions(threshold=np.nan)
test['quest'][:5]
with open('E:\\python\\notebook\\DeepLearning\\traintestsplit\\test.from', 'w', encoding='utf8') as f:
    for content in test['quest'].values:
        #print(content)
        f.write(content + '\n')

with open('E:\\python\\notebook\\DeepLearning\\traintestsplit\\test.to', 'w', encoding='utf8') as f:
    for content in test['cmt'].values:
        f.write(str(content) + '\n')

with open('E:\\python\\notebook\\DeepLearning\\traintestsplit\\train.from', 'w', encoding='utf8') as f:
    for content in train['quest'].values:
        f.write(content + '\n')

with open('E:\\python\\notebook\\DeepLearning\\traintestsplit\\train.to', 'w', encoding='utf8') as f:
    for content in train['cmt'].values:
        f.write(str(content) + '\n')
print("Write successful...")