import pandas as pd
import sqlite3
from pandas import DataFrame
from sqlalchemy import create_engine

con = sqlite3.connect(r"C:\Users\K\Desktop\excel-upload-sqlite3\mins\db.sqlite3")
sql = "SELECT rilegoule.name FROM rilegoule"
df = pd.read_sql(sql, con)
name_list = df['name'].tolist()
sql_number = len(name_list)
i = 0
#依次对数据库中的每一行添加一列id
for name in df['name'].unique():
    i = i+1
    #df.loc[df['name'] == name, "id"] = i
    with con:
        cur = con.cursor()
        cur.execute("""UPDATE rilegoule SET id=? WHERE name=?""", (i, name))
        # df.to_sql("rilegoule", con, if_exists="append", index=False)#失败，新增的一行在以前的后面，id没有增加在之前行的后面
        # df.to_sql("rilegoule", con, if_exists="replace", index=False)#失败，全部替换，只剩下name和id两行
print("tosql!")

excel_data = pd.read_excel(r"C:\Users\K\Desktop\rilegoule.xlsx")
excel_name_list = excel_data['name'].tolist()
for name in excel_name_list:
    if name in name_list:
        con = sqlite3.connect(r"C:\Users\K\Desktop\excel-upload-sqlite3\mins\db.sqlite3")
        sql = "SELECT * FROM rilegoule"
        print(name)
        df = pd.read_sql(sql, con)
        name_dataframe = df[df["name"]==name]
        id = name_dataframe.loc[name_dataframe.last_valid_index(), 'id']

        index = excel_data[excel_data["name"]==name]
        commit_data = pd.DataFrame(data=excel_data, index=[index.last_valid_index()], columns=excel_data.columns)
        #构建单列dataframe
        commit_data.loc[index.last_valid_index(), "id"] = id
        #锁定哪一行的dataframe
        # commit_data = excel_data.loc[index.last_valid_index()]
        # commit_data.to_sql("rilegoule", con, if_exists="replace", index=False)
        name = commit_data.loc[index.last_valid_index(), "name"]
        class1 = str(commit_data.loc[index.last_valid_index(), "class"])
        with con:
            cur = con.cursor()
            cur.execute("""UPDATE rilegoule SET name=?,class=? WHERE id=?""", (name, class1, id))
        #commit_data.to_sql("rilegoule", con, if_exists="replace", index=False)#
        print("if")
    else:
        sql_number = sql_number+1
        index = excel_data[excel_data["name"]==name]
        commit_data = pd.DataFrame(data=excel_data, index=[index.last_valid_index()], columns=excel_data.columns)
        commit_data.loc[index.last_valid_index(), "id"] = sql_number
        # commit_data = excel_data.loc[index.last_valid_index()]
        # commit_data.to_sql("rilegoule", con, if_exists="append", index=False)
        commit_data.to_sql("rilegoule", con, if_exists="append", index=False)
        print("else")
print(excel_name_list)
print(name_list)

'''
更新column的顺序
df = df[['id', 'class', 'address']]
old_names = ['$a', '$b', '$c', '$d', '$e']
new_names = ['a', 'b', 'c', 'd', 'e']
df.rename(columns=dict(zip(old_names, new_names)), inplace=True)
frame = frame[['column I want first', 'column I want second'...etc.]]
'''
