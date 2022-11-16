import pandas as pd

# 创建字典
dict_1 = {'年龄': [23, 22, 21], '岗位': ['客服', '运营', '公关'], '年购买量': [10, 15, 8]}
# 创建df对象
df = pd.DataFrame(dict_1)
df2 = pd.DataFrame()
for i, t in df.iterrows():
    # df_no_total.loc[i] = t.values
    df2 = pd.concat([df2, t], axis=1)
    # print(t)
#     df_no_total = pd.concat([df_no_total, pd.DataFrame(t)], axis=1)
print(df2)
df2 = pd.DataFrame(df2.values.T, index=df2.columns, columns=df2.index)
# df_no_total = pd.concat([df_no_total, df], axis=0)
# df_no_total = pd.concat([df_no_total, df], axis=0)
print(df2)
