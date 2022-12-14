import pandas as pd
from .mytool import tools


# 计算年总资产收益率
def cal_zzcsyl(quarter, conn, dat_pe):
    y_ = tools.get_quarter_array(f=2, day=quarter)
    # print(y_, '=====')
    dat_pe_copy = dat_pe.copy()
    for i, t in dat_pe[0:].iterrows():
        # print(t['code'], t['name'])
        for ii, xx in enumerate(y_[:3]):
            # 查询返回季度第一个有没有这个表
            # print(ii, xx)
            table_ = pd.read_sql(
                """select name from sqlite_master where type='table' and
                    name like '{}'""".format('stock_zcfz_em' + xx),
                conn
            )
            # print(table_.values)
            # return
            if table_.shape[0] == 1:  # 如果表恰好有一个
                if xx[:4] == '1231':  # 如果第一是年度
                    raise
                else:  # 如果第一是不是年度
                    dat_pe_copy = cal_zzcsyl_son(
                            conn, i, t, y_, dat_pe_copy, ii
                        )
                break
            if i == 2:  # 循环３个季度后还没有表－报错
                raise
    return dat_pe_copy


def cal_zzcsyl_son(conn, i, t, y_, dat_pe_copy, ii):
    code_ = t['code'][3:]
    zzc4 = 0
    for zz in range(4):  # 第ii季度之后连续计算４个季度总资产
        # print(t['code'], y_[ii+zz])
        # print(t['code'][3:])
        # 查询该表下面是否有这个股票的总资产数据
        zzc = pd.read_sql(
            """select 总资产 from {} where 股票代码='{}'
                """.format('stock_zcfz_em' + y_[ii+zz], code_),
            conn
        )
        if zzc.shape[0] == 1:  # 总资产有多条－＞报错
            zzc4 += zzc['总资产'].values[0]
        elif zzc.shape[0] == 2:
            zc = zzc['总资产'].values
            if zc[0] == zc[1]:
                zzc4 += zc[0]
            else:
                print(zzc, zc[0] == zc[1])
                raise
        else:
            print(zzc)
            raise
    #     print(zzc4)
    # print(zzc4/4)
    # 查询当季度利润表
    y_ii = y_[ii]  # 获取当前季度
    q_lir = pd.read_sql(
        """select 净利润 from {} where 股票代码='{}'
            """.format('stock_lrb_em' + y_ii, code_),
        conn
    )
    # 查询下一年度利润表
    yy = str(int(y_ii[:4])-1)
    # print(yy, 'tttttttt', yy + y_ii[4:])
    y_lir = pd.read_sql(
        """select 净利润 from {} where 股票代码='{}'
            """.format('stock_lrb_em' + yy + '1231', code_),
        conn
    )
    # 获取下一年相同季度净利润
    q_lir2 = pd.read_sql(
        """select 净利润 from {} where 股票代码='{}'
            """.format('stock_lrb_em' + yy + y_ii[4:], code_),
        conn
    )
    # 有多条－＞报错
    if q_lir.shape[0] != 1 or y_lir.shape[0] != 1 or q_lir2.shape[0] != 1:
        print(q_lir, y_lir, q_lir2)
        raise
    # 计算滚动年度净利润
    # print(q_lir['净利润'], '=====')
    y_lir2 = q_lir['净利润'] + y_lir['净利润'] - q_lir2['净利润']
    # print('=====', y_lir2.values[0])
    if zzc4:  # 如果该股票在年度表里有数据
        # 计算总资产收益率
        syl = y_lir2.values[0]/(zzc4/4)
        # print(syl, '====pppp=', zzc4/4, syl < 0.04)
        if syl < 0.04:  # 总资产收益率小于０.０４，删除该行数据
            # print(i, 'uuuuu')
            dat_pe_copy = dat_pe_copy.drop(labels=i)
            # print(dat_pe_copy)
    return dat_pe_copy
