import pandas as pd
import numpy as np


def test2():
    df = pd.DataFrame({
        'name': ['lili', 'lucy', 'pegga', 'alin', ''],
        'age': [18, 16, np.nan, 23, np.nan],
        'salary': [np.nan, 300, np.nan, 1000, 800]
    })
    df_contains_nan = df.replace('', 0)
    print(df_contains_nan)

    df_contains_nan_2 = df.isna()
    print(df_contains_nan_2)


test2()
