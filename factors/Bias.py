import numpy as np
def signal(*args):
    df = args[0]
    n = int(args[1])
    factor_name = args[2]

    source_cls = df.columns.tolist()

    df = df.dropna(axis=0, subset=["close"]).reset_index(drop=True)

    add_index = np.where(df.index >= n, df.index - n + 1, 0)
    hm_index = (
        df["high"].rolling(n, min_periods=1).apply(lambda x: x.argmax(), raw=True)
    )
    hm_index += add_index
    distance = df.index - hm_index + 1

    tp = (df["close"] + df["high"] + df["low"]) / 3
    ma = tp.rolling(n, min_periods=1).mean()
    bias = tp / ma

    df[factor_name] = bias / distance

    df.drop(
        columns=list(
            set(df.columns.values).difference(set(source_cls + [factor_name]))
        ),
        inplace=True,
    )

    return df
