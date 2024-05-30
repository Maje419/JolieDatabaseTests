import pandas as pd
import matplotlib
import matplotlib.pyplot as plt

font = {'size'   : 12}
matplotlib.rc('font', **font)

def getMean(path: str) -> pd.DataFrame:
    df = pd.read_csv(filepath_or_buffer=path, sep=' ')
    return df.groupby("test").mean()

noConnectionpoolDf = getMean("results/JDBC_HSQL.csv")
connectionPoolDf = getMean("results/Hikari_HSQL.csv")

index = connectionPoolDf.columns

dfNew = pd.merge(noConnectionpoolDf, connectionPoolDf, on='test', how="inner")

dfNew.columns = ["JDBC implementation", "HikariCP implementation"]

ax = dfNew.plot(kind="barh", logx=True, rot=0)

ax.set_xlabel("Avg. ms")

patches = [" \\\\ ", "//"]

for bars in ax.containers:
    ax.bar_label(bars, padding=5)


plt.savefig("mean_ms_for_db_operations.png")
plt.show()