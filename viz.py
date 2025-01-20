import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


def main():
    df = pd.read_csv("log.txt", sep=",", header=None, names=["lib", "foo", "time"])
    df = df.loc[df["foo"].str.startswith("load_data_to_postgres_by_")]
    df["foo"] = df["foo"].str.replace("load_data_to_postgres_by_", "")
    df["foo"] = df["foo"].str.replace("psycopg2", "pg")
    df["foo"] = df["foo"].str.replace("pandas", "pd")

    sns.set_theme(style="darkgrid")

    sns.barplot(x="foo", y="time", data=df)

    plt.savefig("viz.png", dpi=600)
    print("Saved viz.png with %d bars and %d libs" % (df.shape[0], len(df["lib"].unique())))


if __name__ == "__main__":
    main()
