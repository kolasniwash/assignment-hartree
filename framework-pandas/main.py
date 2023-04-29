from typing import List
import pandas as pd

def merge_dfs(left_df: pd.DataFrame, right_df: pd.DataFrame, key: str):
    return left_df.merge(right_df, how="left", left_on=key, right_on=key)


def base_group_set(df: pd.DataFrame, group_set: List[str], total_method="count") -> pd.DataFrame:

    grouping_set_items = {"legal_entity", "counter_party", "tier"}
    out_of_group_items = set(group_set).symmetric_difference(grouping_set_items)

    aggregation_dict = dict((item, total_method) for item in out_of_group_items)
    aggregation_dict["rating"] = "max"

    return df.groupby(group_set, dropna=False).agg(aggregation_dict)


def sum_status(df: pd.DataFrame, group_set: List[str], status: str) -> pd.DataFrame:

    if status not in ["ARAP", "ACCR"]:
        raise ValueError("Status must be one of ARAP, ACCR")

    return df[df["status"] == status].groupby(group_set)\
        .agg({"value": "sum"})\
        .rename(columns={"value": f"sum_of_{status}"})


def union_datasets(views: List[pd.DataFrame]) -> pd.DataFrame:
    return pd.concat(views, axis=0)


def run(grouping_sets: List[List[str]]) -> pd.DataFrame:

    left_df = pd.read_csv("../data/dataset1.csv")
    right_df = pd.read_csv("../data/dataset2.csv")

    df_merged = merge_dfs(left_df, right_df, "counter_party")
    views = list()

    for group_set in grouping_sets:
        base_group = base_group_set(df_merged, group_set, total_method="count")
        ARAP = sum_status(df_merged, group_set, "ARAP")
        ACCR = sum_status(df_merged, group_set, "ACCR")

        view = base_group.join(ARAP, group_set, how="left") \
            .join(ACCR, group_set, how="left") \
            .fillna(0) \
            .reset_index()
        print("------------")
        print(view)
        views.append(view)

    print("------------")
    return union_datasets(views)



if __name__ == "__main__":
    grouping_sets = [
        ["legal_entity"],
        ["legal_entity", "counter_party"],
        ["legal_entity", "counter_party", "tier"],
        ["tier"],
        ["counter_party"]
    ]

    # print(df_merged)
    print("-----------")
    print(run(grouping_sets))
