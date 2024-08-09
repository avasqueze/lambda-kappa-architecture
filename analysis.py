import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


def add_offsets_to_duplicates(df, time_column):
    duplicates = df[time_column].duplicated(keep=False)
    df.loc[duplicates, time_column] += pd.to_timedelta(df.groupby(time_column).cumcount(), unit='ms')
    return df


def load_lambda():
    """
    Loading csv files for lambda representation
    """
    all_data = pd.read_csv("data_samples/lambda_all_data_view.csv", header=None)
    batch = pd.read_csv("data_samples/lambda_batch_view.csv", header=None)
    real = pd.read_csv("data_samples/lambda_real_time_view.csv", header=None)

    all_data = all_data.rename(columns={7: "time",
                                        4: "ones"}).sort_values(by="time").reset_index(drop=True)
    all_data["cumulated"] = all_data["ones"].cumsum()
    all_data["time"] = pd.to_datetime(all_data["time"])
    all_data["category"] = "all_data"
    all_data = add_offsets_to_duplicates(all_data, 'time')

    batch = batch.rename(columns={2: "time",
                                  1: "cumulated"}).sort_values(by="time").reset_index(drop=True)
    batch["time"] = pd.to_datetime(batch["time"])
    batch["category"] = "HadoopResults"
    batch["time_diff_batch"] = batch["time"].diff()

    real = real.rename(columns={3: "time",
                                1: "cumulated"}).sort_values(by="time").reset_index(drop=True)
    real["time"] = pd.to_datetime(real["time"])
    real["category"] = "SparkResults"
    real = real.iloc[2:66].reset_index(drop=True)
    real["time_diff_real"] = real["time"].diff()

    results = []
    for i in range(len(batch) - 1):
        start_time = batch.loc[i, 'time']
        end_time = batch.loc[i + 1, 'time']
        interval_data = real[(real['time'] >= start_time) & (real['time'] < end_time)]
        sum_real_time = interval_data['cumulated'].sum() + batch.loc[i, "cumulated"]
        results.append({
            'time': end_time,
            'cumulated': sum_real_time,
            "category": "SparkResults+HadoopResults"
        })

    lambda_batch_real_combined = pd.concat([all_data, pd.DataFrame(results)], axis=0)
    lambda_batch_real_separeted = pd.concat([all_data, batch, real], axis=0)

    return lambda_batch_real_combined, lambda_batch_real_separeted


def load_kappa():
    """
    Loading csv files for kappa representation
    """
    all_data = pd.read_csv("data_samples\\kappa_all_data_view.csv", header=None)
    all_data = all_data.rename(columns={6: "time",
                                        4: "ones"}).sort_values(by="time").reset_index(drop=True)
    all_data["ones"] = 1
    all_data["cumulated"] = all_data["ones"].cumsum()
    all_data["time"] = pd.to_datetime(all_data["time"])
    all_data["category"] = "all_data"

    real = pd.read_csv("data_samples\\kappa_real_time_view.csv", header=None)
    real = real.rename(columns={3: "time",
                                1: "cumulated"}).sort_values(by="time").reset_index(drop=True)
    real["time"] = pd.to_datetime(real["time"])
    real["category"] = "SparkResults"
    real["time_diff_real"] = real["time"].diff()

    all_data = add_offsets_to_duplicates(all_data, 'time')

    all_data_combined = pd.concat([all_data,real], axis=0)

    offset_data = all_data.merge(real, on="cumulated", how="outer").reset_index(drop=True)
    offset_data["delay"] = offset_data["time_y"] - offset_data["time_x"]

    return all_data_combined, offset_data

# Plot for Lambda
lambda_batch_real_combined, lambda_batch_real_separeted = load_lambda()

fig = plt.figure(figsize=(15, 10))
plt.title("Lambda Architecture", size=20)
sns.lineplot(lambda_batch_real_combined, x="time", y="cumulated", hue="category")
plt.legend(fontsize="x-large")
plt.xticks(fontsize=16)
plt.yticks(fontsize=16)
plt.xlabel('Time', fontsize=18)
plt.ylabel('Cumulated Purchases', fontsize=18)
plt.grid(alpha=0.2)
plt.savefig("docs\\lambda_results.png")
plt.show()

fig = plt.figure(figsize=(15, 10))
plt.title("Lambda Architecture", size=20)
sns.lineplot(lambda_batch_real_separeted, x="time", y="cumulated", hue="category")
plt.legend(fontsize="x-large")
plt.xticks(fontsize=16)
plt.yticks(fontsize=16)
plt.xlabel('Time', fontsize=18)
plt.ylabel('Cumulated Purchases', fontsize=18)
plt.grid(alpha=0.2)
plt.savefig("docs\\lambda_results_batch_real.png")
plt.show()

# Plot for Kappa
kappa_data, offset_data_kappa = load_kappa()

fig = plt.figure(figsize=(15, 10))
sns.lineplot(kappa_data, x="time", y="cumulated", hue="category")
plt.title("Kappa Architecture", size=20)
plt.legend(fontsize="x-large")
plt.xticks(fontsize=16)
plt.yticks(fontsize=16)
plt.xlabel('Time', fontsize=18)
plt.ylabel('Cumulated Purchases', fontsize=18)
plt.grid(alpha=0.2)
plt.savefig("docs\\kappa_results.png")
plt.show()

fig = plt.figure(figsize=(15, 10))
sns.boxplot(offset_data_kappa, x="delay")
plt.show()