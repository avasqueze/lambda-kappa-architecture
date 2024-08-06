import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def add_offsets_to_duplicates(df, time_column):
    duplicates = df[time_column].duplicated(keep=False)
    df.loc[duplicates, time_column] += pd.to_timedelta(df.groupby(time_column).cumcount(), unit='ms')
    return df

all_data = pd.read_csv("data_samples\\all_data_view.csv", header=None)
batch = pd.read_csv("data_samples\\batch_view.csv", header=None)
real = pd.read_csv("data_samples\\real_time_view.csv", header=None)

all_data = all_data.rename(columns={7: "time",
                                    4: "ones"}).sort_values(by="time").reset_index(drop=True)
all_data["cummulated"] = all_data["ones"].cumsum()
all_data["time"] = pd.to_datetime(all_data["time"])
all_data["category"] = "all_data"
all_data = add_offsets_to_duplicates(all_data, 'time')

batch = batch.rename(columns={2: "time",
                              1: "cummulated"}).sort_values(by="time").reset_index(drop=True)
batch["time"] = pd.to_datetime(batch["time"])
batch["category"] = "batch"
batch["time_diff_batch"] = batch["time"].diff()


real = real.rename(columns={3: "time",
                             1: "cummulated"}).sort_values(by="time").reset_index(drop=True)
# real["cummulated"] = real["cummulated"].cumsum() # can be commented out
real["time"] = pd.to_datetime(real["time"])
real["category"] = "real"
real = real.iloc[2:66].reset_index(drop=True)
real["time_diff_real"] = real["time"].diff()

combined_batch_real = batch.copy(deep=True)
combined_batch_real["cummulated"] = batch["cummulated"] + real["cummulated"]
combined_batch_real["category"] = "both"

all_data_combined = pd.concat([all_data,
                               # batch,
                               # real,
                               combined_batch_real
                               ], axis=0)


sns.lineplot(all_data_combined, x="time", y="cummulated", hue="category")
plt.show()

all_data = pd.read_csv("data_samples\\kappa_all_data_view.csv", header=None)
all_data = all_data.rename(columns={6: "time",
                                    4: "ones"}).sort_values(by="time").reset_index(drop=True)
all_data["ones"] = 1
all_data["cummulated"] = all_data["ones"].cumsum()
all_data["time"] = pd.to_datetime(all_data["time"])
all_data["category"] = "all_data"

real = pd.read_csv("data_samples\\kappa_real_time_view.csv", header=None)
real = real.rename(columns={3: "time",
                             1: "cummulated"}).sort_values(by="time").reset_index(drop=True)
real["time"] = pd.to_datetime(real["time"])
real["category"] = "real"
real["time_diff_real"] = real["time"].diff()



# Apply the function
all_data = add_offsets_to_duplicates(all_data, 'time')


all_data_combined = pd.concat([all_data,
                               real,
                               ], axis=0)
# Function to add offsets to duplicate times

sns.lineplot(all_data_combined, x="time", y="cummulated", hue="category")
plt.show()