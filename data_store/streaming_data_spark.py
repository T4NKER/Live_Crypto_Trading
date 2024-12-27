import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os


def write_to_parquet(batch, file_name="streaming_data.parquet"):
    df = pd.DataFrame(batch)

    if os.path.exists(file_name):
        existing_table = pq.read_table(file_name)
        existing_df = existing_table.to_pandas()

        for col in existing_df.columns:
            if col not in df.columns:
                df[col] = None
        for col in df.columns:
            if col not in existing_df.columns:
                existing_df[col] = None

        existing_df = existing_df.dropna(axis=1, how="all")
        df = df.dropna(axis=1, how="all")

        df = df[existing_df.columns]


        combined_df = pd.concat([existing_df, df], ignore_index=True)

        table = pa.Table.from_pandas(combined_df)
        pq.write_table(table, file_name, compression="snappy")
    else:
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, file_name, compression="snappy")

    print(f"Written {len(batch)} records to {file_name}")
