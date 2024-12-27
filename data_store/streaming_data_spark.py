import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os


def write_to_parquet(batch, file_name="streaming_data.parquet"):
    df = pd.DataFrame(batch)

    if os.path.exists(file_name):
        existing_table = pq.read_table(file_name)
        existing_df = existing_table.to_pandas()

        # Align new data with the existing schema
        for col in existing_df.columns:
            if col not in df.columns:
                df[col] = None
        for col in df.columns:
            if col not in existing_df.columns:
                existing_df[col] = None

        # Drop entirely empty or all-NA columns from both DataFrames
        existing_df = existing_df.dropna(axis=1, how="all")
        df = df.dropna(axis=1, how="all")

        # Ensure column order matches
        df = df[existing_df.columns]

        # Combine the existing data with the new batch
        combined_df = pd.concat([existing_df, df], ignore_index=True)

        # Write the combined data back to the Parquet file
        table = pa.Table.from_pandas(combined_df)
        pq.write_table(table, file_name, compression="snappy")
    else:
        # Write the new batch to a new Parquet file
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, file_name, compression="snappy")

    print(f"Written {len(batch)} records to {file_name}")
