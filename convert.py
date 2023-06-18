import pandas as pd
import gcsfs

try:
    print('Initializing Google Cloud Storage file system...')
    fs = gcsfs.GCSFileSystem(project='prime-byway-387902', token='C:/Users/muhfa/.google/credentials/google_credentials.json')

    print('Defining source and target paths...')
    source_path = 'gs://myproject1-delay-airline/corona-virus-report/day_wise.parquet'
    target_path = 'gs://myproject1-delay-airline/corona-virus-report/day_wise_modified.parquet'

    print('Reading Parquet file from Google Cloud Storage...')
    with fs.open(source_path, 'rb') as f:
        df = pd.read_parquet(f)

    print('Modifying column names...')
    df.columns = df.columns.str.replace('.', ' ')

    print('Writing modified DataFrame back to a new Parquet file in Google Cloud Storage...')
    with fs.open(target_path, 'wb') as f:
        df.to_parquet(f)
    
    print('Successfully completed!')

except Exception as e:
    print('An error occurred:', str(e))
