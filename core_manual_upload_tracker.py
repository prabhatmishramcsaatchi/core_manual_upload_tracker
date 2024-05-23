import json
import pandas as pd
import os
import boto3  
from io import StringIO
from sqlalchemy import create_engine, text
from io import BytesIO 
# Environment variables   
CSV_URL = os.environ['CSV_URL']
S3_BUCKET = 'wikitablescrapexample'
S3_FOLDER = 'amazon_sprinklr_pull/core_manual_tracker/'
key = 'amazon_sprinklr_pull/result/master_table.csv'
           
DATABASE_URL = "postgresql://tableausprinklr:HiXy074Hi@prod-fluency-sprinklr-db-eu2.cpmfagf94hdi.eu-west-2.rds.amazonaws.com:5432/sprinklrproddb"
table_name = 'sprinklr_table'  # Specify your PostgreSQL table name here
region_mapping_key = 'amazon_sprinklr_pull/mappingandbenchmark/countrymapping.xlsx'

                 
s3 = boto3.client('s3') 

 
def read_excel_from_s3(bucket_name, key):
    # Download the Excel file from S3
    response = s3.get_object(Bucket=bucket_name, Key=key)
    excel_content = response['Body'].read()

    # Read the Excel content into a pandas DataFrame
    df1 = pd.read_excel(BytesIO(excel_content), usecols=['Country', 'Region'])
    return df1
def lambda_handler(event, context):
    # Process the data
    remove_manual_tracker_rows(table_name)
    #df_sql = fetch_data_from_postgresql(table_name)
    df_processed = process_data(CSV_URL)
    df_sql = read_csv_from_s3(S3_BUCKET, key)

    df_sql['split_permalink'] = df_sql['PERMALINK'].str.rsplit('/', n=2).str[-2]
    split_permalink_set = set(df_sql['split_permalink'])

      
       
    
    # Splitting the 'PERMALINK' column in df_processed and extracting the desired part
    df_processed['split_permalink'] = df_processed['PERMALINK'].str.rsplit('/', n=2).str[-2]
    
 
# Keeping rows in df_processed where 'split_permalink' value is NOT found in df_sql['split_permalink']
    df_processed_filtered = df_processed[~df_processed['split_permalink'].isin(split_permalink_set)]
    df_processed_filtered = df_processed_filtered.drop(columns=['split_permalink'])
    
    # Upload the processed DataFrame to S3
    file_name = 'processed_data.csv'  # You could add logic to make this name dynamic based on the date or other factors
    s3_path = f"{S3_FOLDER}{file_name}"
    upload_to_s3(df_processed_filtered, S3_BUCKET, s3_path)
     
    # Return the success response
    return {
        'statusCode': 200,
        'body': json.dumps(f'Successfully processed and uploaded {len(df_processed)} rows to {S3_BUCKET}/{s3_path}')
    }
        
def process_data(url):
    
    region_df = read_excel_from_s3(S3_BUCKET, region_mapping_key)
    
    # Read and process the CSV data from the URL
    
    df = pd.read_csv(url, skiprows=[0, 1, 3], parse_dates=['Row - Last Updated Date', 'Post Date*'],
                     dayfirst=True, infer_datetime_format=True).dropna(axis=1, how='all')
    df = df[df['Valid Post'] == True]
    df = df[df['Upload to CORE'] == True]
    df['Total Impressions'] = df['Organic Impressions*']
    mask = df['Engagements*'].notna() & df['Total Impressions'].notna() & df['Engagement Rate (%)*'].isna()
    df.loc[mask, 'Engagement Rate (%)*'] = (df['Engagements*'] / df['Total Impressions']) * 100
    df.rename(columns={
        'Organic Video Views to 3 secs':'Video Views',
        'Shares (Total)':'Shares',
        'Comments (Total)':'Comments',
        'Likes (Total)':'Likes',
        'Platform*':'Platform', 
        'Engagements*':'Engagements',
        'Organic Impressions*':'Organic Impressions',
        'Tier 1 Event':'Tier 1 Event?',
        'Post Date*': 'Published Date',
        'Account*': 'Account',
        'Permalink*': 'PERMALINK',
        'Post Format*':'Post Format',
        'Post Text (Original Language)*':'Post',
        'Reputational Topic*': 'Reputational Topic',
        'Content Source*': 'Content Source',
        'Amazon Owned?': 'amazon_owned',
        'If this is an XGC post, what kind is it?':'If this is an XGC post, what kind is it?'
        
    }, inplace=True) 
      
    df['total video views'] = df['Video Views']
    df['Total reach'] = df['Organic Reach']
    df['is_paiddata']=0
    df['Country'] = df['Account'].str[-2:]
    df['is_manual_tracker']=1
    
# Add a 'Region' column with all values set to 'Europe'
    df = pd.merge(df, region_df, on='Country', how='left')
    df['row_number'] = df['Published Date'].dt.strftime('%Y-%m-%d') + df['Account'] + df['PERMALINK'] + "Manual_entry"
    df['Pull Date'] = df['Published Date'].dt.strftime('%Y-%m-%d')
    df.loc[df['Post Format'] == 'Video / IG Reel', 'Post Format'] = 'Video'
    # df['If yes who made it?']='Manual Entry'
    # df['Content Category Type']= 'Manual Entry'
    
  
     
    return df
    
    
    
def fetch_data_from_postgresql(table_name):
    try:
        # Create a connection object
        engine = create_engine(DATABASE_URL)
        connection = engine.connect()

        # Use pandas to read data from the specified PostgreSQL table
        df_sql = pd.read_sql(table_name, connection)
        
        # Close the connection
        connection.close()
        
        return df_sql
    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame() 
        
def remove_manual_tracker_rows(table_name):
    try:
        # Create an engine object
        engine = create_engine(DATABASE_URL)

        # Establish a connection
        connection = engine.connect()

        # Begin a transaction
        transaction = connection.begin()
        try:
            # Prepare the DELETE statement
            delete_statement = text(f"DELETE FROM {table_name} WHERE is_manual_tracker = 1")

            # Execute the DELETE statement
            connection.execute(delete_statement)

            # Commit the transaction
            transaction.commit()
            print(f"Rows where 'is_manual_tracker' = 1 have been successfully removed from {table_name}.")
        except:
            # Rollback the transaction in case of error
            transaction.rollback()
            raise
    except Exception as e:
        print(f"An error occurred while removing 'manual tracker' rows: {e}")
    finally:
        # Ensure the connection is closed
        connection.close()

def read_csv_from_s3(bucket, key, encoding='utf-8', usecols=None):
    """
    Reads a CSV file from an S3 bucket into a pandas DataFrame, using the specified encoding.
    Optionally reads only specified columns to save memory.

    Parameters:
    - bucket (str): The name of the S3 bucket.
    - key (str): The key of the CSV file in the S3 bucket.
    - encoding (str): The encoding to use for reading the CSV file. Defaults to 'utf-8'.
    - usecols (list, optional): Specifies a list of column names to read. Default is None, which reads all columns.

    Returns:
    - pd.DataFrame: A DataFrame containing the CSV data.
    """
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(BytesIO(obj['Body'].read()), encoding=encoding, usecols=usecols)
    return df
   
def upload_to_s3(df, bucket, path):
    # Convert the DataFrame to a CSV string
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    # Upload the CSV string to S3
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, path).put(Body=csv_buffer.getvalue())
    print(f'Successfully uploaded to {bucket}/{path}')

# Make sure the required permissions are set for the Lambda function to access S3
