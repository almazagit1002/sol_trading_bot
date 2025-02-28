import json
import boto3
import os
import time
from datetime import datetime, timedelta
from decimal import Decimal

# Initialize clients
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# Constants
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', 'sol-price-storage-ctech-bot')
PROCESSED_FILES_TABLE = 'SolPriceProcessedFiles'
FILE_COUNT_TABLE = 'SolPriceFileCounter'  # New table for tracking file count
CANDLE_MINUTES = 15  # 15-minute candles
MIN_FILES_THRESHOLD = 5  # Minimum files required before generating candles

def lambda_handler(event, context):
    """
    Process S3 events and generate 15-minute candles.
    This function gets triggered when new price data files are uploaded to S3.
    It tracks files through DynamoDB to ensure each file is processed only once
    and maintains a running count of new files until the threshold is reached.
    """
    print(f"Received event: {json.dumps(event)}")
    
    # Track the number of new files added to process
    new_files_count = 0
    
    # Process each S3 event
    for record in event['Records']:
        # Get the S3 bucket and object key
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        
        print(f"Processing file: {key} from bucket: {bucket}")
        
        # Skip if not a price data file
        if not key.startswith('prices/'):
            print(f"Skipping non-price file: {key}")
            continue
        
        # Check if this file has been processed before
        if not is_file_processed(key):
            # Mark as processed
            mark_file_processed(key)
            new_files_count += 1
            print(f"Marked as processed: {key}")
        else:
            print(f"File already processed: {key}")
    
    print(f"New files detected: {new_files_count}")
    
    # Update the file counter in DynamoDB if we have new files
    if new_files_count > 0:
        current_count = increment_file_counter(new_files_count)
        print(f"Updated file counter. Current count: {current_count}")
        
        # If we've reached or exceeded the threshold, generate candles and reset counter
        if current_count >= MIN_FILES_THRESHOLD:
            print(f"File threshold reached ({current_count} >= {MIN_FILES_THRESHOLD}). Generating candles...")
            generate_candles()
            reset_file_counter()
            print("File counter reset after generating candles")
    
    return {
        'statusCode': 200,
        'body': json.dumps(f'Processed {new_files_count} new files')
    }

def is_file_processed(file_key):
    """Check if a file has been processed already."""
    try:
        table = dynamodb.Table(PROCESSED_FILES_TABLE)
        response = table.get_item(
            Key={
                'FileKey': file_key
            }
        )
        return 'Item' in response
    except Exception as e:
        print(f"Error checking if file is processed: {e}")
        # Assume not processed if there's an error
        return False

def mark_file_processed(file_key):
    """Mark a file as processed in DynamoDB."""
    try:
        table = dynamodb.Table(PROCESSED_FILES_TABLE)
        # Set expiration time to 7 days from now (to keep the table small)
        expiration_time = int(time.time() + (7 * 24 * 60 * 60))
        
        table.put_item(
            Item={
                'FileKey': file_key,
                'ProcessedTime': datetime.utcnow().isoformat(),
                'ExpirationTime': expiration_time
            }
        )
        return True
    except Exception as e:
        print(f"Error marking file as processed: {e}")
        return False

def increment_file_counter(count_to_add=1):
    """
    Increment the file counter in DynamoDB and return the new count.
    Creates the counter if it doesn't exist.
    """
    table = dynamodb.Table(FILE_COUNT_TABLE)
    counter_id = 'new_files_counter'
    
    try:
        # Try to update the existing counter
        try:
            response = table.update_item(
                Key={
                    'CounterId': counter_id
                },
                UpdateExpression='ADD FileCount :incr',
                ExpressionAttributeValues={
                    ':incr': count_to_add
                },
                ReturnValues='UPDATED_NEW'
            )
            
            # Return the new count
            return int(response['Attributes']['FileCount'])
        except Exception as e:
            # If update fails (likely because item doesn't exist), create it
            if 'ValidationException' in str(e) or 'ResourceNotFoundException' in str(e):
                print(f"Counter doesn't exist yet, creating it with initial value {count_to_add}")
                table.put_item(
                    Item={
                        'CounterId': counter_id,
                        'FileCount': count_to_add,
                        'LastUpdated': datetime.utcnow().isoformat()
                    }
                )
                return count_to_add
            else:
                # For other errors, log and re-raise
                print(f"Unexpected error updating counter: {e}")
                raise
                
    except Exception as e:
        print(f"Error incrementing file counter: {e}")
        # Return 0 to prevent candle generation in case of errors
        return 0

def reset_file_counter():
    """Reset the file counter after generating candles."""
    try:
        table = dynamodb.Table(FILE_COUNT_TABLE)
        
        table.update_item(
            Key={
                'CounterId': 'new_files_counter'
            },
            UpdateExpression='SET FileCount = :zero, LastUpdated = :now',
            ExpressionAttributeValues={
                ':zero': 0,
                ':now': datetime.utcnow().isoformat()
            }
        )
        return True
    except Exception as e:
        print(f"Error resetting file counter: {e}")
        return False

def generate_candles():
    """
    Generate 15-minute candles from the raw price data.
    This function will:
    1. Fetch recent price data
    2. Group into 15-minute periods
    3. Calculate OHLC (Open, High, Low, Close) values
    4. Store candle data in S3
    """
    print("Starting candle generation...")
    
    # Calculate the current and previous 15-minute periods
    now = datetime.utcnow()
    # Round down to the nearest 15-minute mark
    current_period_end = now.replace(minute=now.minute - (now.minute % CANDLE_MINUTES), second=0, microsecond=0)
    previous_period_end = current_period_end - timedelta(minutes=CANDLE_MINUTES)
    previous_period_start = previous_period_end - timedelta(minutes=CANDLE_MINUTES)
    
    print(f"Generating candle for period: {previous_period_start} to {previous_period_end}")
    
    # Get all price points in the previous period
    price_data = get_price_data_for_period(previous_period_start, previous_period_end)
    
    if not price_data:
        print("No price data found for the period")
        return
    
    # Calculate OHLC values
    open_price = price_data[0]['price']
    close_price = price_data[-1]['price']
    high_price = max(item['price'] for item in price_data)
    low_price = min(item['price'] for item in price_data)
    
    # Get actual timestamps for the open and close
    open_time = price_data[0]['timestamp']
    close_time = price_data[-1]['timestamp']
    
    # Create candle data with comprehensive timestamp information
    candle = {
        # Timestamp information
        'candle_start': previous_period_start.isoformat(),
        'candle_end': previous_period_end.isoformat(),
        'timestamp': previous_period_end.isoformat(),  # For compatibility with traditional candle format
        'unix_timestamp': int(previous_period_end.timestamp()),  # Unix timestamp in seconds
        'open_time': open_time.isoformat(),
        'close_time': close_time.isoformat(),
        
        # Candle data
        'period': f'{CANDLE_MINUTES}min',
        'open': open_price,
        'high': high_price,
        'low': low_price,
        'close': close_price,
        'volume': None,  # We don't have volume data from the current setup
        
        # Metadata
        'number_of_data_points': len(price_data),
        'generated_at': datetime.utcnow().isoformat()
    }
    
    # Save candle to S3
    year = previous_period_end.strftime("%Y")
    month = previous_period_end.strftime("%m")
    day = previous_period_end.strftime("%d")
    hour = previous_period_end.strftime("%H")
    minute = previous_period_end.strftime("%M")
    
    # Add unix timestamp to the filename for easier sorting and querying
    unix_ts = int(previous_period_end.timestamp())
    candle_key = f"candles/{CANDLE_MINUTES}min/year={year}/month={month}/day={day}/{hour}_{minute}_{unix_ts}.json"
    
    try:
        s3.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=candle_key,
            Body=json.dumps(candle, default=handle_decimal),
            ContentType="application/json"
        )
        print(f"Successfully generated and stored candle at {candle_key}")
        return True
    except Exception as e:
        print(f"Error storing candle: {e}")
        return False

def get_price_data_for_period(start_time, end_time):
    """
    Fetch all price data points within a given time period.
    """
    print(f"Fetching price data from {start_time} to {end_time}")
    
    # Convert datetime objects to required format for S3 prefix
    start_year = start_time.strftime("%Y")
    start_month = start_time.strftime("%m")
    start_day = start_time.strftime("%d")
    start_hour = start_time.strftime("%H")
    
    # We'll list objects in the hour containing our start_time
    # This is a simplification - for a full implementation, you might need to span multiple hours
    prefix = f"prices/year={start_year}/month={start_month}/day={start_day}/hour={start_hour}/"
    
    print(f"Listing objects with prefix: {prefix}")
    
    # List all objects with the prefix
    try:
        response = s3.list_objects_v2(
            Bucket=S3_BUCKET_NAME,
            Prefix=prefix
        )
        
        if 'Contents' not in response:
            print(f"No objects found with prefix: {prefix}")
            return []
        
        price_data = []
        
        # Process each object
        for obj in response['Contents']:
            key = obj['Key']
            
            # Get object data
            try:
                obj_response = s3.get_object(
                    Bucket=S3_BUCKET_NAME,
                    Key=key
                )
                
                file_content = obj_response['Body'].read().decode('utf-8')
                price_point = json.loads(file_content)
                
                # Parse timestamp and check if it's within our period
                timestamp = datetime.fromisoformat(price_point['timestamp'])
                
                if start_time <= timestamp <= end_time:
                    price_data.append({
                        'timestamp': timestamp,
                        'price': float(price_point['price'])
                    })
            except Exception as e:
                print(f"Error processing file {key}: {e}")
        
        # Sort by timestamp
        price_data.sort(key=lambda x: x['timestamp'])
        
        print(f"Found {len(price_data)} price points in the period")
        return price_data
    except Exception as e:
        print(f"Error in get_price_data_for_period: {e}")
        return []

def handle_decimal(obj):
    """Handler for serializing Decimal objects to JSON."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Object of type '%s' is not JSON serializable" % type(obj).__name__)