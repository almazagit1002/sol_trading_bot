import boto3
import pandas as pd
import json
from datetime import datetime, timedelta
import io

class S3JSONToDataFrame:
    def __init__(self, bucket_name, base_path='candles/15min/'):
        """
        Initialize the class with bucket name and base path.
        
        Args:
            bucket_name (str): The name of the S3 bucket
            base_path (str): The base path in the bucket where files are stored
        """
        self.s3_client = boto3.client('s3')
        self.bucket_name = bucket_name
        self.base_path = base_path
        
    def _construct_path(self, year, month, day):
        """
        Construct the path for a specific date.
        
        Args:
            year (int): Year
            month (int): Month
            day (int): Day
            
        Returns:
            str: The constructed path
        """
        return f"{self.base_path}year={year}/month={month:02d}/day={day:02d}/"
    
    def _list_files(self, prefix):
        """
        List files in a specific path.
        
        Args:
            prefix (str): The prefix to list files from
            
        Returns:
            list: List of file keys
        """
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            return []
        except Exception as e:
            print(f"Error listing files: {e}")
            return []
    
    def _get_previous_date(self, year, month, day):
        """
        Get the previous date.
        
        Args:
            year (int): Year
            month (int): Month
            day (int): Day
            
        Returns:
            tuple: (year, month, day) for the previous date
        """
        current_date = datetime(year, month, day)
        previous_date = current_date - timedelta(days=1)
        print(previous_date.year, previous_date.month, previous_date.day)
        return previous_date.year, previous_date.month, previous_date.day
    
    def _get_file_content(self, file_key):
        """
        Get the content of a file.
        
        Args:
            file_key (str): The key of the file to read
            
        Returns:
            dict: The parsed JSON content
        """
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=file_key)
            content = response['Body'].read().decode('utf-8')
            return json.loads(content)
        except Exception as e:
            print(f"Error reading file {file_key}: {e}")
            return None
    
    def get_last_n_files(self, n=20, specific_date=None):
        """
        Get the last n files from the S3 bucket.
        
        Args:
            n (int): Number of files to retrieve
            specific_date (tuple): Optional (year, month, day) to start from
            
        Returns:
            list: List of file contents
        """
        files_content = []
        files_to_fetch = n
        
        # Start from specified date or today
        if specific_date:
            year, month, day = specific_date
        else:
            now = datetime.now()
            year, month, day = now.year, now.month, now.day
        
        while files_to_fetch > 0:
            path = self._construct_path(year, month, day)
            files = self._list_files(path)
            
            # Sort files by name (assuming they have timestamp in the name)
            files.sort(reverse=True)
            
            for file_key in files:
                if files_to_fetch <= 0:
                    break
                    
                content = self._get_file_content(file_key)
                if content:
                    files_content.append(content)
                    files_to_fetch -= 1
            
            # If we still need more files and have processed all files in current day
            if files_to_fetch > 0 and len(files) > 0:
                # Move to previous day
                year, month, day = self._get_previous_date(year, month, day)
            elif files_to_fetch > 0 and len(files) == 0:
                # No files in current day, move to previous day
                year, month, day = self._get_previous_date(year, month, day)
            else:
                # We have enough files
                break
                
            # Safety check to avoid infinite loops
            if len(files_content) + files_to_fetch > n * 2:
                print(f"Warning: Could only retrieve {len(files_content)} files")
                break
        
        return files_content
    
    def create_dataframe(self, n=20, specific_date=None):
        """
        Create a DataFrame from the last n files.
        
        Args:
            n (int): Number of files to retrieve
            specific_date (tuple): Optional (year, month, day) to start from
            
        Returns:
            pandas.DataFrame: DataFrame with the data
        """
        files_content = self.get_last_n_files(n, specific_date)
        
        if not files_content:
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(files_content)
        
        # Clean up redundant time fields
        time_columns = ['candle_start', 'candle_end', 'timestamp', 'open_time', 'close_time', 'generated_at', 'volume']
        
        # Convert timestamp to datetime and set as index
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.set_index('timestamp')
        
        # Drop redundant time columns but keep timestamp as index
        for col in time_columns:
            if col in df.columns and col != 'timestamp':
                df = df.drop(columns=[col])
        
        # Sort by index to ensure chronological order
        df = df.sort_index()
        
        return df

# Example usage
if __name__ == "__main__":
    # Replace with your actual bucket name
    bucket_name = "sol-price-storage-ctech-bot"
    
    # Create an instance of the class
    s3_to_df = S3JSONToDataFrame(bucket_name)
    
    # Get DataFrame with last 20 files
    df = s3_to_df.create_dataframe(n=20)
    
    # Print the resulting DataFrame
    print(df.tail())
    
  