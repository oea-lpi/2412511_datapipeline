import datetime
import logging
import os
from pathlib import Path
import shutil

import ginsapy.giutility.connect.PyQStationConnectWin as Qstation
import matplotlib.dates as mpdt
import numpy as np
import pandas as pd
from scipy.io import savemat

from gantner_operations.GInsConnection import GInsConnection


logger = logging.getLogger(__name__)

class DataConverterUDBF:
    """ 
    Supply utility to extract all information from a .dat file and convert it into an output file.
    """
    def __init__(self, raw_file, path_dir, path_udbf, round_factor, index_timestamp=0):
        self.raw_file = raw_file 
        self.path_dir = path_dir
        self.path_udbf = path_udbf
        self.data = None    
        self.df_stats = None
        self.index_timestamp = index_timestamp
        self.index_unit_time = None
        self.date_strings = []
        self.date_num = []
        self.channel_names = None
        self.sample_rate = None
        self.channel_unit = []
        self.df = None
        self.time_relativ_vector = None
        self.round_factor = round_factor

    def check_readability_of_data_file(self) -> int:
        """
        Check feasibility of input data before analysis:
        If the file is larger than FILESIZE_THRESHOLD bytes, treat as healthy (0),
        otherwise unhealthy (1).

        Returns:
            Integer: 0 if healthy, 1 if unhealthy
        """
        file_path = Path(self.path_dir) / self.raw_file

        reference_100hz = 35 * 1024**2

        threshhold_lower = reference_100hz * 0.9
        threshhold_upper = reference_100hz * 1.1

        try:
            size = file_path.stat().st_size
        except Exception:
            size = 0

        return 0 if threshhold_lower <= size <= threshhold_upper else 1

        
    def read_udbf_file(self) -> bool:
        """
        Connect and extract info from .dat file.

        Output: True, Fills .data parameter of classobject and creates a .time_relativ_vector
        """
        with GInsConnection() as conn:
            # Connect and extract file info.
            conn.init_file(self.path_udbf)
            raw_num = conn.read_channel_count()
            try:
                self.channel_num = int(raw_num)
            except (ValueError, TypeError):
                raise ValueError(f"Invalid channel count from GInsConnection: {raw_num!r}")
            channel_names = [conn.read_index_name(i) for i in range(self.channel_num)]
            self.channel_names = channel_names        
            self.sample_rate = conn.read_sample_rate()
            self.channel_unit = conn.read_channels_unit()

            # Import file info into numpy matrix, dat_file[row,column].
            try:
                dat_file=Qstation.read_gins_dat(conn)
                self.data = dat_file
            except IOError as e:
                logger.warning(f"File {self.raw_file} could not be imported.")
                raise e
            except Exception as e:
                logger.warning(f"File {self.raw_file} could not be imported.")
                raise

            # Construct a relativ time vector
            time_abs_start = 0
            time_step = 1/self.sample_rate
            num_rows_dat_file = dat_file.shape[0]
            time_end = num_rows_dat_file / self.sample_rate # In second
            time_relativ_vector_row = np.arange(time_abs_start, time_end, time_step)
            time_relativ_vector = time_relativ_vector_row.reshape(-1,1)
            self.time_relativ_vector = time_relativ_vector
        return True

    def ole2datetime(self, oledt: int) -> datetime.datetime:
        """ 
        Helper method to convert OLE to datetime.
        """
        OLE_TIME_ZERO = datetime.datetime(1899, 12, 30, 0, 0, 0) # (Object Linking and Embedding) 
        return OLE_TIME_ZERO + datetime.timedelta(days=float(oledt))

    def normalize_datetime(self, dt: datetime.datetime) -> datetime.datetime:
        """ 
        Helper method to normalize the datetime.
        """
        if dt.second is None:
            dt = dt.replace(second=0)
        if dt.microsecond is None:
            dt = dt.replace(micorsecond=0)
        return dt

    def date_converter(self) -> bool:
        """ 
        Creates a nomalized and converted time columns from the .dat file.
        Columns are date in %Y-%m-%d, time in %H:%M:%S, milliseconds

        Output: True, Fills .df_time parameter
        """
        dat_file = self.data 
        index_timestamp = 0
        date_strings=[self.ole2datetime(oledt) for oledt in dat_file[:,index_timestamp]]
        date_num=mpdt.date2num(date_strings)
        date_strings = [self.normalize_datetime(dt) for dt in date_strings]
        self.date_strings = date_strings
        self.date_num = date_num
        df_time = pd.DataFrame({'Datetime': date_strings})
        df_time['Datum'] = df_time['Datetime'].dt.strftime('%Y-%m-%d')
        df_time['Uhrzeit'] = df_time['Datetime'].dt.strftime('%H:%M:%S')
        df_time['Millisekunden'] = df_time['Datetime'].dt.microsecond // 1000
        df_time.drop(columns=['Datetime'], inplace=True)
        self.df_time = df_time
        return True

    def save_as_mat(self, output_dir: str) -> bool:
        """ 
        Converts info from .dat file into a .mat file.
        Contains relative_time, absolute_time, date, time, millisecond, values.

        Output: True, Creates a .mat file
        """
        mat_dict = {}
        name_of_mat = os.path.join(output_dir, self.raw_file.replace('.dat', '.mat'))
        try:
            assert self.data.shape[1] == len(self.channel_names)
            for idx, name in enumerate(self.channel_names):
                if idx == 0:
                    timestamp_data = {'relative_time': self.time_relativ_vector,
                                      'absolut_time':self.data[:,idx].reshape(-1,1),
                                      'date':self.df_time['Datum'].values.astype('U'),
                                      'time':self.df_time['Uhrzeit'].values.astype('U'),
                                      'millisecond':self.df_time['Millisekunden'].values}
                    mat_dict[name] = timestamp_data
                else:    
                    mat_dict[name] = self.data[:,idx].reshape(-1,1)

            savemat(name_of_mat, mat_dict)
            logger.debug(f"MAT file created: {name_of_mat}")
        except Exception as e:
            logger.warning(f"Could not create a .mat file for {self.raw_file}: {e}")
        return True
    
    def save_statistics_csv(self, finished_dir: str) -> bool:
        """ 
        Compute basic stats for each sensor channel and save them as a CSV.
        Uses the channel_names and data to calculate the folling stats:
        mean, median, min, max — all rounded by self.round_factor

        Returns:
            True: If CSV file was created
        """
        import re
        from datetime import datetime
        LPI_PATTERN = re.compile(r'_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})')

        try:
            stats_rows = []

            match = LPI_PATTERN.search(self.raw_file)
            if match:
                ts_str = match.group(1)  # e.g. "2025-06-19_12-20-00"
                ts = datetime.strptime(ts_str, "%Y-%m-%d_%H-%M-%S")
                aligned = (ts.minute % 10 == 0 and ts.second == 0)
            else:
                aligned = False

            # Skip the first 10 seconds of the values to avoid including 0's, which may occur on restart of the system and distort the CSV.
            skip = 0
            if not aligned:
                skip = int(self.sample_rate * 10)

            # Skip index 0 (timestamp / OLE date)
            for idx, name in enumerate(self.channel_names):
                if idx == 0:
                    continue
            
                values = self.data[:, idx]
                if skip > 0 and len(values) <= skip:
                    logger.warning(f"Not enough samples in {name} to skip first 10s, dropping channel.")
                    continue
                trimmed = values[skip:] if skip > 0 else values

                mean = round(np.mean(trimmed), self.round_factor)
                median = round(np.median(trimmed), self.round_factor)
                vmin = round(np.min(trimmed), self.round_factor)
                vmax = round(np.max(trimmed), self.round_factor)

                stats_rows.append({
                    'Sensor':     name,
                    'Mean':       mean,
                    'Median':     median,
                    'Minimum':    vmin,
                    'Maximum':    vmax
                })

            df_stats = pd.DataFrame(stats_rows)

            # Determine output path
            stats_filename = self.raw_file.replace('.dat', '_stats.csv')
            if finished_dir:
                stats_path = os.path.join(finished_dir, stats_filename)
            else:
                stats_path = stats_filename

            df_stats.to_csv(stats_path, index=False)
            self.df_stats = df_stats
            logger.debug(f"Statistics CSV created: {stats_path}")
            return True
        except Exception as e:
            logger.warning(f"Couldn't write stats CSV for {self.raw_file}: {e}")  
            raise  
    
    def move_to_finished(self, finished_dir: str) -> bool:
        """ 
        Move the original DAT file into a 'finished' directory.

        Returns:
            True: If CSV file was created
        """
        # Ensure the source file exists
        full_path = os.path.join(self.path_dir, self.raw_file)
        if not os.path.isfile(full_path):
            logger.warning(f"Source file not found: {self.raw_file}")
            raise FileNotFoundError(self.raw_file)

        basename = os.path.basename(self.raw_file)
        dest_path = os.path.join(finished_dir, basename)
        try:
            shutil.move(full_path, dest_path)
            logger.debug(f"Moved {self.raw_file} to {dest_path}")
            # Update internal reference so future calls know the new path
            self.path_dir = finished_dir
            return True
        except Exception as e:
            logger.warning(f"Failed to move {self.raw_file} to {dest_path}: {e}")
            raise