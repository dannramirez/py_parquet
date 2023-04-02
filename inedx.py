import thriftpy2
from thriftpy2.protocol import TCompactProtocol
from thriftpy2.transport import TMemoryBuffer

def read_parquet_footer(file_path):
    with open(file_path, 'rb') as f:
        f.seek(-8, 2)  # Move back 8 bytes from the end of the file
        metadata_size = int.from_bytes(f.read(4), 'little')
        f.seek(-(metadata_size + 4), 1)  # Move to the start of the footer metadata
        metadata_bytes = f.read(metadata_size)
        
    return metadata_bytes

# Load Parquet Thrift definition
parquet_thrift = thriftpy2.load("parquet.thrift", module_name="parquet_thrift")

file_path = "path/to/your/problematic/parquet/file.parquet"
metadata_bytes = read_parquet_footer(file_path)

# Deserialize the Thrift metadata
transport = TMemoryBuffer(metadata_bytes)
protocol = TCompactProtocol(transport)
metadata = parquet_thrift.FileMetaData()
metadata.read(protocol)

print(metadata)
