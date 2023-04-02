import pyarrow.parquet as pq
import pyarrow as pa
import thriftpy2
from thriftpy2.protocol import TCompactProtocol
from thriftpy2.transport import TMemoryBuffer
from thriftpy2.thrift import TType

file_path = "path/to/your/parquet/file.parquet"

# Load the Parquet Thrift definition
parquet_thrift = thriftpy2.load("parquet.thrift", "parquet")

try:
    # Read the Parquet file
    table = pq.read_table(file_path)
    print("Parquet file read successfully.")
except pa.ArrowInvalid as e:
    if "ParquetException: Couldn't deserialize thrift msg" in str(e):
        # Read the raw file
        with open(file_path, "rb") as f:
            raw_data = f.read()

        # Find the position of the magic bytes
        magic = b"PAR1"
        pos = raw_data.find(magic)

        if pos != -1:
            # Extract the metadata size
            metadata_size_bytes = raw_data[pos - 4:pos]
            metadata_size = int.from_bytes(metadata_size_bytes, byteorder="little")

            # Extract and parse the metadata
            metadata_buffer = raw_data[pos - metadata_size - 4:pos]
            transport = TMemoryBuffer(metadata_buffer)
            proto = TCompactProtocol(transport)
            metadata = parquet_thrift.FileMetaData()
            metadata.read(proto)

            # Print the Thrift metadata
            print("Thrift metadata:")
            print(metadata)
        else:
            print("Error: Couldn't find the magic bytes in the Parquet file.")
    else:
        print(f"Error reading Parquet file (ArrowInvalid): {e}")
except pa.ArrowIOError as e:
    print(f"Error reading Parquet file (ArrowIOError): {e}")
except pa.ArrowTypeError as e:
    print(f"Error reading Parquet file (ArrowTypeError): {e}")
except Exception as e:
    print(f"Error reading Parquet file (other error): {e}")
