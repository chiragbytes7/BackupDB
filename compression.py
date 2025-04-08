
# we have to open the directory here , compress it and send it accross to the hdfs
import os
import zstandard as zstd
import subprocess

def compressmysql(input_dir, output_dir, compression_level = 3):
    dir_name = os.path.basename(input_dir.rstrip("/"))  
    output_file = os.path.join(output_dir, f"{dir_name}.zst")
    compressor = zstd.ZstdCompressor(level = compression_level)
    with open(output_file,"wb") as f_out:   
        with compressor.stream_writer(f_out) as compressor_stream:
            for root, subdir, files in os.walk(input_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    relative_path = os.path.relpath(file_path, input_dir)

                    file_size = os.path.getsize(file_path)
                    header = f"{relative_path}:{file_size}\n".encode()
                    compressor_stream.write(header)

                    with open(file_path, "rb") as f_in:
                        while chunk := f_in.read(1024*1024):
                            compressor_stream.write(chunk)
    print("compression done")
    return output_file

def compressmongo():
    