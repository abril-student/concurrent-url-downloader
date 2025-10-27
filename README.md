# Concurrent HTTP/HTTPS downloader

A multi-threaded downloader implemented using Python’s built-in `http.client` library.  
It supports **HTTP range requests**, **parallel downloads**, **resume on interruption**, and **SHA-256 integrity validation**.

## Features

- Concurrent download of large files via HTTP or HTTPS  
- Support for servers with `Accept-Ranges: bytes`  
- Automatic resume of partial downloads
- Adjustable number of workers and chunk size per request  
- Automatic file assembly from downloaded parts  
- Optional SHA-256 integrity verification  
- Clean removal of partial files after success  

## Requirements

Python 3.8 or later.

## Usage

```python downloader.py <url> [options]```

### Example command:
```
python downloader.py \
  "https://download.blender.org/peach/bigbuckbunny_movies/BigBuckBunny_320x180.mp4" \
  -w 8 \
  --chunk-size-mb 16 \
  --sha256 f78f39603e6774907f2faafabf26a667f4a6fc31769ec304a8a8f7c62d280508
```

### Example output:

```URL: https://download.blender.org/peach/bigbuckbunny_movies/BigBuckBunny_320x180.mp4
Content-Length: 64657027 bytes | Accept-Ranges: bytes
Parts: 16 | Chunk: ~4194304 bytes | Workers: 8
Done: BigBuckBunny_320x180.mp4 (64657027 bytes)
```
