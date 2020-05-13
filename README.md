# Run API locally
    ConcurrierApi serve

# Run worker locally
    ConcurrierApi worker

# Run benchmarks
    ConcurrierApi bench -t download
    ConcurrierApi bench -t download -b 1 -o sequential

# Development
    pip install -e .'[dev]'

# Run redis
    docker run --name redis:5.0.7 -p 7001:6379 -d redis

# Test instance
    DownloadImage
    {"imageUrls": ["https://farm7.staticflickr.com/3175/2549770743_71d73d1ba5_o.jpg"], "folder": "imgs"}
