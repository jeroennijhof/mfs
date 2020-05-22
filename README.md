# Mirrored File System (MFS)
Keep your files in sync across multiple servers without a single point of failure.

## Usage
<pre>
./mfsd -h
Usage of ./mfsd:
  -i int
    	Sync interval in seconds (default 2)
  -p string
    	Path to files which need to be synced (default "/data")
  -s value
    	Server to connect to, define multiple for HA
  -t string
    	Authentication Token (default "token")
</pre>

## Try it
You need docker and docker-compose installed.<br>
After running the following you have 3 containers keeping /data in sync.

<pre>
./build.sh
docker-compose up
</pre>
