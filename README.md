# Mirrored File System (MFS)
Keep your files in sync across multiple servers without a single point of failure.

***NOTE: run at least two mfsd instances as sync worker to garantee no single point of failure***

## Architecture
MFS is using [NATS](https://nats.io/) as messaging bus. You need a running NATS environment before you can use MFS.

By default a MFS instance (except sync worker) will start with an initial sync, after sync it will listens for file changes and send the changes to all other MFS instances.

### Sync Worker
This is a special MFS instance which will not sync on startup but will listen for file changes and sync requests from other MFS instances. If a sync requests is received it will send all files to the MFS instance who made the request.

It is really important that this instance is up to date before you start MFS with the sync worker option.

## Usage
<pre>
./mfsd -h
Usage of ./mfsd:
  -S   This instance will act as sync worker
  -i int
       Sync interval in seconds (default 2)
  -p string
       Path to files which need to be synced (default "/data")
  -s value
       NATS <host>:<port>, define multiple for high availability
  -t string
       NATS Authentication Token (default "token")
</pre>

## Try it
You need docker and docker-compose installed.<br>
After running the following you have 3 containers keeping /data in sync.

<pre>
./build.sh
docker-compose up
</pre>
