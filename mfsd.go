package main

import (
	"log"
	"flag"
	"time"
	"github.com/radovskyb/watcher"
	"mfs/mfs"
)

type strArray []string
func (i *strArray) String() string {
	return "localhost:4443"
}
func (i *strArray) Set(value string) error {
	*i = append(*i, value)
	return nil
}

var files = make(map[string]watcher.Event)

func main() {
	var path string
	var interval int
	var token string
	var servers strArray
	var sync bool

	flag.StringVar(&path, "p", "/data", "Path to files which need to be synced")
	flag.IntVar(&interval, "i", 2, "Sync interval in seconds")
	flag.StringVar(&token, "t", "token", "Authentication Token")
	flag.Var(&servers, "s", "Server to connect to, define multiple for HA")
	flag.BoolVar(&sync, "S", false, "This instance will act as sync master")
	flag.Parse()

	w := watcher.New()
	go func() {
		for {
			select {
			case event := <-w.Event:
				files[event.Path] = event
			case err := <-w.Error:
				log.Println("mfsd error:", err)
			case <-w.Closed:
				return
			}
		}
	}()

	mfsClient := mfs.Client(servers, token, interval, sync, path)
	go func(interval int) {
		for {
			time.Sleep(time.Duration(interval) * time.Second)
			mfsClient.Send(files)
			files = make(map[string]watcher.Event)
		}
	}(interval)
	defer mfsClient.Close()

	if err := w.AddRecursive(path); err != nil {
		log.Fatalln("mfsd critical:", err)
	}

	if err := w.Start(time.Millisecond * 100); err != nil {
		log.Fatalln("mfsd critical:", err)
	}
}
