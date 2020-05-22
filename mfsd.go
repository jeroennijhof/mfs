package main

import (
	"fmt"
	"os"
	"flag"
	"time"
	"github.com/fsnotify/fsnotify"
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

var files = make(map[string]string)

func main() {
	var path string
	var interval int
	var token string
	var servers strArray

	flag.StringVar(&path, "p", "/data", "Path to files which need to be synced")
	flag.IntVar(&interval, "i", 2, "Sync interval in seconds")
	flag.StringVar(&token, "t", "token", "Authentication Token")
	flag.Var(&servers, "s", "Server to connect to, define multiple for HA")
	flag.Parse()

	watcher, err := fsnotify.NewWatcher()
	done := make(chan bool)
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Op&fsnotify.Write == fsnotify.Write {
					files[event.Name] = mfs.Sync
				}
				if event.Op&fsnotify.Remove == fsnotify.Remove {
					files[event.Name] = mfs.Remove
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				fmt.Println("error:", err)
			}
		}
	}()

	mfsClient := mfs.Client(servers, token, interval)
	go func(interval int) {
		for {
			time.Sleep(time.Duration(interval) * time.Second)
			mfsClient.Send(files)
                        files = make(map[string]string)
		}
	}(interval)
	defer mfsClient.Close()

	err = watcher.Add(path)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	<-done
}
