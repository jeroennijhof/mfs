package mfs

import (
	"log"
	"strings"
	"os"
	"io/ioutil"
	"syscall"
	"time"
	"path/filepath"
	"github.com/radovskyb/watcher"
	nats "github.com/nats-io/nats.go"
)

type MFS struct {
	Url string
	Ec *nats.EncodedConn
}

type MFSFile struct {
	Type string
	Hostname string
	TargetHost string
	Path string
	OldPath string
	Mode uint32
	Uid uint32
	Gid uint32
	Content []byte
}

type MFSSync struct {
	Type string
	Hostname string
	Status string
}

const Sync = "SYNC"
const Request = "REQUEST"
const Response = "RESPONSE"
var lock = make(map[string]bool)

// Helper recv functions
func Skip(mfsFile *MFSFile) bool {
	hostname, _ := os.Hostname()
	if hostname == mfsFile.Hostname {
		return true
	}
	if mfsFile.TargetHost != "" && mfsFile.TargetHost != hostname {
		return true
	}
	return false
}

func CreateDir(mfsFile *MFSFile) {
	log.Println("mfs: creating directory", mfsFile.Path)
	if Skip(mfsFile) {
		log.Println("mfs: skip creating directory")
		return
	}
	err := os.Mkdir(mfsFile.Path, os.FileMode(mfsFile.Mode))
	if err != nil {
		log.Println("mfs error:", err)
		return
	}
	err = os.Chown(mfsFile.Path, int(mfsFile.Uid), int(mfsFile.Gid))
	if err != nil {
		log.Println("mfs error:", err)
	}
}

func WriteFile(mfsFile *MFSFile) {
	log.Println("mfs: writing file", mfsFile.Path)
	if Skip(mfsFile) {
		log.Println("mfs: skip writing file")
		return
	}
	err := ioutil.WriteFile(mfsFile.Path, mfsFile.Content, os.FileMode(mfsFile.Mode))
	if err != nil {
		log.Println("mfs error:", err)
		return
	}
	err = os.Chown(mfsFile.Path, int(mfsFile.Uid), int(mfsFile.Gid))
	if err != nil {
		log.Println("mfs error:", err)
	}
}

func RemoveFile(mfsFile *MFSFile) {
	log.Println("mfs: removing file", mfsFile.Path)
	if Skip(mfsFile) {
		log.Println("mfs: skip removing file")
		return
	}
	err := os.Remove(mfsFile.Path)
	if err != nil {
		log.Println("mfs error:", err)
	}
}

func MoveFile(mfsFile *MFSFile) {
	log.Println("mfs: moving file", mfsFile.OldPath, "to", mfsFile.Path)
	if Skip(mfsFile) {
		log.Println("mfs: skip moving file")
		return
	}
	err := os.Rename(mfsFile.OldPath, mfsFile.Path)
	if err != nil {
		log.Println("mfs error:", err)
	}
}

func ChmodFile(mfsFile *MFSFile) {
	log.Println("mfs: chmod file", mfsFile.Path)
	if Skip(mfsFile) {
		log.Println("mfs: skip chmod file")
		return
	}
	err := os.Chmod(mfsFile.Path, os.FileMode(mfsFile.Mode))
	if err != nil {
		log.Println("mfs error:", err)
	}
}

// Helper send functions
func SendDir(ec *nats.EncodedConn, event watcher.Event, target string) {
	hostname, _ := os.Hostname()
	if !event.FileInfo.IsDir() {
		return
	}
	stat, _ := event.FileInfo.Sys().(*syscall.Stat_t)

	mfsFile := &MFSFile{Type: watcher.Create.String(), Hostname: hostname, TargetHost: target,
		Path: event.Path, Mode: stat.Mode, Uid: stat.Uid, Gid: stat.Gid}
	ec.Publish(watcher.Create.String(), mfsFile)
}

func SendFile(ec *nats.EncodedConn, event watcher.Event, target string) {
	hostname, _ := os.Hostname()
	if event.FileInfo.IsDir() {
		return
	}
	stat, _ := event.FileInfo.Sys().(*syscall.Stat_t)
	content, err := ioutil.ReadFile(event.Path)
	if err != nil {
		log.Println("mfs error:", err)
		return
	}

	mfsFile := &MFSFile{Type: watcher.Write.String(), Hostname: hostname, TargetHost: target,
		Path: event.Path, Mode: stat.Mode, Uid: stat.Uid, Gid: stat.Gid, Content: content}
	ec.Publish(watcher.Write.String(), mfsFile)
}

func SendRemoveFile(ec *nats.EncodedConn, event watcher.Event) {
	hostname, _ := os.Hostname()

	mfsFile := &MFSFile{Type: watcher.Remove.String(), Hostname: hostname, Path: event.Path}
	ec.Publish(watcher.Remove.String(), mfsFile)
}

func SendMoveFile(ec *nats.EncodedConn, event watcher.Event) {
	hostname, _ := os.Hostname()

	mfsFile := &MFSFile{Type: watcher.Move.String(), Hostname: hostname, Path: event.Path,
		OldPath: event.OldPath}
	ec.Publish(watcher.Move.String(), mfsFile)
}

func SendChmodFile(ec *nats.EncodedConn, event watcher.Event) {
	hostname, _ := os.Hostname()
	stat, _ := event.FileInfo.Sys().(*syscall.Stat_t)

	mfsFile := &MFSFile{Type: watcher.Chmod.String(), Hostname: hostname, Path: event.Path,
		Mode: stat.Mode}
	ec.Publish(watcher.Chmod.String(), mfsFile)
}

// Helper init sync functions
func SyncFiles(ec *nats.EncodedConn, mfsSync *MFSSync, path string) {
	// Get all directories and files and send them to client
	err := filepath.Walk(path,
		func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		event := watcher.Event{Path: path, FileInfo: info}
		if info.IsDir() {
			SendDir(ec, event, mfsSync.Hostname)
		} else {
			SendFile(ec, event, mfsSync.Hostname)
		}
		return nil
	})
	if err != nil {
		log.Println(err)
	}
	mfsSyncReply := &MFSSync{Type: Response, Hostname: mfsSync.Hostname, Status: "done"}
	ec.Publish(Sync, mfsSyncReply)
}

func SyncClient(ec *nats.EncodedConn) {
	hostname, _ := os.Hostname()
	status := "waiting"

	subSync, _ := ec.Subscribe(Sync, func(mfsSync *MFSSync) {
		if mfsSync.Type == Response &&  mfsSync.Hostname == hostname {
			status = mfsSync.Status
		}
	})

	log.Println("mfs: init sync started")
	mfsSync := &MFSSync{Type: Request, Hostname: hostname}
	ec.Publish(Sync, mfsSync)

	//Wait for Sync message complete
	for {
		time.Sleep(time.Duration(1) * time.Second)
		if status != "waiting" {
			log.Println("mfs: init sync done")
			break
		}
	}
	subSync.Unsubscribe()
}

// Public functions
func Client(servers []string, token string, interval int, sync bool, path string) MFS {
	url := strings.Join(servers, ", nats://")
	url = strings.Join([]string{"nats://", url}, "")

	nc, err := nats.Connect(url, nats.Token(token))
	if err != nil {
		log.Fatalln("mfs critical:", err)
	}
	ec, err := nats.NewEncodedConn(nc, nats.JSON_ENCODER)
	if err != nil {
		log.Fatalln("mfs critical:", err)
	}

	mfsClient := MFS {Url: url, Ec: ec}
	ec.Subscribe(watcher.Create.String(), func(mfsFile *MFSFile) {
		lock[mfsFile.Path] = true
		CreateDir(mfsFile)
		go func(interval int) {
			time.Sleep(time.Duration(interval) * time.Second)
			delete(lock, mfsFile.Path)
		}(interval)
	})
	ec.Subscribe(watcher.Write.String(), func(mfsFile *MFSFile) {
		lock[mfsFile.Path] = true
		WriteFile(mfsFile)
		go func(interval int) {
			time.Sleep(time.Duration(interval) * time.Second)
			delete(lock, mfsFile.Path)
		}(interval)
	})
	ec.Subscribe(watcher.Remove.String(), func(mfsFile *MFSFile) {
		lock[mfsFile.Path] = true
		RemoveFile(mfsFile)
		go func(interval int) {
			time.Sleep(time.Duration(interval) * time.Second)
			delete(lock, mfsFile.Path)
		}(interval)
	})
	ec.Subscribe(watcher.Move.String(), func(mfsFile *MFSFile) {
		lock[mfsFile.Path] = true
		MoveFile(mfsFile)
		go func(interval int) {
			time.Sleep(time.Duration(interval) * time.Second)
			delete(lock, mfsFile.Path)
		}(interval)
	})
	ec.Subscribe(watcher.Chmod.String(), func(mfsFile *MFSFile) {
		lock[mfsFile.Path] = true
		ChmodFile(mfsFile)
		go func(interval int) {
			time.Sleep(time.Duration(interval) * time.Second)
			delete(lock, mfsFile.Path)
		}(interval)
	})
	if sync {
		ec.Subscribe(Sync, func(mfsSync *MFSSync) {
			if mfsSync.Type == Request {
				SyncFiles(ec, mfsSync, path)
			}
		})
	} else {
		SyncClient(ec)
	}
	return mfsClient
}

func (m MFS) Send(files map[string]watcher.Event) {
	for key := range files {
		if lock[key] {
			continue
		}
		log.Println(files[key])
		if files[key].Op == watcher.Create {
			SendDir(m.Ec, files[key], "")
			SendFile(m.Ec, files[key], "")
		}
		if files[key].Op == watcher.Write {
			SendFile(m.Ec, files[key], "")
		}
		if files[key].Op == watcher.Remove {
			SendRemoveFile(m.Ec, files[key])
		}
		if files[key].Op == watcher.Rename || files[key].Op == watcher.Move {
			SendMoveFile(m.Ec, files[key])
		}
		if files[key].Op == watcher.Chmod {
			SendChmodFile(m.Ec, files[key])
		}
	}
}

func (m MFS) Close() {
	m.Ec.Close()
}
