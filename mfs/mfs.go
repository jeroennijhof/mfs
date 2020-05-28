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
	Hostname string
	Status string
}

const Sync = "SYNC"
const SyncReply = "SYNCREPLY"
const Request = "REQUEST"
const Response = "RESPONSE"
var lock = make(map[string]bool)

// Helper recv functions
func Skip(mfsFile *MFSFile) bool {
	hostname, _ := os.Hostname()
	if hostname == mfsFile.Hostname {
		log.Println("mfs: skip same host")
		return true
	}
	if mfsFile.TargetHost != "" && mfsFile.TargetHost != hostname {
		log.Println("mfs: skip sync request")
		return true
	}
	return false
}

func CreateDir(mfsFile *MFSFile) {
	log.Println("mfs: creating directory", mfsFile.Path)
	if Skip(mfsFile) {
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
	if mfsFile.OldPath != "" {
		return
	}
	log.Println("mfs: writing file", mfsFile.Path)
	if Skip(mfsFile) {
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

func CreateSymlink(mfsFile *MFSFile) {
	if mfsFile.OldPath == "" {
		return
	}
	log.Println("mfs: creating symlink", mfsFile.Path)
	if Skip(mfsFile) {
		return
	}
	err := os.Symlink(mfsFile.OldPath, mfsFile.Path)
	if err != nil {
		log.Println("mfs error:", err)
		return
	}
}

func RemoveFile(mfsFile *MFSFile) {
	log.Println("mfs: removing file", mfsFile.Path)
	if Skip(mfsFile) {
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
	if event.FileInfo.IsDir() || event.FileInfo.Mode()&os.ModeSymlink != 0 {
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

func SendSymlink(ec *nats.EncodedConn, event watcher.Event, target string) {
	hostname, _ := os.Hostname()
	if event.FileInfo.Mode()&os.ModeSymlink == 0 {
		return
	}
	link, err := os.Readlink(event.Path)
	if err != nil {
		log.Println("mfs error:", err)
		return
	}

	mfsFile := &MFSFile{Type: watcher.Write.String(), Hostname: hostname, TargetHost: target,
		Path: event.Path, OldPath: link}
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
		SendDir(ec, event, mfsSync.Hostname)
		SendFile(ec, event, mfsSync.Hostname)
		SendSymlink(ec, event, mfsSync.Hostname)
		return nil
	})
	if err != nil {
		log.Println(err)
	}
	mfsSyncReply := &MFSSync{Hostname: mfsSync.Hostname, Status: "done"}
	ec.Publish(SyncReply, mfsSyncReply)
}

func SyncClient(ec *nats.EncodedConn) {
	hostname, _ := os.Hostname()
	status := "waiting"

	subSyncReply, _ := ec.Subscribe(SyncReply, func(mfsSync *MFSSync) {
		if mfsSync.Hostname == hostname {
			status = mfsSync.Status
		}
	})

	log.Println("mfs: init sync started")
	mfsSync := &MFSSync{Hostname: hostname}
	ec.Publish(Sync, mfsSync)

	//Wait for Sync message complete
	for {
		time.Sleep(time.Duration(1) * time.Second)
		if status != "waiting" {
			log.Println("mfs: init sync done")
			break
		}
	}
	subSyncReply.Unsubscribe()
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
		CreateSymlink(mfsFile)
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
		// Use nats queue group for init sync
		ec.QueueSubscribe(Sync, "sync_workers", func(mfsSync *MFSSync) {
			SyncFiles(ec, mfsSync, path)
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
			SendSymlink(m.Ec, files[key], "")
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
