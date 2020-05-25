package mfs

import (
	"log"
	"strings"
	"os"
	"io/ioutil"
	"syscall"
	"time"
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
	Path string
	OldPath string
	Mode uint32
	Uid uint32
	Gid uint32
	Content []byte
}

var lock = make(map[string]bool)

// Helper recv functions
func CreateDir(mfsFile *MFSFile) {
	log.Println("mfs: creating directory", mfsFile.Path)
	hostname, _ := os.Hostname()
	if hostname == mfsFile.Hostname {
		log.Println("mfs: skip creating directory, same host")
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
	hostname, _ := os.Hostname()
	if hostname == mfsFile.Hostname {
		log.Println("mfs: skip writing file, same host")
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
	hostname, _ := os.Hostname()
	if hostname == mfsFile.Hostname {
		log.Println("mfs: skip removing file, same host")
		return
	}
	err := os.Remove(mfsFile.Path)
	if err != nil {
		log.Println("mfs error:", err)
	}
}

func MoveFile(mfsFile *MFSFile) {
	log.Println("mfs: moving file", mfsFile.OldPath, "to", mfsFile.Path)
	hostname, _ := os.Hostname()
	if hostname == mfsFile.Hostname {
		log.Println("mfs: skip moving file, same host")
		return
	}
	err := os.Rename(mfsFile.OldPath, mfsFile.Path)
	if err != nil {
		log.Println("mfs error:", err)
	}
}

func ChmodFile(mfsFile *MFSFile) {
	log.Println("mfs: chmod file", mfsFile.Path)
	hostname, _ := os.Hostname()
	if hostname == mfsFile.Hostname {
		log.Println("mfs: skip chmod file, same host")
		return
	}
	err := os.Chmod(mfsFile.Path, os.FileMode(mfsFile.Mode))
	if err != nil {
		log.Println("mfs error:", err)
	}
}

// Helper send functions
func SendDir(ec *nats.EncodedConn, event watcher.Event) {
	hostname, _ := os.Hostname()
	if !event.FileInfo.IsDir() {
		return
	}
	stat, _ := event.FileInfo.Sys().(*syscall.Stat_t)

	mfsFile := &MFSFile{Type: watcher.Create.String(), Hostname: hostname, Path: event.Path,
		Mode: stat.Mode, Uid: stat.Uid, Gid: stat.Gid}
	ec.Publish(watcher.Create.String(), mfsFile)
}

func SendFile(ec *nats.EncodedConn, event watcher.Event) {
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

	mfsFile := &MFSFile{Type: watcher.Write.String(), Hostname: hostname, Path: event.Path,
		Mode: stat.Mode, Uid: stat.Uid, Gid: stat.Gid, Content: content}
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

// Public functions
func Client(servers []string, token string, interval int) MFS {
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
	return mfsClient
}

func (m MFS) Send(files map[string]watcher.Event) {
	for key := range files {
		if lock[key] {
			continue
		}
		log.Println(files[key])
		if files[key].Op == watcher.Create {
			SendDir(m.Ec, files[key])
			SendFile(m.Ec, files[key])
		}
		if files[key].Op == watcher.Write {
			SendFile(m.Ec, files[key])
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
