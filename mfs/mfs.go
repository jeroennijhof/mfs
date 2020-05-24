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
	Mode uint32
	Uid uint32
	Gid uint32
	Content []byte
}

var lockCreate string
var lockWrite string
var lockRemove string

// Helper functions
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

func SendDir(ec *nats.EncodedConn, dir string) {
	hostname, _ := os.Hostname()
	fileinfo, err := os.Stat(dir)
	if err != nil {
		log.Println("mfs error:", err)
		return
	}
	if !fileinfo.IsDir() {
		return
	}
	stat, _ := fileinfo.Sys().(*syscall.Stat_t)

	mfsFile := &MFSFile{Type: watcher.Create.String(), Hostname: hostname, Path: dir,
		Mode: stat.Mode, Uid: stat.Uid, Gid: stat.Gid}
	ec.Publish(watcher.Create.String(), mfsFile)
}

func SendFile(ec *nats.EncodedConn, file string) {
	hostname, _ := os.Hostname()
	fileinfo, err := os.Stat(file)
	if err != nil {
		log.Println("mfs error:", err)
		return
	}
	if fileinfo.IsDir() {
		return
	}
	stat, _ := fileinfo.Sys().(*syscall.Stat_t)
	content, err := ioutil.ReadFile(file)
	if err != nil {
		log.Println("mfs error:", err)
		return
	}

	mfsFile := &MFSFile{Type: watcher.Write.String(), Hostname: hostname, Path: file,
		Mode: stat.Mode, Uid: stat.Uid, Gid: stat.Gid, Content: content}
	ec.Publish(watcher.Write.String(), mfsFile)
}

func SendRemoveFile(ec *nats.EncodedConn, file string) {
	hostname, _ := os.Hostname()
	mfsFile := &MFSFile{Type: watcher.Remove.String(), Hostname: hostname, Path: file}
	ec.Publish(watcher.Remove.String(), mfsFile)
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
		lockCreate = mfsFile.Path
		CreateDir(mfsFile)
		go func(interval int) {
			time.Sleep(time.Duration(interval) * time.Second)
			lockCreate = ""
		}(interval)
	})
	ec.Subscribe(watcher.Write.String(), func(mfsFile *MFSFile) {
		lockWrite = mfsFile.Path
		WriteFile(mfsFile)
		go func(interval int) {
			time.Sleep(time.Duration(interval) * time.Second)
			lockWrite = ""
		}(interval)
	})
	ec.Subscribe(watcher.Remove.String(), func(mfsFile *MFSFile) {
		lockRemove = mfsFile.Path
		RemoveFile(mfsFile)
		go func(interval int) {
			time.Sleep(time.Duration(interval) * time.Second)
			lockRemove = ""
		}(interval)
	})
	return mfsClient
}

func (m MFS) Send(files map[string]watcher.Op) {
	for key := range files {
		if key == lockCreate || key == lockWrite || key == lockRemove {
			continue
		}
		if files[key] == watcher.Create {
			SendDir(m.Ec, key)
			SendFile(m.Ec, key)
		}
		if files[key] == watcher.Write {
			SendFile(m.Ec, key)
		}
		if files[key] == watcher.Remove {
			SendRemoveFile(m.Ec, key)
		}
	}
}

func (m MFS) Close() {
	m.Ec.Close()
}
