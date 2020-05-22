package mfs

import (
	"strings"
	"os"
	"fmt"
	"io/ioutil"
	"syscall"
	"time"
	nats "github.com/nats-io/nats.go"
)

const Sync = "SYNC"
const Remove = "REMOVE"

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

var lockSync string
var lockRemove string

// Helper functions
func SyncFile(mfsFile *MFSFile) {
	fmt.Println("Syncing file:", mfsFile.Path)
	hostname, _ := os.Hostname()
	if hostname == mfsFile.Hostname {
		fmt.Println("Skip syncing file, same host")
		return
	}
	err := ioutil.WriteFile(mfsFile.Path, mfsFile.Content, os.FileMode(mfsFile.Mode))
	if err != nil {
		fmt.Println(err)
	}
	err = os.Chown(mfsFile.Path, int(mfsFile.Uid), int(mfsFile.Gid))
	if err != nil {
		fmt.Println(err)
	}
}

func RemoveFile(mfsFile *MFSFile) {
	fmt.Println("Removing file:", mfsFile.Path)
	hostname, _ := os.Hostname()
	if hostname == mfsFile.Hostname {
		fmt.Println("Skip removing file, same host")
		return
	}
        err := os.Remove(mfsFile.Path)
        if err != nil {
                fmt.Println(err)
        }
}

func SendFile(ec *nats.EncodedConn, file string) {
	hostname, _ := os.Hostname()
	content, err := ioutil.ReadFile(file)
	if err != nil {
		fmt.Println(err)
		return
	}
        fileinfo, _ := os.Stat(file)
        stat, _ := fileinfo.Sys().(*syscall.Stat_t)

	mfsFile := &MFSFile{Type: Sync, Hostname: hostname, Path: file, Mode: stat.Mode, Uid: stat.Uid, Gid: stat.Gid, Content: content}
        ec.Publish(Sync, mfsFile)
}

func SendRemoveFile(ec *nats.EncodedConn, file string) {
	hostname, _ := os.Hostname()
        mfsFile := &MFSFile{Type: Remove, Hostname: hostname, Path: file}
        ec.Publish(Remove, mfsFile)
}

// Public functions
func Client(servers []string, token string, interval int) MFS {
        var url = strings.Join(servers, ", nats://")
        url = strings.Join([]string{"nats://", url}, "")

        nc, err := nats.Connect(url, nats.Token(token))
        if err != nil {
                fmt.Println(err)
                os.Exit(1)
        }
        ec, err := nats.NewEncodedConn(nc, nats.JSON_ENCODER)
        if err != nil {
                fmt.Println(err)
                os.Exit(1)
        }

	mfsClient := MFS {Url: url, Ec: ec}
        ec.Subscribe(Sync, func(mfsFile *MFSFile) {
		lockSync = mfsFile.Path
                SyncFile(mfsFile)
	        go func(interval int) {
			time.Sleep(time.Duration(interval) * time.Second)
			lockSync = ""
		}(interval)
        })
        ec.Subscribe(Remove, func(mfsFile *MFSFile) {
		lockRemove = mfsFile.Path
                RemoveFile(mfsFile)
	        go func(interval int) {
			time.Sleep(time.Duration(interval) * time.Second)
			lockRemove = ""
		}(interval)
        })
        return mfsClient
}

func (m MFS) Send(files map[string]string) {
        for key := range files {
		if key == lockSync || key == lockRemove {
			continue
		}
                if files[key] == Sync {
			SendFile(m.Ec, key)
		}
                if files[key] == Remove {
			SendRemoveFile(m.Ec, key)
		}
        }
}

func (m MFS) Close() {
	m.Ec.Close()
}
