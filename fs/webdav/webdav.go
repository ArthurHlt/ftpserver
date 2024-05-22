package webdav

import (
	"errors"
	"fmt"
	"github.com/fclairamb/ftpserver/config/confpar"
	"github.com/fclairamb/go-log"
	"github.com/spf13/afero"
	"github.com/studio-b12/gowebdav"
	"io"
	"net/http"
	"os"
	"sync"
	"time"
)

// ErrNotImplemented is returned when something is not implemented
var ErrNotImplemented = errors.New("not implemented")

// file is the afero.File implementation
type file struct {
	mutex     *sync.RWMutex
	path      string
	readPipe  io.ReadCloser
	writePipe io.WriteCloser
	reader    io.ReadCloser
	client    *gowebdav.Client
	logger    log.Logger
	writeErr  chan error
}

func newFile(path string, client *gowebdav.Client, logger log.Logger) *file {
	return &file{
		path:     path,
		client:   client,
		mutex:    &sync.RWMutex{},
		logger:   logger,
		writeErr: make(chan error, 1),
	}
}

func (f *file) Close() error {
	if f.reader != nil {
		f.reader.Close() // nolint: errcheck
	}
	if f.writePipe != nil {
		f.writePipe.Close()
	}

	select {
	case err := <-f.writeErr:
		return err
	default:
	}

	return nil
}

func (f *file) Read(p []byte) (n int, err error) {
	f.mutex.RLock()
	defer f.mutex.RUnlock()
	if f.reader == nil {
		r, err := f.client.ReadStream(f.path)
		if err != nil {
			return 0, err
		}
		f.reader = r
	}
	return f.reader.Read(p)
}

func (f *file) ReadAt(p []byte, off int64) (n int, err error) {
	if f.reader == nil {
		r, err := f.client.ReadStreamRange(f.path, off, -1)
		if err != nil {
			return 0, err
		}
		f.reader = r
	}
	return f.reader.Read(p)
}

func (f *file) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}

func (f *file) Write(p []byte) (n int, err error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	if f.writePipe == nil {
		f.readPipe, f.writePipe = io.Pipe()
		if err != nil {
			return 0, err
		}
		go func() {
			defer close(f.writeErr)
			defer f.readPipe.Close()
			err := f.client.WriteStream(f.path, f.readPipe, 0)
			if err != nil && err != io.EOF {
				f.logger.Error("WriteStream", "path", f.path, "error", err)
				f.writeErr <- fmt.Errorf("WriteStream: %w", err)
			}
		}()
	}

	return f.writePipe.Write(p)
}

func (f *file) WriteAt(p []byte, off int64) (n int, err error) {
	return 0, ErrNotImplemented
}

func (f *file) Name() string {
	return f.path
}

func (f *file) Readdir(count int) ([]os.FileInfo, error) {
	infos, err := f.client.ReadDir(f.path)
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return []os.FileInfo{}, nil
	}
	if count < 0 {
		return infos, nil
	}
	return infos[:count], nil
}

func (f *file) Readdirnames(n int) ([]string, error) {
	names := make([]string, 0)
	infos, err := f.Readdir(n)
	if err != nil {
		return nil, err
	}
	for _, info := range infos {
		names = append(names, info.Name())
	}
	return names, nil
}

func (f *file) Stat() (os.FileInfo, error) {
	return f.client.Stat(f.path)
}

func (f *file) Sync() error {
	return nil
}

func (f *file) Truncate(size int64) error {
	return ErrNotImplemented
}

func (f *file) WriteString(s string) (ret int, err error) {
	return f.Write([]byte(s))
}

type Fs struct {
	client *gowebdav.Client
	logger log.Logger
}

func LoadFs(access *confpar.Access, logger log.Logger) (afero.Fs, error) {
	url := ""
	if urlRaw, ok := access.Params["url"]; ok {
		url = urlRaw
	}
	user := ""
	if userRaw, ok := access.Params["user"]; ok {
		user = userRaw
	}
	pass := ""
	if passRaw, ok := access.Params["pass"]; ok {
		pass = passRaw
	}

	c := gowebdav.NewClient(url, user, pass)
	c.SetTimeout(0)
	c.SetInterceptor(func(method string, req *http.Request) {
		logger.Debug("Request", "method", method, "url", req.URL.String())
	})
	err := c.Connect()
	if err != nil {
		return nil, err
	}

	return &Fs{
		client: c,
		logger: logger,
	}, nil

}

func (f *Fs) Create(name string) (afero.File, error) {
	return newFile(name, f.client, f.logger), nil
}

func (f *Fs) Mkdir(name string, perm os.FileMode) error {
	return f.client.Mkdir(name, perm)
}

func (f *Fs) MkdirAll(path string, perm os.FileMode) error {
	return f.client.MkdirAll(path, perm)
}

func (f *Fs) Open(name string) (afero.File, error) {
	return newFile(name, f.client, f.logger), nil
}

func (f *Fs) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	return newFile(name, f.client, f.logger), nil
}

func (f *Fs) Remove(name string) error {
	return f.client.Remove(name)
}

func (f *Fs) RemoveAll(path string) error {
	return f.client.RemoveAll(path)
}

func (f *Fs) Rename(oldname, newname string) error {
	return f.client.Rename(oldname, newname, true)
}

func (f *Fs) Stat(name string) (os.FileInfo, error) {
	return f.client.Stat(name)
}

func (f *Fs) Name() string {
	return "webdav"
}

func (f *Fs) Chmod(name string, mode os.FileMode) error {
	return nil
}

func (f *Fs) Chown(name string, uid, gid int) error {
	return nil
}

func (f *Fs) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return nil
}
