package zfs

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
	"context"
	"github.com/problame/go-rwccmd"
	"github.com/prometheus/client_golang/prometheus"
	"regexp"
	"strconv"
)

type DatasetPath struct {
	comps []string
}

func (p *DatasetPath) ToString() string {
	return strings.Join(p.comps, "/")
}

func (p *DatasetPath) Empty() bool {
	return len(p.comps) == 0
}

func (p *DatasetPath) Extend(extend *DatasetPath) {
	p.comps = append(p.comps, extend.comps...)
}

func (p *DatasetPath) HasPrefix(prefix *DatasetPath) bool {
	if len(prefix.comps) > len(p.comps) {
		return false
	}
	for i := range prefix.comps {
		if prefix.comps[i] != p.comps[i] {
			return false
		}
	}
	return true
}

func (p *DatasetPath) TrimPrefix(prefix *DatasetPath) {
	if !p.HasPrefix(prefix) {
		return
	}
	prelen := len(prefix.comps)
	newlen := len(p.comps) - prelen
	oldcomps := p.comps
	p.comps = make([]string, newlen)
	for i := 0; i < newlen; i++ {
		p.comps[i] = oldcomps[prelen+i]
	}
	return
}

func (p *DatasetPath) TrimNPrefixComps(n int) {
	if len(p.comps) < n {
		n = len(p.comps)
	}
	if n == 0 {
		return
	}
	p.comps = p.comps[n:]

}

func (p DatasetPath) Equal(q *DatasetPath) bool {
	if len(p.comps) != len(q.comps) {
		return false
	}
	for i := range p.comps {
		if p.comps[i] != q.comps[i] {
			return false
		}
	}
	return true
}

func (p *DatasetPath) Length() int {
	return len(p.comps)
}

func (p *DatasetPath) Copy() (c *DatasetPath) {
	c = &DatasetPath{}
	c.comps = make([]string, len(p.comps))
	copy(c.comps, p.comps)
	return
}

func (p *DatasetPath) MarshalJSON() ([]byte, error) {
	return json.Marshal(p.comps)
}

func (p *DatasetPath) UnmarshalJSON(b []byte) error {
	p.comps = make([]string, 0)
	return json.Unmarshal(b, &p.comps)
}

func NewDatasetPath(s string) (p *DatasetPath, err error) {
	p = &DatasetPath{}
	if s == "" {
		p.comps = make([]string, 0)
		return p, nil // the empty dataset path
	}
	const FORBIDDEN = "@#|\t<>*"
	/* Documenation of allowed characters in zfs names:
	https://docs.oracle.com/cd/E19253-01/819-5461/gbcpt/index.html
	Space is missing in the oracle list, but according to
	https://github.com/zfsonlinux/zfs/issues/439
	there is evidence that it was intentionally allowed
	*/
	if strings.ContainsAny(s, FORBIDDEN) {
		err = fmt.Errorf("contains forbidden characters (any of '%s')", FORBIDDEN)
		return
	}
	p.comps = strings.Split(s, "/")
	if p.comps[len(p.comps)-1] == "" {
		err = fmt.Errorf("must not end with a '/'")
		return
	}
	return
}

func toDatasetPath(s string) *DatasetPath {
	p, err := NewDatasetPath(s)
	if err != nil {
		panic(err)
	}
	return p
}

type ZFSError struct {
	Stderr  []byte
	WaitErr error
}

func (e *ZFSError) Error() string {
	return fmt.Sprintf("zfs exited with error: %s\nstderr:\n%s", e.WaitErr.Error(), e.Stderr)
}

var ZFS_BINARY string = "zfs"

func ZFSList(properties []string, zfsArgs ...string) (res [][]string, err error) {

	args := make([]string, 0, 4+len(zfsArgs))
	args = append(args,
		"list", "-H", "-p",
		"-o", strings.Join(properties, ","))
	args = append(args, zfsArgs...)

	cmd := exec.Command(ZFS_BINARY, args...)

	var stdout io.Reader
	stderr := bytes.NewBuffer(make([]byte, 0, 1024))
	cmd.Stderr = stderr

	if stdout, err = cmd.StdoutPipe(); err != nil {
		return
	}

	if err = cmd.Start(); err != nil {
		return
	}

	s := bufio.NewScanner(stdout)
	buf := make([]byte, 1024)
	s.Buffer(buf, 0)

	res = make([][]string, 0)

	for s.Scan() {
		fields := strings.SplitN(s.Text(), "\t", len(properties))

		if len(fields) != len(properties) {
			err = errors.New("unexpected output")
			return
		}

		res = append(res, fields)
	}

	if waitErr := cmd.Wait(); waitErr != nil {
		err := &ZFSError{
			Stderr:  stderr.Bytes(),
			WaitErr: waitErr,
		}
		return nil, err
	}
	return
}

type ZFSListResult struct {
	Fields []string
	Err    error
}

// ZFSListChan executes `zfs list` and sends the results to the `out` channel.
// The `out` channel is always closed by ZFSListChan:
// If an error occurs, it is closed after sending a result with the Err field set.
// If no error occurs, it is just closed.
// If the operation is cancelled via context, the channel is just closed.
//
// However, if callers do not drain `out` or cancel via `ctx`, the process will leak either running because
// IO is pending or as a zombie.
func ZFSListChan(ctx context.Context, out chan ZFSListResult, properties []string, zfsArgs ...string) {
	defer close(out)

	args := make([]string, 0, 4+len(zfsArgs))
	args = append(args,
		"list", "-H", "-p",
		"-o", strings.Join(properties, ","))
	args = append(args, zfsArgs...)

	sendResult := func(fields []string, err error) (done bool) {
		select {
		case <-ctx.Done():
			return true
		case out <- ZFSListResult{fields, err}:
			return false
		}
	}

	cmd, err := rwccmd.CommandContext(ctx, ZFS_BINARY, args, []string{})
	if err != nil {
		sendResult(nil, err)
		return
	}
	if err = cmd.Start(); err != nil {
		sendResult(nil, err)
		return
	}
	defer cmd.Close()

	s := bufio.NewScanner(cmd)
	buf := make([]byte, 1024) // max line length
	s.Buffer(buf, 0)

	for s.Scan() {
		fields := strings.SplitN(s.Text(), "\t", len(properties))
		if len(fields) != len(properties) {
			sendResult(nil, errors.New("unexpected output"))
			return
		}
		if sendResult(fields, nil) {
			return
		}
	}
	if s.Err() != nil {
		sendResult(nil, s.Err())
	}
	return
}

func validateRelativeZFSVersion(s string) error {
	if len(s) <= 1 {
		return errors.New("version must start with a delimiter char followed by at least one character")
	}
	if !(s[0] == '#' || s[0] == '@') {
		return errors.New("version name starts with invalid delimiter char")
	}
	// FIXME whitespace check...
	return nil
}

func validateZFSFilesystem(fs string) error {
	if len(fs) < 1 {
		return errors.New("filesystem path must have length > 0")
	}
	return nil
}

func absVersion(fs, v string) (full string, err error) {
	if err := validateZFSFilesystem(fs); err != nil {
		return "", err
	}
	if err := validateRelativeZFSVersion(v); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s%s", fs, v), nil
}

func buildCommonSendArgs(fs string, from, to string, token string) ([]string, error) {
	args := make([]string, 0, 3)
	if token != "" {
		args = append(args, "-t", token)
		return args, nil
	}

	toV, err := absVersion(fs, to)
	if err != nil {
		return nil, err
	}

	fromV := ""
	if from != "" {
		fromV, err = absVersion(fs, from)
		if err != nil {
			return nil, err
		}
	}

	if fromV == "" { // Initial
		args = append(args, toV)
	} else {
		args = append(args, "-i", fromV, toV)
	}
	return args, nil
}

type sendStreamCopier struct {
	recorder readErrRecorder
}

type readErrRecorder struct {
	io.ReadCloser
	readErr error
}

type sendStreamCopierError struct {
	isReadErr bool // if false, it's a write error
	err error
}

func (e sendStreamCopierError) Error() string {
	if e.isReadErr {
		return fmt.Sprintf("stream: read error: %s", e.err)
	} else {
		return fmt.Sprintf("stream: writer error: %s", e.err)
	}
}

func (e sendStreamCopierError) IsReadError() bool { return e.isReadErr }
func (e sendStreamCopierError) IsWriteError() bool { return !e.isReadErr }

func (r *readErrRecorder) Read(p []byte) (n int, err error) {
	n, err = r.ReadCloser.Read(p)
	r.readErr = err
	return n, err
}

func newSendStreamCopier(stream io.ReadCloser) *sendStreamCopier {
	return &sendStreamCopier{recorder: readErrRecorder{stream, nil}}
}

func (c sendStreamCopier) WriteStreamTo(w io.Writer) StreamCopierError {
	_, err := io.Copy(w, &c.recorder)
	if err != nil {
		if c.recorder.readErr != nil {
			return sendStreamCopierError{isReadErr: true, err: c.recorder.readErr}
		} else {
			return sendStreamCopierError{isReadErr: false, err: err}
		}
	}
	return nil
}

func (c sendStreamCopier) Read(p []byte) (n int, err error) {
	return c.recorder.Read(p)
}

func (c sendStreamCopier) Close() error {
	return c.recorder.ReadCloser.Close()
}

func pipeWithCapacity(capacity int) (r, w *os.File, err error) {
	if capacity <= 0 {
		panic(fmt.Sprintf("capacity must be positive %v", capacity))
	}
	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		return nil, nil, err
	}
	res, err := unix.FcntlInt(stdoutWriter.Fd(), unix.F_SETPIPE_SZ, capacity)
	if err != nil {
		stdoutReader.Close() // TODO log / metric?
		stdoutWriter.Close() // TODO log / metric?
		return nil, nil, fmt.Errorf("cannot set pipe capacity to %v", capacity)
	} else if res == -1 {
		return nil, nil, errors.New("cannot set pipe capacity: fcntl returned -1")
	}
	return stdoutReader, stdoutWriter, nil
}

type sendStream struct {
	cmd *exec.Cmd
	kill context.CancelFunc

	closeMtx sync.Mutex
	stdoutReader *os.File
	opErr error

}

func (s *sendStream) Read(p []byte) (n int, err error) {
	s.closeMtx.Lock()
	opErr := s.opErr
	s.closeMtx.Unlock()
	if opErr != nil {
		return 0, opErr
	}

	n, err = s.stdoutReader.Read(p)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Read err %v\n", err) // FIXME
		// TODO we assume here that any read error is permanent
		// which is most likely the case for a local zfs send
		return n, s.killAndWait(err)
	}
	return n, err
}

func (s *sendStream) Close() error {
	fmt.Fprintf(os.Stderr, "close\n") // FIXME
	return s.killAndWait(nil)
}

func (s *sendStream) killAndWait(precedingReadErr error) error {

	fmt.Fprintf(os.Stderr, "killAndWait\n") // FIXME
	defer fmt.Fprintf(os.Stderr, "killAndWait returns\n") // FIXME
	if precedingReadErr == io.EOF {
		// give the zfs process a little bit of time to terminate itself
		// if it holds this deadline, exitErr will be nil
		time.AfterFunc(200*time.Millisecond, s.kill)
	} else {
		s.kill()
	}

	// allow async kills from Close(), that's why we only take the mutex here
	s.closeMtx.Lock()
	defer s.closeMtx.Unlock()

	if s.opErr != nil {
		return s.opErr
	}

	waitErr := s.cmd.Wait()
	// distinguish between ExitError (which is actually a non-problem for us)
	// vs failed wait syscall (for which we give upper layers the chance to retyr)
	var exitErr *exec.ExitError
	if waitErr != nil {
		if ee, ok := waitErr.(*exec.ExitError); ok {
			exitErr = ee
		} else {
			return waitErr
		}
	}

	// now, after we know the program exited do we close the pipe
	var closePipeErr error
	if s.stdoutReader != nil {
		closePipeErr = s.stdoutReader.Close()
		if closePipeErr == nil {
			// avoid double-closes in case anything below doesn't work
			// and someone calls Close again
			s.stdoutReader = nil 
		} else {
			return closePipeErr
		}
	}

	// we managed to tear things down, no let's give the user some pretty *ZFSError
	if exitErr != nil {
		s.opErr = &ZFSError{
			Stderr: exitErr.Stderr,
			WaitErr: exitErr,
		}
	} else {
		s.opErr = fmt.Errorf("zfs send exited with status code 0")
	}

	// detect the edge where we're called from s.Read
	// after the pipe EOFed and zfs send exited without errors
	// this is actullay the "hot" / nice path
	if exitErr == nil && precedingReadErr == io.EOF {
		return precedingReadErr
	}

	return s.opErr
}

// if token != "", then send -t token is used
// otherwise send [-i from] to is used
// (if from is "" a full ZFS send is done)
func ZFSSend(ctx context.Context, fs string, from, to string, token string) (streamCopier StreamCopier, err error) {

	// return newSendStreamCopier(devnoop.Get()), nil

	args := make([]string, 0)
	args = append(args, "send")

	sargs, err := buildCommonSendArgs(fs, from, to, token)
	if err != nil {
		return nil, err
	}
	args = append(args, sargs...)

	ctx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(ctx, ZFS_BINARY, args...)

	// setup stdout with an os.Pipe to control pipe buffer size
	stdoutReader, stdoutWriter, err := pipeWithCapacity(1<<25); // FIXME constant
	if err != nil {
		cancel()
		return nil, err
	}

	cmd.Stdout = stdoutWriter

	if err := cmd.Start(); err != nil {
		cancel()
		stdoutWriter.Close()
		stdoutReader.Close()
		return nil, err
	}
	stdoutWriter.Close()

	stream := &sendStream{
		cmd: cmd,
		kill: cancel,
		stdoutReader: stdoutReader,
	}

	return newSendStreamCopier(stream), err
}


type DrySendType string

const (
	DrySendTypeFull        DrySendType = "full"
	DrySendTypeIncremental DrySendType = "incremental"
)

func DrySendTypeFromString(s string) (DrySendType, error) {
	switch s {
	case string(DrySendTypeFull): return DrySendTypeFull, nil
	case string(DrySendTypeIncremental): return DrySendTypeIncremental, nil
	default:
		return "", fmt.Errorf("unknown dry send type %q", s)
	}
}

type DrySendInfo struct {
	Type DrySendType
	Filesystem string // parsed from To field
	From, To string // direct copy from ZFS output
	SizeEstimate int64 // -1 if size estimate is not possible
}

var sendDryRunInfoLineRegex = regexp.MustCompile(`^(full|incremental)(\t(\S+))?\t(\S+)\t([0-9]+)$`)

// see test cases for example output
func (s *DrySendInfo) unmarshalZFSOutput(output []byte) (err error) {
	lines := strings.Split(string(output), "\n")
	for _, l := range lines {
		regexMatched, err := s.unmarshalInfoLine(l)
		if err != nil {
			return fmt.Errorf("line %q: %s", l, err)
		}
		if !regexMatched {
			continue
		}
		return nil
	}
	return fmt.Errorf("no match for info line (regex %s)", sendDryRunInfoLineRegex)
}


// unmarshal info line, looks like this:
//   full	zroot/test/a@1	5389768
//   incremental	zroot/test/a@1	zroot/test/a@2	5383936
// => see test cases
func (s *DrySendInfo) unmarshalInfoLine(l string) (regexMatched bool, err error) {

	m := sendDryRunInfoLineRegex.FindStringSubmatch(l)
	if m == nil {
		return false, nil
	}
	s.Type, err = DrySendTypeFromString(m[1])
	if err != nil {
		return true, err
	}

	s.From = m[3]
	s.To = m[4]
	toFS, _, _ , err := DecomposeVersionString(s.To)
	if err != nil {
		return true, fmt.Errorf("'to' is not a valid filesystem version: %s", err)
	}
	s.Filesystem = toFS

	s.SizeEstimate, err = strconv.ParseInt(m[5], 10, 64)
	if err != nil {
		return true, fmt.Errorf("cannot not parse size: %s", err)
	}

	return true, nil
}

// from may be "", in which case a full ZFS send is done
// May return BookmarkSizeEstimationNotSupported as err if from is a bookmark.
func ZFSSendDry(fs string, from, to string, token string) (_ *DrySendInfo, err error) {

	if strings.Contains(from, "#") {
		/* TODO:
		 * ZFS at the time of writing does not support dry-run send because size-estimation
		 * uses fromSnap's deadlist. However, for a bookmark, that deadlist no longer exists.
		 * Redacted send & recv will bring this functionality, see
		 * 	https://github.com/openzfs/openzfs/pull/484
		 */
		 fromAbs, err := absVersion(fs, from)
		 if err != nil {
		 	return nil, fmt.Errorf("error building abs version for 'from': %s", err)
		 }
		 toAbs, err := absVersion(fs, to)
		 if err != nil {
		 	return nil, fmt.Errorf("error building abs version for 'to': %s", err)
		 }
		 return &DrySendInfo{
			Type: DrySendTypeIncremental,
			Filesystem: fs,
			From: fromAbs,
			To: toAbs,
			SizeEstimate: -1}, nil
	}

	args := make([]string, 0)
	args = append(args, "send", "-n", "-v", "-P")
	sargs, err := buildCommonSendArgs(fs, from, to, token)
	if err != nil {
		return nil, err
	}
	args = append(args, sargs...)

	cmd := exec.Command(ZFS_BINARY, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, err
	}
	var si DrySendInfo
	if err := si.unmarshalZFSOutput(output); err != nil {
		return nil, fmt.Errorf("could not parse zfs send -n output: %s", err)
	}
	return &si, nil
}

type StreamCopierError interface {
	error
	IsReadError() bool
	IsWriteError() bool
}

type StreamCopier interface {
	WriteStreamTo(w io.Writer) StreamCopierError
	Close() error
}


func ZFSRecv(ctx context.Context, fs string, streamCopier StreamCopier, additionalArgs ...string) (err error) {

	// return streamCopier.WriteStreamTo(devnoop.Get())

	if err := validateZFSFilesystem(fs); err != nil {
		return err
	}

	args := make([]string, 0)
	args = append(args, "recv")
	if len(args) > 0 {
		args = append(args, additionalArgs...)
	}
	args = append(args, fs)

	ctx, cancelCmd := context.WithCancel(ctx)
	defer cancelCmd()
	cmd := exec.CommandContext(ctx, ZFS_BINARY, args...)

	stderr := bytes.NewBuffer(make([]byte, 0, 1024))
	cmd.Stderr = stderr

	// TODO report bug upstream
	// Setup an unused stdout buffer.
	// Otherwise, ZoL v0.6.5.9-1 3.16.0-4-amd64 writes the following error to stderr and exits with code 1
	//   cannot receive new filesystem stream: invalid backup stream
	stdout := bytes.NewBuffer(make([]byte, 0, 1024))
	cmd.Stdout = stdout

	stdin, stdinWriter, err := pipeWithCapacity(1<<25);
	if err != nil {
		return err
	}
	
	cmd.Stdin = stdin

	if err = cmd.Start(); err != nil {
		stdinWriter.Close()
		stdin.Close()
		return err
	}
	stdin.Close()
	defer stdinWriter.Close()
		
	fmt.Fprintf(os.Stderr, "Recv: cmd started\n") // FIXME

	copierErrChan := make(chan StreamCopierError)
	go func() {
		copierErrChan <- streamCopier.WriteStreamTo(stdinWriter)
	}()
	waitErrChan := make(chan *ZFSError)
	go func() {
		defer close(waitErrChan)
		if err = cmd.Wait(); err != nil {
			waitErrChan <- &ZFSError{
				Stderr:  stderr.Bytes(),
				WaitErr: err,
			}
			return
		}
	}()

	// streamCopier always fails before or simultaneously with Wait
	// thus receive from it first
	copierErr := <- copierErrChan
	if copierErr != nil {
		fmt.Fprintf(os.Stderr, "Recv: copierErr != nil %v\n", copierErr) // FIXME
		cancelCmd()	
	}
	fmt.Fprintf(os.Stderr, "Recv: copierErr = %v\n", copierErr) // FIXME
	waitErr := <- waitErrChan
	fmt.Fprintf(os.Stderr, "Recv returned: copierErr=%v waiterr=%v\n", copierErr, waitErr) // FIXME
	if copierErr != nil && copierErr.IsWriteError() && waitErr != nil {
		return waitErr // has more interesting info in that case
	}
	return copierErr // if it's not a write error, the copier error is more interesting
}

type ClearResumeTokenError struct {
	ZFSOutput []byte
	CmdError error
}

func (e ClearResumeTokenError) Error() string {
	return fmt.Sprintf("could not clear resume token: %q", string(e.ZFSOutput))
}

// always returns *ClearResumeTokenError
func ZFSRecvClearResumeToken(fs string) (err error) {
	if err := validateZFSFilesystem(fs); err != nil {
		return err
	}

	cmd := exec.Command(ZFS_BINARY, "recv", "-A", fs)
	o, err := cmd.CombinedOutput()
	if err != nil {
		if bytes.Contains(o, []byte("does not have any resumable receive state to abort")) {
			return nil
		}
		return &ClearResumeTokenError{o, err}
	}
	return nil
}

type ZFSProperties struct {
	m map[string]string
}

func NewZFSProperties() *ZFSProperties {
	return &ZFSProperties{make(map[string]string, 4)}
}

func (p *ZFSProperties) Set(key, val string) {
	p.m[key] = val
}

func (p *ZFSProperties) Get(key string) string {
	return p.m[key]
}

func (p *ZFSProperties) appendArgs(args *[]string) (err error) {
	for prop, val := range p.m {
		if strings.Contains(prop, "=") {
			return errors.New("prop contains rune '=' which is the delimiter between property name and value")
		}
		*args = append(*args, fmt.Sprintf("%s=%s", prop, val))
	}
	return nil
}

func ZFSSet(fs *DatasetPath, props *ZFSProperties) (err error) {
	return zfsSet(fs.ToString(), props)
}

func zfsSet(path string, props *ZFSProperties) (err error) {
	args := make([]string, 0)
	args = append(args, "set")
	err = props.appendArgs(&args)
	if err != nil {
		return err
	}
	args = append(args, path)

	cmd := exec.Command(ZFS_BINARY, args...)

	stderr := bytes.NewBuffer(make([]byte, 0, 1024))
	cmd.Stderr = stderr

	if err = cmd.Start(); err != nil {
		return err
	}

	if err = cmd.Wait(); err != nil {
		err = &ZFSError{
			Stderr:  stderr.Bytes(),
			WaitErr: err,
		}
	}

	return
}

func ZFSGet(fs *DatasetPath, props []string) (*ZFSProperties, error) {
	return zfsGet(fs.ToString(), props, sourceAny)
}

var zfsGetDatasetDoesNotExistRegexp = regexp.MustCompile(`^cannot open '(\S+)': (dataset does not exist|no such pool or dataset)`)

type DatasetDoesNotExist struct {
	Path string
}

func (d *DatasetDoesNotExist) Error() string { return fmt.Sprintf("dataset %q does not exist", d.Path) }

type zfsPropertySource uint

const (
	sourceLocal zfsPropertySource = 1 << iota
	sourceDefault
	sourceInherited
	sourceNone
	sourceTemporary
	sourceReceived

	sourceAny zfsPropertySource = ^zfsPropertySource(0)
)

func (s zfsPropertySource) zfsGetSourceFieldPrefixes() []string {
	prefixes := make([]string, 0, 7)
	if s&sourceLocal != 0 {prefixes = append(prefixes, "local")}
	if s&sourceDefault != 0 {prefixes = append(prefixes, "default")}
	if s&sourceInherited != 0 {prefixes = append(prefixes, "inherited")}
	if s&sourceNone != 0 {prefixes = append(prefixes, "-")}
	if s&sourceTemporary != 0 { prefixes = append(prefixes, "temporary")}
	if s&sourceReceived != 0 { prefixes = append(prefixes, "received")}
	if s == sourceAny { prefixes = append(prefixes, "") }
	return prefixes
}

func zfsGet(path string, props []string, allowedSources zfsPropertySource) (*ZFSProperties, error) {
	args := []string{"get", "-Hp", "-o", "property,value,source", strings.Join(props, ","), path}
	cmd := exec.Command(ZFS_BINARY, args...)
	stdout, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			if exitErr.Exited() {
				// screen-scrape output
				if sm := zfsGetDatasetDoesNotExistRegexp.FindSubmatch(exitErr.Stderr); sm != nil {
					if string(sm[1]) == path {
						return nil, &DatasetDoesNotExist{path}
					}
				}
			}
		}
		return nil, err
	}
	o := string(stdout)
	lines := strings.Split(o, "\n")
	if len(lines) < 1 || // account for newlines
		len(lines)-1 != len(props) {
		return nil, fmt.Errorf("zfs get did not return the number of expected property values")
	}
	res := &ZFSProperties{
		make(map[string]string, len(lines)),
	}
	allowedPrefixes := allowedSources.zfsGetSourceFieldPrefixes()
	for _, line := range lines[:len(lines)-1] {
		fields := strings.FieldsFunc(line, func(r rune) bool {
			return r == '\t'
		})
		if len(fields) != 3 {
			return nil, fmt.Errorf("zfs get did not return property,value,source tuples")
		}
		for _, p := range allowedPrefixes {
			if strings.HasPrefix(fields[2],p) {
				res.m[fields[0]] = fields[1]
				break
			}
		}
	}
	return res, nil
}

func ZFSDestroy(dataset string) (err error) {

	var dstype, filesystem string
	idx := strings.IndexAny(dataset, "@#")
	if idx == -1 {
		dstype = "filesystem"
		filesystem = dataset
	} else {
		switch dataset[idx] {
		case '@': dstype = "snapshot"
		case '#': dstype = "bookmark"
		}
		filesystem = dataset[:idx]
	}

	defer prometheus.NewTimer(prom.ZFSDestroyDuration.WithLabelValues(dstype, filesystem))

	cmd := exec.Command(ZFS_BINARY, "destroy", dataset)

	stderr := bytes.NewBuffer(make([]byte, 0, 1024))
	cmd.Stderr = stderr

	if err = cmd.Start(); err != nil {
		return err
	}

	if err = cmd.Wait(); err != nil {
		err = &ZFSError{
			Stderr:  stderr.Bytes(),
			WaitErr: err,
		}
	}

	return

}

func zfsBuildSnapName(fs *DatasetPath, name string) string { // TODO defensive
	return fmt.Sprintf("%s@%s", fs.ToString(), name)
}

func zfsBuildBookmarkName(fs *DatasetPath, name string) string { // TODO defensive
	return fmt.Sprintf("%s#%s", fs.ToString(), name)
}

func ZFSSnapshot(fs *DatasetPath, name string, recursive bool) (err error) {

	promTimer := prometheus.NewTimer(prom.ZFSSnapshotDuration.WithLabelValues(fs.ToString()))
	defer promTimer.ObserveDuration()

	snapname := zfsBuildSnapName(fs, name)
	cmd := exec.Command(ZFS_BINARY, "snapshot", snapname)

	stderr := bytes.NewBuffer(make([]byte, 0, 1024))
	cmd.Stderr = stderr

	if err = cmd.Start(); err != nil {
		return err
	}

	if err = cmd.Wait(); err != nil {
		err = &ZFSError{
			Stderr:  stderr.Bytes(),
			WaitErr: err,
		}
	}

	return

}

func ZFSBookmark(fs *DatasetPath, snapshot, bookmark string) (err error) {

	promTimer := prometheus.NewTimer(prom.ZFSBookmarkDuration.WithLabelValues(fs.ToString()))
	defer promTimer.ObserveDuration()

	snapname := zfsBuildSnapName(fs, snapshot)
	bookmarkname := zfsBuildBookmarkName(fs, bookmark)

	cmd := exec.Command(ZFS_BINARY, "bookmark", snapname, bookmarkname)

	stderr := bytes.NewBuffer(make([]byte, 0, 1024))
	cmd.Stderr = stderr

	if err = cmd.Start(); err != nil {
		return err
	}

	if err = cmd.Wait(); err != nil {
		err = &ZFSError{
			Stderr:  stderr.Bytes(),
			WaitErr: err,
		}
	}

	return

}
