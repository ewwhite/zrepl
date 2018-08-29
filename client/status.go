package client

import (
	"github.com/zrepl/zrepl/config"
	"github.com/zrepl/zrepl/daemon"
	"fmt"
	"github.com/zrepl/zrepl/replication"
	"github.com/mitchellh/mapstructure"
	"github.com/zrepl/zrepl/replication/fsrep"
	"github.com/nsf/termbox-go"
	"time"
	"github.com/pkg/errors"
	"sort"
	"sync"
)

type tui struct {
	x, y int
	indent int

	lock sync.Mutex //For report and error
	report map[string]interface{}
	err error
}

func newTui() tui {
	return tui{}
}

func (t *tui) moveCursor(x, y int) {
	t.x += x
	t.y += y
}

func (t *tui) moveLine(dl int, col int) {
	t.y += dl
	t.x = t.indent * 4 + col
}

func (t *tui) write(text string) {
	for _, c := range text {
		termbox.SetCell(t.x, t.y, c, termbox.ColorDefault, termbox.ColorDefault)
		t.x += 1
	}
}

func (t *tui) printf(text string, a ...interface{}) {
	t.write(fmt.Sprintf(text, a...))
}

func (t *tui) newline() {
	t.moveLine(1, 0)
}

func (t *tui) setIndent(indent int) {
	t.indent = indent
	t.moveLine(0, 0)
}

func (t *tui) addIndent(indent int) {
	t.indent += indent
	t.moveLine(0, 0)
}


func RunStatus(config config.Config, args []string) error {
	httpc, err := controlHttpClient(config.Global.Control.SockPath)
	if err != nil {
		return err
	}

	t := newTui()
	t.lock.Lock()
	t.err = errors.New("Got no report yet")
	t.lock.Unlock()

	err = termbox.Init()
	if err != nil {
		return err
	}
	defer termbox.Close()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	go func() {
		for _ = range ticker.C {
			m := make(map[string]interface{})

			err2 := jsonRequestResponse(httpc, daemon.ControlJobEndpointStatus,
				struct {}{},
				&m,
			)

			t.lock.Lock()
			t.err = err2
			t.report = m
			t.lock.Unlock()
			t.draw()
		}
	}()

	termbox.HideCursor()
	termbox.Clear(termbox.ColorDefault, termbox.ColorDefault)

	loop:
	for {
		switch ev := termbox.PollEvent(); ev.Type {
		case termbox.EventKey:
			switch ev.Key {
			case termbox.KeyEsc:
				break loop
			case termbox.KeyCtrlC:
				break loop
			}
		case termbox.EventResize:
			t.draw()
		}
	}

	return nil

}

func (t *tui) draw() {
	t.lock.Lock()
	defer t.lock.Unlock()

	termbox.Clear(termbox.ColorDefault, termbox.ColorDefault)
	t.x = 0
	t.y = 0
	t.indent = 0

	if t.err != nil {
		t.write(t.err.Error())
	} else {
		//Iterate over map in alphabetical order
		keys := make([]string, len(t.report))
		i := 0
		for k, _ := range t.report {
			keys[i] = k
			i++
		}
		sort.Strings(keys)

		for _, k := range keys {
			v := t.report[k]
			if len(k) == 0 || k[0] == '_' { //Internal job
				continue
			}
			t.setIndent(0)

			t.printf("Job: %s", k)
			t.setIndent(1)
			t.newline()

			if v == nil {
				t.printf("No report generated yet")
				t.newline()
				continue
			}
			rep := replication.Report{}
			err := mapstructure.Decode(v, &rep)
			if err != nil {
				t.printf("Failed to decode report: %s", err.Error())
				t.newline()
				continue
			}
			t.printf("Status:  %s", rep.Status)
			t.newline()
			t.printf("Problem: %s", rep.Problem)
			t.newline()

			for _, fs := range rep.Completed {
				printFilesystem(fs, t)
			}
			if rep.Active != nil {
				printFilesystem(rep.Active, t)
			}
			for _, fs := range rep.Pending {
				printFilesystem(fs, t)
			}

		}
	}
	termbox.Flush()
}

func times(str string, n int) (out string) {
	for i := 0; i < n; i++ {
		out += str
	}
	return
}

func rightPad(str string, length int, pad string) string {
	return str + times(pad, length-len(str))
}

func (t *tui) drawBar(name string, status string, total int, done int) {
	t.write(rightPad(name, 20, " "))
	t.write(" ")
	t.write(rightPad(status, 20, " "))

	if total > 0 {
		length := 50
		completedLength := length * done / total

		//FIXME finished bar has 1 off size compared to not finished bar
		t.write(times("=", completedLength-1))
		t.write(">")
		t.write(times("-", length-completedLength))

		t.printf(" %d/%d", done, total)
	}

	t.newline()
}

func printFilesystem(rep *fsrep.Report, t *tui) {
	t.drawBar(rep.Filesystem, rep.Status, len(rep.Completed) + len(rep.Pending), len(rep.Completed))
	if (rep.Problem != "") {
		t.addIndent(1)
		t.printf("Problem: %s", rep.Problem)
		t.newline()
		t.addIndent(-1)
	}
}