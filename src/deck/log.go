package deck

import (
	"io"
	"io/ioutil"
	"log"
	"os"
)

type Logger struct {
	Debug, Info, Failure, Warning, Error *log.Logger
}

var (
	Log *Logger
)

func InitLogging(debugWriter io.Writer, infoWriter io.Writer, failureWriter io.Writer,
	warningWriter io.Writer, errorWriter io.Writer) {

	Log = &Logger{
		Debug:   log.New(debugWriter, "[DEBUG  ] ", log.Ldate|log.Ltime|log.Lshortfile),
		Info:    log.New(infoWriter, "[INFO   ] ", log.Ldate|log.Ltime|log.Lshortfile),
		Failure: log.New(failureWriter, "[FAILURE] ", log.Ldate|log.Ltime|log.Lshortfile),
		Warning: log.New(warningWriter, "[WARNING] ", log.Ldate|log.Ltime|log.Lshortfile),
		Error:   log.New(errorWriter, "[ERROR  ] ", log.Ldate|log.Ltime|log.Lshortfile),
	}
}

func InitDefaultLogging() {
	InitInfoLogging()
}

func InitDebugLogging() {
	InitLogging(os.Stdout, os.Stdout, os.Stdout, os.Stderr, os.Stderr)
}

func InitInfoLogging() {
	InitLogging(ioutil.Discard, os.Stdout, os.Stdout, os.Stderr, os.Stderr)
}

func InitFailureLogging() {
	InitLogging(ioutil.Discard, ioutil.Discard, os.Stdout, os.Stderr, os.Stderr)
}

func InitWarningLogging() {
	InitLogging(ioutil.Discard, ioutil.Discard, ioutil.Discard, os.Stderr, os.Stderr)
}

func InitErrorLogging() {
	InitLogging(ioutil.Discard, ioutil.Discard, ioutil.Discard, ioutil.Discard, os.Stderr)
}

func InitSilentLogging() {
	InitLogging(ioutil.Discard, ioutil.Discard, ioutil.Discard, ioutil.Discard, ioutil.Discard)
}
