
package log

import(
    "io"
    "log"
    "os"
)

var (
    Debug, Info, Failure, Warning, Error *log.Logger
)

func Init(debugWriter io.Writer, infoWriter io.Writer, failureWriter io.Writer,
        warningWriter io.Writer, errorWriter io.Writer) {

    Debug = log.New(debugWriter,     "[DEBUG  ] ", log.Ldate|log.Ltime|log.Lshortfile)
    Info = log.New(infoWriter,       "[INFO   ] ", log.Ldate|log.Ltime|log.Lshortfile)
    Failure = log.New(failureWriter, "[FAILURE] ", log.Ldate|log.Ltime|log.Lshortfile)
    Warning = log.New(warningWriter, "[WARNING] ", log.Ldate|log.Ltime|log.Lshortfile)
    Error = log.New(errorWriter,     "[ERROR  ] ", log.Ldate|log.Ltime|log.Lshortfile)
}

func InitDefault() {
    Init(os.Stdout, os.Stdout, os.Stdout, os.Stderr, os.Stderr)
}


