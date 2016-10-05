
package log

import(
    "testing"
)

func TestLogging(t *testing.T) {

    //Mark 1 eyeball
    InitDefault()

    Debug.Println("Debug Statement")
    Info.Println("Info Statement")
    Failure.Println("Failure statement")
    Warning.Println("Warning statement")
    Error.Println("Error statement")
}
