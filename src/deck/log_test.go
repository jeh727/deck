package deck

import (
	"testing"
)

func TestLogging(t *testing.T) {

	//Mark 1 eyeball
	InitDefaultLogging()

	Log.Debug.Println("Debug Statement")
	Log.Info.Println("Info Statement")
	Log.Failure.Println("Failure statement")
	Log.Warning.Println("Warning statement")
	Log.Error.Println("Error statement")
}
