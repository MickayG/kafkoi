package kafkoi

func checkFlagSet(argName string, v *string) {
	if *v == "" {
		panic("Argument " + argName + " is not set")
	}
}
