package main

import (
	"fmt"
	"time"
)

func main() {
	t := time.Now().UTC().UnixMilli()
	fmt.Printf("%s", t)
}