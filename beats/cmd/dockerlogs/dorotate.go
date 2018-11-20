package main

import (
	"fmt"
	"time"

	"github.com/argpass/golab/beats/cmd/dockerlogs/internal"
)

func main() {
	data := "hello dockerlog---------->awefawefawef--awefawefawe"
	fmt.Println("start")
	w, err := internal.NewRotateWriter("/tmp/zkchen.log", 15+10+1, 5)
	fmt.Println("create file")
	if err != nil {
		panic(err)
	}
	for i := 0; i < 1000; i++ {
		fmt.Printf("to write [%d]\n", i)
		_, err = w.Write([]byte(fmt.Sprintf("%s_%d", data, time.Now().Unix())))
		if err != nil {
			fmt.Printf("err:%+v\n", err)
			panic(err)
		}
	}
	err = w.Close()
	if err != nil {
		panic(err)
	}
}
