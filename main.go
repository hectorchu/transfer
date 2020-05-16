package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"os"

	"github.com/cheggaaa/pb/v3"
)

func exit(msg string) {
	fmt.Println(msg)
	os.Exit(1)
}

func main() {
	if len(os.Args) < 2 {
		exit("Needs an argument")
	}
	if os.Args[1] == "-l" {
		if len(os.Args) < 3 {
			exit("Needs filename argument")
		}
		listen(os.Args[2])
	} else {
		connect(os.Args[1])
	}
}

const port = ":3333"
const chunkSize = 4 * 1024 * 1024

func listen(filename string) {
	fmt.Println("Serving", filename)
	ln, err := net.Listen("tcp", port)
	if err != nil {
		exit("Failed to listen")
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			exit("Failed to accept")
		}
		go handleConnection(conn, filename)
	}
}

func calcChunkSize(chunkIndex, fileSize int64) int64 {
	if (chunkIndex+1)*chunkSize > fileSize {
		return fileSize - chunkIndex*chunkSize
	}
	return chunkSize
}

func handleConnection(conn net.Conn, filename string) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	file, err := os.Open(filename)
	if err != nil {
		exit("Failed to open file")
	}
	defer file.Close()
	stat, _ := file.Stat()
	size := stat.Size()
	fmt.Fprintln(conn, stat.Name())
	binary.Write(conn, binary.LittleEndian, size)
	chunks := (size + chunkSize - 1) / chunkSize
	for i := int64(0); i < chunks; i++ {
		file.Seek(i*chunkSize, 0)
		buf := make([]byte, calcChunkSize(i, size), chunkSize)
		if _, err := io.ReadFull(file, buf); err != nil {
			exit("Failed to read file")
		}
		cksum := crc32.ChecksumIEEE(buf)
		binary.Write(conn, binary.LittleEndian, cksum)
		var ok bool
		if err := binary.Read(r, binary.LittleEndian, &ok); err != nil {
			break
		}
		if ok {
			continue
		}
		if _, err := conn.Write(buf); err != nil {
			break
		}
	}
}

func connect(server string) {
	conn, err := net.Dial("tcp", server+port)
	if err != nil {
		exit("Failed to connect")
	}
	defer conn.Close()
	r := bufio.NewReader(conn)
	filename, err := r.ReadString('\n')
	if err != nil {
		exit("Failed to read filename")
	}
	filename = filename[:len(filename)-1]
	var size int64
	if err := binary.Read(r, binary.LittleEndian, &size); err != nil {
		exit("Failed to read file size")
	}
	fmt.Printf("%s, size = %d bytes\n", filename, size)
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		exit("Failed to open file")
	}
	defer file.Close()
	file.Truncate(size)
	chunks := (size + chunkSize - 1) / chunkSize
	bar := pb.StartNew(int(chunks))
	for i := int64(0); i < chunks; i++ {
		var cksum uint32
		if err := binary.Read(r, binary.LittleEndian, &cksum); err != nil {
			exit("Failed to read checksum")
		}
		file.Seek(i*chunkSize, 0)
		buf := make([]byte, calcChunkSize(i, size), chunkSize)
		if _, err := io.ReadFull(file, buf); err == nil {
			cksum2 := crc32.ChecksumIEEE(buf)
			if cksum == cksum2 {
				binary.Write(conn, binary.LittleEndian, true)
				bar.Increment()
				continue
			}
		}
		binary.Write(conn, binary.LittleEndian, false)
		if _, err := io.ReadFull(r, buf); err != nil {
			exit("Failed to read chunk")
		}
		if _, err = file.WriteAt(buf, i*chunkSize); err != nil {
			exit("Failed to write chunk to file")
		}
		bar.Increment()
	}
	bar.Finish()
}
