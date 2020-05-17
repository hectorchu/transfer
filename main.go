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
const chunkSize = 64 * 1024

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

func calcChunkSize(chunkIndex int, fileSize int64) int64 {
	if int64(chunkIndex+1)*chunkSize > fileSize {
		return fileSize % chunkSize
	}
	return chunkSize
}

func calcHashes(file *os.File) (hashes []uint32) {
	stat, _ := file.Stat()
	size := stat.Size()
	file.Seek(0, 0)
	chunks := (size + chunkSize - 1) / chunkSize
	hashes = make([]uint32, chunks, chunks)
	for i := range hashes {
		h := crc32.NewIEEE()
		if _, err := io.CopyN(h, file, calcChunkSize(i, size)); err != nil {
			exit("Failed to hash file")
		}
		hashes[i] = h.Sum32()
	}
	return
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
	hashes := calcHashes(file)
	binary.Write(conn, binary.LittleEndian, hashes)
	hashOk := make([]bool, chunks, chunks)
	if err := binary.Read(r, binary.LittleEndian, hashOk); err != nil {
		return
	}
	for i := range hashOk {
		if !hashOk[i] {
			file.Seek(int64(i*chunkSize), 0)
			if _, err := io.CopyN(conn, file, calcChunkSize(i, size)); err != nil {
				return
			}
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
	hashes := make([]uint32, chunks, chunks)
	hashes2 := calcHashes(file)
	if err := binary.Read(r, binary.LittleEndian, hashes); err != nil {
		exit("Failed to read hashes")
	}
	hashOk := make([]bool, chunks, chunks)
	for i := range hashes2 {
		hashOk[i] = hashes[i] == hashes2[i]
	}
	binary.Write(conn, binary.LittleEndian, hashOk)
	bar := pb.StartNew(int(chunks))
	for i := range hashOk {
		if !hashOk[i] {
			file.Seek(int64(i*chunkSize), 0)
			h := crc32.NewIEEE()
			if _, err := io.CopyN(file, io.TeeReader(r, h), calcChunkSize(i, size)); err != nil {
				exit("Failed to read chunk")
			}
			if h.Sum32() != hashes[i] {
				exit("Hash mismatch")
			}
		}
		bar.Increment()
	}
	bar.Finish()
}
